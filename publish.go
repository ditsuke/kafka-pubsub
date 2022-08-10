package kafka_pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync"
	"time"
)

const (
	maxRetriesInternal = 10
	maxRetriesExplicit = 5
	waitOnWriteFailure = 250 * time.Millisecond
)

type IntIterator func() (messageNumber int, ok bool)

// WriteToKafka writes a series of messages to a kafka topic as directed through the Options passed.
// The consumer attempts to terminate gracefully when the ctx passed expires.
func WriteToKafka(ctx context.Context, opts PubOptions) error {
	brokerSeeds := []string{fmt.Sprintf("%s:%d", opts.KafkaHost, opts.KafkaPort)}

	// Create a kafka client.
	// Producer idempotence is enabled by default (!)
	// We handle partitioning records manually through a custom strategy that fits the messages well.
	k, err := kgo.NewClient(
		kgo.SeedBrokers(brokerSeeds...),
		kgo.DefaultProduceTopic(opts.Topic),
		kgo.RecordRetries(maxRetriesInternal),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %v", err)
	}
	log.Println("connected to kafka broker")
	defer k.Close()

	// Create opts.Topic if it doesn't exist.
	err = createTopic(k, opts)
	if err != nil {
		return err
	}

	// Concurrently write messages to each kafka partition, independently.
	// This strategy ensures maximum throughput while still allow us to guarantee message ordering within each partition
	wg := sync.WaitGroup{}
	chanError := make(chan error)

	for i := 0; i < opts.Partitions; i++ {
		wg.Add(1)
		go func(i int32) {
			defer wg.Done()
			writer := partitionWriter{
				kClient:   k,
				partition: i,
				opts:      opts,
				iterator: func() IntIterator {
					index := int(i)
					return func() (int, bool) {
						current := index
						if current >= opts.EventCount {
							return 0, false
						}
						index += opts.Partitions
						return current, true
					}
				}(),
			}
			err := writer.write(ctx)
			if err != nil {
				log.Printf("error writing to partition %d: %+v\n", i, err)
				chanError <- err
			}
		}(int32(i))
	}

	// Close chanDone when all goroutines have finished.
	chanDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(chanDone)
	}()

	// Close client when context expires
	go func() {
		select {
		case <-ctx.Done():
			log.Println("context expired. closing kafka client")
			k.Close()
			log.Println("kafka client closed")
		case <-chanDone:
			log.Println("chanDone closure received in context expiration listener.")
		}
	}()

	// Block until all goroutines have finished, or we encounter an error we couldn't recover from.
	select {
	case err := <-chanError:
		log.Printf("error: %+v", err)
		if errors.Is(err, kgo.ErrClientClosed) {
			return nil
		}
		return err
	case <-chanDone:
		log.Println("partition writers are done writing")
		return nil
	}
}

// createTopic creates opts.Topic if it doesn't exist.
func createTopic(k *kgo.Client, opts PubOptions) error {
	adminClient := kadm.NewClient(k)
	response, err := adminClient.CreateTopics(context.Background(), int32(opts.Partitions), 1, nil, opts.Topic)
	if rErr := response[opts.Topic].Err; err != nil || rErr != nil && !errors.Is(rErr, kerr.TopicAlreadyExists) {
		return fmt.Errorf("failed to create topic: issue_error=%v, creation_error=%+v", err, rErr)
	}
	return nil
}

type partitionWriter struct {
	kClient   *kgo.Client
	partition int32
	opts      PubOptions
	// iterator yields the message number to write/batch up.
	iterator IntIterator
}

func (p partitionWriter) write(ctx context.Context) error {
	// Loop until we encounter an error or write all messages yielded by the iterator.
	for {
		batch := batchMessages(p.opts.BatchSize, p.partition, func(messageNumber int) ([]byte, error) {
			n, ok := p.iterator()
			if !ok {
				return nil, errors.New("iterator exhausted")
			}
			return []byte(fmt.Sprintf("%d", n)), nil
		})

		if len(batch) == 0 || ctx.Err() != nil {
			break
		}
		_, err := p.writeBatch(ctx, batch)
		if err != nil {
			return err
		}
	}
	return nil
}

// writeBatch writes a batch of messages to a kafka writer with a retry policy. The function
// returns the number of messages written along with an error that should be checked for.
func (p partitionWriter) writeBatch(ctx context.Context, messageBatch []*kgo.Record) (int, error) {
	for i := 0; i < maxRetriesExplicit; i++ {
		results := p.kClient.ProduceSync(ctx, messageBatch...)
		// If the first result is nil, then we've successfully written the batch.
		if results[0].Err == nil {
			log.Printf("wrote batch (size=%d): %s, ... %s", p.opts.BatchSize, messageBatch[0].Value, messageBatch[len(messageBatch)-1].Value)
			return len(messageBatch), nil
		}

		// We shouldn't retry if the client has been closed.
		if errors.Is(results[0].Err, kgo.ErrClientClosed) {
			return 0, results[0].Err
		}

		log.Printf("failed to write batch: %+v. retrying after waiting...\n", results[0].Err)
		time.Sleep(waitOnWriteFailure)
	}
	return 0, fmt.Errorf("failed to write batch after %d retries", maxRetriesExplicit)
}

// batchMessages packs a batch of messages, given a batch size, a target partition and function that yields
// the value corresponding to a message number within the batch.
func batchMessages(batchSize int, partition int32, valueIterator func(messageNumber int) ([]byte, error)) []*kgo.Record {
	messages := make([]*kgo.Record, batchSize)
	for i := range messages {
		value, err := valueIterator(i)
		if err != nil {
			return messages[:i]
		}
		messages[i] = &kgo.Record{
			Value:     value,
			Partition: partition,
		}
	}
	return messages
}
