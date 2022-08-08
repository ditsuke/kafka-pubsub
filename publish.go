package kafka_pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	maxRetriesInternal = 10
	maxRetriesExplicit = 5
	waitOnWriteFailure = 250 * time.Millisecond
)

// WriteToKafka writes a series of messages to a kafka topic as directed through the Options passed.
func WriteToKafka(opts PubOptions) error {
	brokerSeeds := []string{fmt.Sprintf("%s:%d", opts.KafkaHost, opts.KafkaPort)}

	// Create a kafka client.
	// Producer idempotence is enabled by default (!)
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
	err = CreateTopic(k, opts)
	if err != nil {
		return err
	}

	// Concurrently write messages to each kafka partition, independently.
	// This strategy ensures maximum throughput while still allow us to guarantee message ordering within each partition
	wg := sync.WaitGroup{}
	chanError := make(chan error)
	for i := 0; i < opts.Partitions; i++ {
		wg.Add(1)
		go func(i int) {
			err := WriteNumbersToPartition(i, k, opts)
			if err != nil {
				chanError <- err
			}
			wg.Done()
		}(i)
	}

	chanDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(chanDone)
	}()

	select {
	case err := <-chanError:
		log.Printf("error: %+v", err)
		return err
	case <-chanDone:
		log.Println("partition writers are done writing")
		return nil
	}
}

// CreateTopic creates opts.Topic if it doesn't exist.
func CreateTopic(k *kgo.Client, opts PubOptions) error {
	adminClient := kadm.NewClient(k)
	response, err := adminClient.CreateTopics(context.Background(), int32(opts.Partitions), 1, nil, opts.Topic)
	if rErr := response[opts.Topic].Err; err != nil || rErr != nil && !errors.Is(rErr, kerr.TopicAlreadyExists) {
		return fmt.Errorf("failed to create topic: issue_error=%v, creation_error=%+v", err, rErr)
	}
	return nil
}

func WriteNumbersToPartition(i int, kClient *kgo.Client, opts PubOptions) error {
	for j := i; j < opts.EventCount; {
		written, err := WriteMessageBatchWithRetries(j, kClient, opts)
		if err != nil {
			return err
		}
		// increment to starting pointer of next batch
		j += written * opts.Partitions
	}
	return nil
}

// WriteMessageBatchWithRetries writes a batch of messages to a kafka writer with a retry policy. The function
// returns the number of messages written along with an error that should be checked for.
func WriteMessageBatchWithRetries(start int, kClient *kgo.Client, opts PubOptions) (int, error) {
	messageBatch := packMessageBatch(opts.BatchSize, start, opts.Partitions, opts.EventCount, func(messageNumber int) int32 {
		return int32(messageNumber % opts.Partitions)
	})

	for i := 0; i < maxRetriesExplicit; i++ {
		results := kClient.ProduceSync(context.Background(), messageBatch...)
		// If the first result is nil, then we've successfully written the batch.
		if results[0].Err == nil {
			log.Printf("wrote batch: %d, %d, ... %d", start, start+opts.Partitions, start+opts.Partitions*(len(messageBatch)-1))
			return len(messageBatch), nil
		}
		log.Printf("failed to write batch: %+v. retrying after waiting...\n", results[0].Err)
		time.Sleep(waitOnWriteFailure)
	}
	return 0, fmt.Errorf("failed to write batch after %d retries", maxRetriesExplicit)
}

// packMessageBatch packs a batch of messages to send through to a broker.
func packMessageBatch(size, start, jump, limit int, partitioner func(messageNumber int) int32) []*kgo.Record {
	messages := make([]*kgo.Record, size)
	for i := 0; i < size; i++ {
		messageNumber := start + i*jump
		if messageNumber >= limit {
			return messages[:i]
		}
		messages[i] = &kgo.Record{
			Value:     []byte(strconv.Itoa(messageNumber)),
			Partition: partitioner(messageNumber),
		}
	}
	return messages
}
