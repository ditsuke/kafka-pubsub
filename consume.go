package kafka_pubsub

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"sync"
)

const (
	defaultMaxFetches = 100_000
)

// partitionConsumer instances consume records from a single partition. Records must be pushed into the
// recs channel by a master consumer for a topic that delegates records to partition-specific consumers for increased throughput.
type partitionConsumer struct {
	kClient   *kgo.Client
	topic     string
	partition int32

	// recs receives records to process
	recs chan []*kgo.Record

	quit           chan struct{}
	doneProcessing chan struct{}
}

func (pc *partitionConsumer) consume() {
batchLoop:
	for {
		// Keep track of the last offset we have consumed to guarantee EOS in case we fail to commit records after consuming them
		var lastConsumedOffset int64
		select {
		case recs := <-pc.recs:
			for _, rec := range recs {
				if !(rec.Offset > lastConsumedOffset) {
					// We have already consumed a greater offset -> skip consuming
					log.Printf("SKIPPING consumption of t=%s p=%d message=%s; message_offset=%d current_offset=%d",
						rec.Topic, rec.Partition, rec.Value, rec.Offset, lastConsumedOffset,
					)
					continue
				}
				log.Printf("consuming t=%s p=%d message=%s", pc.topic, pc.partition, rec.Value)
				lastConsumedOffset = rec.Offset
			}
			err := pc.kClient.CommitRecords(context.Background(), recs...)
			if err != nil {
				log.Printf("error while committing records: %+v, t=%s, p=%d, offset_start=%d, offset_end=%d",
					err, pc.topic, pc.partition, recs[0].Offset, recs[len(recs)-1].Offset,
				)
			}
		case <-pc.quit:
			break batchLoop
		}
	}
	close(pc.doneProcessing)
}

// stop the partition consumer. This will block until the consumer has finished processing.
func (pc *partitionConsumer) stop() {
	log.Printf("stopping partition consumer for t=%s p=%d", pc.topic, pc.partition)
	close(pc.quit)
	<-pc.doneProcessing
	return
}

type topicPartition struct {
	topic     string
	partition int32
}

// splitTopicConsumer is a consumer that fetches records from a topic and delegates them to concurrent consumers for each partition.
type splitTopicConsumer struct {
	pcs map[topicPartition]*partitionConsumer

	// stopPolling stops the consumer from polling for new records.
	stopPolling context.CancelFunc

	quit           chan struct{}
	donePolling    chan struct{}
	doneProcessing chan struct{}
}

func (tc *splitTopicConsumer) consume(ctx context.Context, opts SubOptions) (int, error) {
	brokerSeeds := []string{fmt.Sprintf("%s:%d", opts.KafkaHost, opts.KafkaPort)}

	// Create a new kafka client to back a consumer group
	// This consumer group will read from the topic written by the writer/producer
	// All partitions will have single concurrent consumers within the group
	// adjustments to guarantee read-once.
	kClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokerSeeds...),
		kgo.ConsumerGroup("some-consumer-group"),
		kgo.ConsumeTopics(opts.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(tc.assign),
		kgo.OnPartitionsRevoked(tc.revoked),
		kgo.OnPartitionsLost(tc.revoked),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to create kafka client: %+v", err)
	}

	pollCtx, cancelFunc := context.WithCancel(ctx)
	tc.stopPolling = cancelFunc
	var eventsConsumed int
	for {
		if (eventsConsumed >= opts.EventCount && opts.EventCount > 0) ||
			pollCtx.Err() != nil {
			break
		}
		maxFetches := func() int {
			if opts.EventCount > 0 {
				return opts.EventCount - eventsConsumed
			}
			return defaultMaxFetches
		}()
		fetches := kClient.PollRecords(pollCtx, maxFetches)
		if fetches.IsClientClosed() || pollCtx.Err() != nil {
			break
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error: topic %s, partition %d, error: %v\n", topic, partition, err)
		})

		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			pc, ok := tc.pcs[topicPartition{opts.Topic, partition.Partition}]
			if !ok {
				panic(fmt.Errorf("could not find partition consumer t=%s p=%d", opts.Topic, partition.Partition))
			}
			pc.recs <- partition.Records
			eventsConsumed += len(partition.Records)
		})
	}
	close(tc.donePolling)
	// Allow the consumer group to re-balance (as we're done consuming)
	kClient.AllowRebalance()
	kClient.Close()
	tc.stopPartitionConsumers()
	close(tc.doneProcessing)
	log.Printf("read %d records from topic %s", eventsConsumed, opts.Topic)
	return eventsConsumed, nil
}

// stopPartitionConsumers stops all topic consumers. This function blocks until all topic consumers have stopped.
func (tc *splitTopicConsumer) stopPartitionConsumers() {
	wg := sync.WaitGroup{}
	for tp, pc := range tc.pcs {
		delete(tc.pcs, tp)
		wg.Add(1)
		go func(pc *partitionConsumer) {
			pc.stop()
			wg.Done()
		}(pc)
	}
	// wait for all partition consumers to stop
	wg.Wait()
}

// stop the topic consumer. This will block until all the topic consumers have finished processing and stopped.
func (tc *splitTopicConsumer) stop() {
	log.Printf("stopping topic consumer")
	tc.stopPolling()
	<-tc.donePolling
	log.Printf("stopped topic polling")
	<-tc.doneProcessing
	log.Printf("stopping kafka client and topic consumers")
}

func (tc *splitTopicConsumer) assign(_ context.Context, kClient *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &partitionConsumer{
				kClient:        kClient,
				topic:          topic,
				partition:      partition,
				recs:           make(chan []*kgo.Record),
				doneProcessing: make(chan struct{}),
				quit:           make(chan struct{}),
			}
			tp := topicPartition{topic, partition}
			tc.pcs[tp] = pc
			go pc.consume()
		}
	}
}

func (tc *splitTopicConsumer) revoked(_ context.Context, _ *kgo.Client, removed map[string][]int32) {
	wg := sync.WaitGroup{}

	// Block and wait for all the partition consumers to finish processing. This helps us guarantee exactly-once
	// semantics since we're effectively waiting for the consumers to commit their offsets before we allow re-balancing to end
	// and polling to begin again.
	defer wg.Wait()
	for topic, partitions := range removed {
		for _, partition := range partitions {
			tp := topicPartition{topic, partition}
			pc, ok := tc.pcs[tp]
			if !ok {
				panic(fmt.Errorf("did not find consumer for revoked partition"))
			}
			// remove the partition consumer from the map
			delete(tc.pcs, tp)
			wg.Add(1)
			go func() { pc.stop(); wg.Done() }()
		}
	}
}

func ReadFromKafka(ctx context.Context, opts SubOptions) (int, error) {
	topicConsumer := splitTopicConsumer{
		pcs:            make(map[topicPartition]*partitionConsumer),
		quit:           make(chan struct{}),
		donePolling:    make(chan struct{}),
		doneProcessing: make(chan struct{}),
	}

	type consumerResponse struct {
		consumed int
		err      error
	}
	chanConsumerDone := make(chan struct{})
	chanConsumerResponse := make(chan consumerResponse)
	go func() {
		consumed, err := topicConsumer.consume(ctx, opts)
		chanConsumerResponse <- consumerResponse{consumed, err}
		close(chanConsumerDone)
	}()

	// Stop consumers on context cancellation.
	go func() {
		select {
		case <-ctx.Done():
			log.Printf("context cancelled, stopping topic consumer...")
			topicConsumer.stop()
			close(chanConsumerDone)
		case <-chanConsumerDone:
		}
		log.Println("topic consumer stopped apparently")
	}()

	// Block until the consumer has stopped, or we receive a second OS signal to terminate immediately
	<-chanConsumerDone
	log.Println("consumer has stopped")
	consumerYield := <-chanConsumerResponse
	return consumerYield.consumed, consumerYield.err

}
