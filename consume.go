package kafka_pubsub

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
	"sync"
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
		select {
		case recs := <-pc.recs:
			for _, rec := range recs {
				log.Printf("consuming t=%s p=%d message=%s", pc.topic, pc.partition, rec.Value)
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

type splitTopicConsumer struct {
	pcs map[topicPartition]*partitionConsumer

	stopPolling    context.CancelFunc
	quit           chan struct{}
	doneProcessing chan struct{}
}

func (tc *splitTopicConsumer) consume(opts SubOptions) {
	brokerSeeds := []string{fmt.Sprintf("%s:%d", opts.KafkaHost, opts.KafkaPort)}

	// Create a new kafka client to back a consumer group
	// This consumer group will read from the topic written by the writer/producer
	// All partitions will have single concurrent consumers within the group
	// @todo to guarantee read-once semantics we need to disable auto-commit. Disabling auto-commit requires some
	//  adjustments to guarantee read-once.
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
		panic(fmt.Errorf("failed to create kafka client: %+v", err))
	}

	pollingCtx, cancelFunc := context.WithCancel(context.Background())
	tc.stopPolling = cancelFunc
	var read int
	for {
		records := kClient.PollFetches(pollingCtx)
		if records.IsClientClosed() || pollingCtx.Err() != nil {
			break
		}
		records.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error: topic %s, partition %d, error: %v\n", topic, partition, err)
		})

		records.EachPartition(func(partition kgo.FetchTopicPartition) {
			pc, ok := tc.pcs[topicPartition{opts.Topic, partition.Partition}]
			if !ok {
				panic(fmt.Errorf("could not find partition consumer t=%s p=%d", opts.Topic, partition.Partition))
			}
			pc.recs <- partition.Records
			read += len(partition.Records)
		})
	}
	kClient.AllowRebalance()
	kClient.Close()
	close(tc.doneProcessing)
	log.Printf("=== read %d records from topic %s", read, opts.Topic)
}

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
	tc.stopPolling()
	log.Printf("stopped topic polling")
	tc.stopPartitionConsumers()
	log.Printf("stopped partition consumers")
	<-tc.doneProcessing
	log.Printf("stopping kafka client")
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

func ReadFromKafka(opts SubOptions) {
	topicConsumer := splitTopicConsumer{
		pcs:            make(map[topicPartition]*partitionConsumer),
		quit:           make(chan struct{}),
		doneProcessing: make(chan struct{}),
	}

	go topicConsumer.consume(opts)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	// Block until we receive an OS signal
	<-sig
	done := make(chan struct{})
	go func() {
		topicConsumer.stop()
		close(done)
	}()

	// Block until the kafka client is closed, or we receive a second OS signal to terminate immediately
	select {
	case <-done:
		log.Println("kafka client closed")
	case <-sig:
		log.Println("second interrupt received, exiting")
	}
}
