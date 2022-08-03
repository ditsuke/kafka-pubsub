package kafka_task

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

type Config struct {
	KafkaHost string
	KafkaPort int

	// Topic is the kafka topic to write to.
	Topic string

	// Partitions is the number of partitions we divide the topic into.
	Partitions int

	// EventCount is the total number of events to write to Kafka.
	EventCount int

	// BatchSize is the number of messages we batch together while writing to kafka. An optimal batch size
	// maximised throughput.
	BatchSize int
}

// WriteToKafka writes a series of messages to a kafka topic as directed through the Config passed.
func WriteToKafka(cfg Config) {
	brokerSeeds := []string{fmt.Sprintf("%s:%d", cfg.KafkaHost, cfg.KafkaPort)}

	// Create a kafka client.
	// Producer idempotence is enabled by default (!)
	k, err := kgo.NewClient(
		kgo.SeedBrokers(brokerSeeds...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RecordRetries(maxRetriesInternal),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create kafka client: %v", err))
	}
	log.Println("connected to kafka broker")
	defer k.Close()

	// Create cfg.Topic if it doesn't exist.
	CreateTopic(k, cfg)

	// Concurrently write messages to each kafka partition, independently.
	// This strategy ensures maximum throughput while still allow us to guarantee message ordering within each partition
	wg := sync.WaitGroup{}
	for i := 0; i < cfg.Partitions; i++ {
		wg.Add(1)
		go func(i int) {
			WriteNumbersToPartition(i, k, cfg)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

// CreateTopic creates cfg.Topic if it doesn't exist.
func CreateTopic(k *kgo.Client, cfg Config) {
	adminClient := kadm.NewClient(k)
	response, err := adminClient.CreateTopics(context.Background(), int32(cfg.Partitions), 1, nil, cfg.Topic)
	if rErr := response[cfg.Topic].Err; err != nil || rErr != nil && !errors.Is(rErr, kerr.TopicAlreadyExists) {
		panic(fmt.Errorf("failed to create topic: issue_error=%v, creation_error=%+v", err, rErr))
	}
}

func WriteNumbersToPartition(i int, kClient *kgo.Client, cfg Config) {
	for j := i; j < cfg.EventCount; j += WriteMessageBatchWithRetries(j, kClient, cfg) * cfg.Partitions {
	}
}

// WriteMessageBatchWithRetries writes a batch of messages to a kafka writer with a retry policy. On failure after maxRetriesInternal,
// the function will panic. On success, the function returns the number of messages written to make sure the next batch's offset is adjusted.
func WriteMessageBatchWithRetries(start int, kClient *kgo.Client, cfg Config) int {
	messageBatch := packMessageBatch(cfg.BatchSize, start, cfg.Partitions, cfg.EventCount, func(messageNumber int) int32 {
		return int32(messageNumber % cfg.Partitions)
	})

	for i := 0; i < maxRetriesExplicit; i++ {
		results := kClient.ProduceSync(context.Background(), messageBatch...)
		// If the first result is nil, then we've successfully written the batch.
		if results[0].Err == nil {
			log.Printf("wrote batch: %d, %d, ... %d", start, start+cfg.Partitions, start+cfg.Partitions*(len(messageBatch)-1))
			return len(messageBatch)
		}
		log.Printf("failed to write batch: %+v. retrying after waiting...\n", results[0].Err)
		time.Sleep(waitOnWriteFailure)
	}
	panic("failed to write message after retries")
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
			Key:       []byte(strconv.Itoa(messageNumber)),
			Partition: partitioner(messageNumber),
		}
	}
	return messages
}
