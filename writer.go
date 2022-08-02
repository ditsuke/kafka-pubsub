package kafka_task

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	maxRetriesInternal = 10
	maxRetriesExplicit = 5
	electionWait       = 250 * time.Millisecond
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

func WriteToKafka(cfg Config) {
	// Create producer ===
	writer := kafka.Writer{
		Addr:                   kafka.TCP(fmt.Sprintf("%s:%d", cfg.KafkaHost, cfg.KafkaPort)),
		Topic:                  cfg.Topic,
		RequiredAcks:           1,
		AllowAutoTopicCreation: true,
		MaxAttempts:            maxRetriesInternal,
		Balancer: kafka.BalancerFunc(func(message kafka.Message, i ...int) int {
			// Use a custom balancer to balance between partitions
			// Essentially, this balancer behaves like the round-robin balancer, but only
			// it will map a message key to the same partition, which is useful for consistent partitioning in
			// case of failures.
			l := len(i)
			key, err := strconv.Atoi(string(message.Key))
			if err != nil {
				panic("key is not an integer")
			}
			return i[key%l]
		}),
	}
	// Concurrently write messages to each kafka partition, independently.
	// This strategy ensures maximum throughput while still allow us to guarantee message ordering within each partition
	wg := sync.WaitGroup{}
	for i := 0; i < cfg.Partitions; i++ {
		wg.Add(1)
		go func(i int) {
			WriteNumbersToPartition(i, &writer, cfg)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func WriteNumbersToPartition(i int, writer *kafka.Writer, cfg Config) {
	for j := i; j < cfg.EventCount; j += WriteMessageBatchWithRetries(j, writer, cfg) * cfg.Partitions {
	}
}

// WriteMessageBatchWithRetries writes a batch of messages to a kafka writer with a retry policy. On failure after maxRetriesInternal,
// the function will panic. On success, the function returns the number of messages written to make sure the next batch's offset is adjusted.
func WriteMessageBatchWithRetries(startFrom int, writer *kafka.Writer, cfg Config) int {
	messageBatch := makeMessageBatch(cfg.BatchSize, startFrom, cfg.Partitions, cfg.EventCount)
	for i := 0; i < maxRetriesExplicit; i++ {
		// @todo: if retrying a message, we must peek at the partition to see if its already been written
		err := writer.WriteMessages(context.Background(), messageBatch...)
		if err == nil {
			log.Printf("wrote messages: %d, %d, ... %d \n", startFrom, startFrom+cfg.Partitions, startFrom+(len(messageBatch)-1)*cfg.Partitions)
			return len(messageBatch)
		}
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, kafka.UnknownTopicOrPartition) {
			log.Println("leader not available, waiting for election")
			time.Sleep(electionWait)
			continue
		}
		panic(fmt.Errorf("failed to match error type: %v", err))
	}
	panic("failed to write message after retries")
}

func makeMessageBatch(batchSize, startFrom, gap, eventLimit int) []kafka.Message {
	messages := make([]kafka.Message, batchSize)
	for i := 0; i < batchSize; i++ {
		messageNumber := startFrom + i*gap
		if messageNumber >= eventLimit {
			return messages[:i]
		}
		messages[i] = kafka.Message{
			Value: []byte(strconv.Itoa(messageNumber)),
			Key:   []byte(strconv.Itoa(messageNumber)),
		}
	}
	return messages
}
