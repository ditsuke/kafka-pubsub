package kafka_pubsub_test

import (
	"fmt"
	ps "github.com/ditsuke/kafka-pubsub"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"
	"time"
)

func defaultSubOpts(topicSuffix string) ps.SubOptions {
	rand.Seed(time.Now().Unix())
	return ps.SubOptions{
		Options: ps.Options{
			KafkaHost:  "localhost",
			KafkaPort:  9092,
			Topic:      "test-bench-" + topicSuffix,
			Partitions: 3,
		},
		Group: "test-group",
	}
}

func BenchmarkReadFromKafka_10_000(b *testing.B) {
	const (
		// OptimalBatchSize as observed from the publisher benchmark
		optimalBatchSize = 400

		// Events to read
		eventCount = 100_000
	)

	// Topic suffix to make sure we are writing to a new topic
	rand.Seed(time.Now().Unix())
	topicSuffix := fmt.Sprintf("%d", rand.Int())

	log.SetOutput(ioutil.Discard)

	// Read 10M events into Kafka
	pubOpts := defaultPubOpts(topicSuffix)
	pubOpts.EventCount = 10_000_000
	pubOpts.BatchSize = optimalBatchSize
	ps.WriteToKafka(pubOpts)

	// Reset timer to ignore setup time
	b.ResetTimer()

	// Setup
	opts := defaultSubOpts(topicSuffix)
	opts.EventCount = eventCount
	for i := 0; i < b.N; i++ {
		ps.ReadFromKafka(opts)
	}

	b.ReportMetric(float64(eventCount), "reads/op")
}
