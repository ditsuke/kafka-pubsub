package main

// This is a custom benchmarking executable for the kafka publisher and consumer.
// While a standard go benchmark works well, it is not possible to use it
// for customized metric representation such as the number of events
// our implementation can read or write in a second. As an alternative to this custom
// test binary, a shell script could have been used instead to extract and re-represent
// data from the benchmark results.

import (
	"fmt"
	ps "github.com/ditsuke/kafka-pubsub"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	// optimalBatchSize as observed from the publisher benchmark
	optimalBatchSize = 400

	// Number of events to use as a sample size in the independent subscriber benchmark
	consumptionSampleSize = 100_000

	// Number of events to use as a sample size in the independent publisher benchmark
	publishSampleSize = 10_000
)

func main() {
	// Topic suffix to make sure we are writing to a new topic
	rand.Seed(time.Now().Unix())
	topicSuffix := fmt.Sprintf("%d", rand.Int())

	log.SetOutput(ioutil.Discard)

	_, _ = fmt.Fprintf(os.Stderr, "Benchmarking consumer with a sample size of %d events ...\n", consumptionSampleSize)
	benchmarkConsumer(topicSuffix)

	_, _ = fmt.Fprintf(os.Stderr, "\n")
	_, _ = fmt.Fprintf(os.Stderr, "Benchmarking publisher with a sample size of %d events ...\n", publishSampleSize)
	benchmarkPublisher(topicSuffix)
}

// benchmarkConsumer benchmarks our kafka consumer by consuming consumptionSampleSize events from a topic.
// The function returns the consumer's independent throughput in events/second.
func benchmarkConsumer(topicSuffix string) {
	// Publish 10M events to Kafka
	pubOpts := defaultPubOpts(topicSuffix)
	pubOpts.EventCount = consumptionSampleSize * 100
	pubOpts.BatchSize = optimalBatchSize
	ps.WriteToKafka(pubOpts)

	opts := defaultSubOpts(topicSuffix)
	opts.EventCount = consumptionSampleSize

	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ps.ReadFromKafka(opts)
		}
	})

	readsPerSec := int64(float64(consumptionSampleSize) * 1e9 /
		float64(result.NsPerOp()),
	)

	fmt.Printf("consumer throughput: %d events/second\n", readsPerSec)
}

func benchmarkPublisher(topicSuffix string) {
	const (
		batchSizeMin = 100
		batchSizeMax = 2000
		stepSize     = 100
	)

	// Convert nanoseconds per op returned by testing.Benchmark to events/second
	toEventsPerSec := func(nsPerOp int64) int64 {
		return int64(float64(publishSampleSize) * 1e9 / float64(nsPerOp))
	}

	// Map batch sizes to throughput's in writes/second
	results := make(map[int]int64)
	results[0] = 0

	// Keep track of the batch size with the highest observed throughput
	var bestBatchSize int
	for batchSize := batchSizeMin; batchSize <= batchSizeMax; batchSize += stepSize {
		opts := defaultPubOpts(topicSuffix)
		opts.EventCount = publishSampleSize
		opts.BatchSize = batchSize

		result := testing.Benchmark(func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ps.WriteToKafka(opts)
			}
		})

		writesPerSec := toEventsPerSec(result.NsPerOp())
		results[batchSize] = writesPerSec
		if writesPerSec > results[bestBatchSize] {
			bestBatchSize = batchSize
		}

		fmt.Printf("publisher throughput, batch size %4d: %8d events/second\n", batchSize, writesPerSec)
	}

	fmt.Printf("\n")
	fmt.Printf("optimal batch size: %d, throughput=%d writes/second\n", bestBatchSize, results[bestBatchSize])
}

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

func defaultPubOpts(topicSuffix string) ps.PubOptions {
	return ps.PubOptions{
		Options: ps.Options{
			KafkaHost:  "localhost",
			KafkaPort:  9092,
			Topic:      "test-bench-" + topicSuffix,
			Partitions: 3,
		},
	}
}
