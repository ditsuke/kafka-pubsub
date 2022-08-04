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

func benchmarkWriteToKafka(cfg ps.PubOptions, b *testing.B) {
	for i := 0; i < b.N; i++ {
		ps.WriteToKafka(cfg)
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

func BenchmarkWriteToKafka_100(b *testing.B) {
	const (
		eventCount   = 100
		batchSizeMin = 1
		batchSizeMax = 250
		stepSize     = 20
	)

	// Topic suffix to make sure we are writing to a new topic
	rand.Seed(time.Now().Unix())
	topicSuffix := fmt.Sprintf("%d", rand.Int())

	// Discard logs (?: maybe a flag/opt to customise)
	log.SetOutput(ioutil.Discard)

	for batchSize := batchSizeMin; batchSize <= batchSizeMax; batchSize += stepSize {
		b.Run(fmt.Sprintf("batch_size=%d", batchSize), func(b *testing.B) {
			opts := defaultPubOpts(topicSuffix)
			opts.EventCount = eventCount
			opts.BatchSize = batchSize

			benchmarkWriteToKafka(opts, b)
		})
	}
}
