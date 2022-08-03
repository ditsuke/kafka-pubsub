package kafka_task

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
)

func ReadFromKafka(cfg Config) {
	brokerSeeds := []string{fmt.Sprintf("%s:%d", cfg.KafkaHost, cfg.KafkaPort)}

	// Create a new kafka client to back a consumer group
	// This consumer group will read from the topic written by the writer/producer
	// All partitions will have single concurrent consumers within the group
	// @todo to guarantee read-once semantics we need to disable auto-commit. Disabling auto-commit requires some
	//  adjustments to guarantee read-once.
	kClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokerSeeds...),
		kgo.ConsumerGroup("some-consumer-group"),
		kgo.ConsumeTopics(cfg.Topic),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create kafka client: %+v", err))
	}

	go consume(kClient, cfg)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt)

	<-sig
	done := make(chan struct{})

	go func() {
		kClient.Close()
		close(done)
	}()

	select {
	case <-done:
		log.Println("kafka client closed")
	case <-sig:
		log.Println("second interrupt received, exiting")
	}
}

func consume(kClient *kgo.Client, cfg Config) {
	var read int
	for {
		records := kClient.PollFetches(context.Background())
		if records.IsClientClosed() {
			log.Printf("=== read %d records from topic %s", read, cfg.Topic)
			return
		}
		records.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error: topic %s, partition %d, error: %v\n", topic, partition, err)
		})

		records.EachRecord(func(r *kgo.Record) {
			log.Printf("record: %s\n", r.Value)
			read++
		})
	}
}
