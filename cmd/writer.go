package main

import (
	"flag"
	"fmt"
	"github.com/segmentio/kafka-go"
	"kafka-task"
	"log"
	"os"
)

const (
	defaultKafkaHost  = "localhost"
	defaultKafkaPort  = 9092
	defaultTopic      = "test-numbers"
	defaultPartitions = 3
	defaultEventCount = 9999
	defaultBatchSize  = 100
)

func main() {
	var cfg kafka_task.Config
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&cfg.KafkaHost, "kafka-host", defaultKafkaHost, "kafka host")
	flags.IntVar(&cfg.KafkaPort, "kafka-port", defaultKafkaPort, "kafka port")
	flags.StringVar(&cfg.Topic, "topic", defaultTopic, "topic to write to")
	flags.IntVar(&cfg.Partitions, "partitions", defaultPartitions, "number of partitions")
	flags.IntVar(&cfg.EventCount, "events", defaultEventCount, "number of events to write")
	flags.IntVar(&cfg.BatchSize, "batch-size", defaultBatchSize, "number of events to write in a batch")
	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("configured: %+v\n", cfg)

	// test connection
	k, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err)
	}
	defer func() { _ = k.Close() }()
	fmt.Println("connected to kafka broker")
	err = k.CreateTopics(kafka.TopicConfig{
		Topic:             cfg.Topic,
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		panic(fmt.Errorf("failed to create topic: %v", err))
	}
	kafka_task.WriteToKafka(cfg)
}
