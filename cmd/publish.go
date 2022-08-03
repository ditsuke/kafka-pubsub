package main

import (
	"flag"
	ps "github.com/ditsuke/kafka-pubsub"
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
	var cfg ps.Config
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
	ps.WriteToKafka(cfg)
}
