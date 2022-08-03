package main

import (
	"flag"
	ps "github.com/ditsuke/kafka-pubsub"
	. "github.com/ditsuke/kafka-pubsub/cmd/internal"
	"log"
	"os"
)

func main() {
	var cfg ps.PubOptions
	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&cfg.KafkaHost, "kafka-host", DefaultKafkaHost, "kafka host")
	flags.IntVar(&cfg.KafkaPort, "kafka-port", DefaultKafkaPort, "kafka port")
	flags.StringVar(&cfg.Topic, "topic", DefaultTopic, "topic to write to")
	flags.IntVar(&cfg.Partitions, "partitions", DefaultPartitions, "number of partitions")
	flags.IntVar(&cfg.EventCount, "events", DefaultEventCount, "number of events to write")
	flags.IntVar(&cfg.BatchSize, "batch-size", DefaultBatchSize, "number of events to write in a batch")
	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("configured: %+v\n", cfg)
	ps.WriteToKafka(cfg)
}
