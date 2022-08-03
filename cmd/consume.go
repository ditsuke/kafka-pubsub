package main

import (
	"flag"
	ps "github.com/ditsuke/kafka-pubsub"
	. "github.com/ditsuke/kafka-pubsub/cmd/internal"
	"log"
	"os"
)

func main() {
	var cfg ps.SubOptions

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&cfg.KafkaHost, "kafka-host", DefaultKafkaHost, "kafka host")
	flags.IntVar(&cfg.KafkaPort, "kafka-port", DefaultKafkaPort, "kafka port")
	flags.StringVar(&cfg.Topic, "topic", DefaultTopic, "topic to consume")
	flags.StringVar(&cfg.Group, "group", DefaultConsumerGroup, "consumer group to use for consumption")
	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("configured: %+v\n", cfg)
	ps.ReadFromKafka(cfg)
}
