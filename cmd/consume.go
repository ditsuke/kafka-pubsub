package main

import (
	"flag"
	ps "github.com/ditsuke/kafka-pubsub"
	"log"
	"os"
)

func main() {
	var cfg ps.Config

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&cfg.KafkaHost, "kafka-host", "localhost", "kafka host")
	flags.IntVar(&cfg.KafkaPort, "kafka-port", 9092, "kafka port")
	flags.StringVar(&cfg.Topic, "topic", "test-numbers", "topic to consume")
	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("configured: %+v\n", cfg)
	ps.ReadFromKafka(cfg)
}
