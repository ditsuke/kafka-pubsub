package main

import (
	"flag"
	kafka_task "kafka-task"
	"log"
	"os"
)

func main() {
	var cfg kafka_task.Config

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&cfg.KafkaHost, "kafka-host", "localhost", "kafka host")
	flags.IntVar(&cfg.KafkaPort, "kafka-port", 9092, "kafka port")
	flags.StringVar(&cfg.Topic, "topic", "test-numbers", "topic to consume")
	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("configured: %+v\n", cfg)
	kafka_task.ReadFromKafka(cfg)
}
