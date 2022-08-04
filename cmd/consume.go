package main

import (
	"flag"
	ps "github.com/ditsuke/kafka-pubsub"
	. "github.com/ditsuke/kafka-pubsub/cmd/internal"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	var cfg ps.SubOptions
	var suppressLogs bool

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&cfg.KafkaHost, "kafka-host", DefaultKafkaHost, "kafka host")
	flags.IntVar(&cfg.KafkaPort, "kafka-port", DefaultKafkaPort, "kafka port")
	flags.StringVar(&cfg.Topic, "topic", DefaultTopic, "topic to consume")
	flags.StringVar(&cfg.Group, "group", DefaultConsumerGroup, "consumer group to use for consumption")
	flags.BoolVar(&suppressLogs, "no-logs", false, "no logging to stdout")
	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	if suppressLogs {
		log.SetOutput(ioutil.Discard)
	}
	log.Printf("configured: %+v\n", cfg)
	ps.ReadFromKafka(cfg)
}
