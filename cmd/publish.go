package main

import (
	"flag"
	"fmt"
	ps "github.com/ditsuke/kafka-pubsub"
	. "github.com/ditsuke/kafka-pubsub/cmd/internal"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	var cfg ps.PubOptions
	var suppressLogs bool

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&cfg.KafkaHost, "kafka-host", DefaultKafkaHost, "kafka host")
	flags.IntVar(&cfg.KafkaPort, "kafka-port", DefaultKafkaPort, "kafka port")
	flags.StringVar(&cfg.Topic, "topic", DefaultTopic, "topic to write to")
	flags.IntVar(&cfg.Partitions, "partitions", DefaultPartitions, "number of partitions")
	flags.IntVar(&cfg.EventCount, "events", DefaultEventCount, "number of events to write")
	flags.IntVar(&cfg.BatchSize, "batch-size", DefaultBatchSize, "number of events to write in a batch")
	flags.BoolVar(&suppressLogs, "no-logs", false, "no logging to stdout")

	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	if suppressLogs {
		log.SetOutput(ioutil.Discard)
	}
	log.Printf("configured: %+v\n", cfg)
	err = ps.WriteToKafka(cfg)
	if err != nil {
		panic(fmt.Errorf("error publishing: %+v", err))
	}
}
