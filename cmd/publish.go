package main

import (
	"context"
	"flag"
	ps "github.com/ditsuke/kafka-pubsub"
	. "github.com/ditsuke/kafka-pubsub/cmd/internal"
	"github.com/ditsuke/kafka-pubsub/command_helpers"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
)

func main() {
	var opts ps.PubOptions
	var suppressLogs bool

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&opts.KafkaHost, "kafka-host", DefaultKafkaHost, "kafka host")
	flags.IntVar(&opts.KafkaPort, "kafka-port", DefaultKafkaPort, "kafka port")
	flags.StringVar(&opts.Topic, "topic", DefaultTopic, "topic to write to")
	flags.IntVar(&opts.Partitions, "partitions", DefaultPartitions, "number of partitions")
	flags.IntVar(&opts.EventCount, "events", DefaultEventCount, "number of events to write")
	flags.IntVar(&opts.BatchSize, "batch-size", DefaultBatchSize, "number of events to write in a batch")
	flags.BoolVar(&suppressLogs, "no-logs", false, "no logging to stdout")

	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	if suppressLogs {
		log.SetOutput(ioutil.Discard)
	}
	log.Printf("configured: %+v\n", opts)

	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill)
	ctx, cancel := context.WithCancel(context.Background())

	go command_helpers.CancelOnSignal(sig, cancel)

	err = ps.WriteToKafka(ctx, opts)
	if err != nil {
		log.Fatalf("error writing: %+v\n", err)
	}
}
