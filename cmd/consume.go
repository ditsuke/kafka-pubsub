package main

import (
	"context"
	"flag"
	"fmt"
	ps "github.com/ditsuke/kafka-pubsub"
	. "github.com/ditsuke/kafka-pubsub/cmd/internal"
	"github.com/ditsuke/kafka-pubsub/command_helpers"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
)

func main() {
	var opts ps.SubOptions
	var suppressLogs bool

	flags := flag.NewFlagSet("", flag.ExitOnError)
	flags.StringVar(&opts.KafkaHost, "kafka-host", DefaultKafkaHost, "kafka host")
	flags.IntVar(&opts.KafkaPort, "kafka-port", DefaultKafkaPort, "kafka port")
	flags.StringVar(&opts.Topic, "topic", DefaultTopic, "topic to consume")
	flags.StringVar(&opts.Group, "group", DefaultConsumerGroup, "consumer group to use for consumption")
	flags.BoolVar(&suppressLogs, "no-logs", false, "no logging to stdout")
	flags.IntVar(&opts.EventCount, "events-to-consume", DefaultEventsConsume, "number of events to consume. >=0 for unlimited.")
	err := flags.Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	if suppressLogs {
		log.SetOutput(ioutil.Discard)
	}
	log.Printf("configured: %+v\n", opts)

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal)
	signal.Notify(sig, os.Interrupt, os.Kill)
	go command_helpers.CancelOnSignal(sig, cancel)

	_, err = ps.ReadFromKafka(ctx, opts)
	if err != nil {
		fmt.Printf("error consuming: %+v", err)
	}
}
