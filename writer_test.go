package kafka_task_test

import (
	"fmt"
	"kafka-task"
	"log"
	"os"
	"testing"
)

func benchmarkWriteToKafka(cfg kafka_task.Config, b *testing.B) {
	for i := 0; i < b.N; i++ {
		kafka_task.WriteToKafka(cfg)
	}
}

func defaultConfig() kafka_task.Config {
	return kafka_task.Config{
		KafkaHost:  "localhost",
		KafkaPort:  9092,
		Topic:      "test-bench",
		Partitions: 3,
	}
}

func BenchmarkWriteToKafka(b *testing.B) {
	eventCount := 99
	batchSizeMin := 1
	batchSizeMax := 250
	stepSize := 20

	logFile, err := os.OpenFile("logfile.txt", os.O_APPEND, 0755)
	if err != nil {
		b.Fatalf("could not open log file: %+v", err)
	}
	log.SetOutput(logFile)

	for batchSize := batchSizeMin; batchSize <= batchSizeMax; batchSize += stepSize {
		b.Run(fmt.Sprintf("batch_size=%d", batchSize), func(b *testing.B) {
			cfg := defaultConfig()
			cfg.EventCount = eventCount
			cfg.BatchSize = batchSize

			benchmarkWriteToKafka(cfg, b)
		})
	}
}
