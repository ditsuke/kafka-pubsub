package kafka_pubsub

type Options struct {
	KafkaHost string
	KafkaPort int

	// Topic is the kafka topic to publish to/consume.
	Topic string

	// Partitions is the number of partitions we divide the topic into.
	Partitions int
}

type PubOptions struct {
	Options

	// EventCount is the total number of events to publish to Kafka.
	EventCount int

	// BatchSize is the number of messages we batch together while writing to kafka. An optimal batch size
	// maximised throughput.
	BatchSize int
}

type SubOptions struct {
	Options
}
