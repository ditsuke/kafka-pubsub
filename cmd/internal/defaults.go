package internal

const (
	DefaultKafkaHost     = "localhost"
	DefaultKafkaPort     = 9092
	DefaultTopic         = "test-numbers"
	DefaultPartitions    = 3
	DefaultEventCount    = 10_000_000
	DefaultBatchSize     = 100
	DefaultConsumerGroup = "test-group"
	DefaultEventsConsume = DefaultEventCount
)
