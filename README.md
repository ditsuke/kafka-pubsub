# Kafka Publish-Subscribe

## Introduction
This project is a simple Kafka publish-subscribe system in Go with an added set of requirements it tries to meet covered
in later sections.
The publisher and subscriber are packaged into their own self-contained binaries, `publish` and `consume`, respectively.
A Zookeeper-backed Kafka cluster is dockerized in with `docker-compose` for convenient and portable reproducibility 
of the test environment. To activate:
```bash
make up
```

To bring down the docker-compose project:
```bash
make down
```

## Components
The publisher and subscriber are standalone binaries. To build:
```bash
make build
```
The binaries will be available in the `build` directory.

### Publisher
The `publish` binary is a Kafka producer that writer messages to a Kafka topic. Parameters, including the number of
and the batch size of messages to write (used to optimise throughput) can be configured with command line flags
passed to the binary.

The publisher meets the following requirements:
- Writes, by default, 10,00,000 messages (0-9,999,999) to a test topic divided into 3 partitions in the Kafka cluster.
  Exactly-once semantics are achieved with the idempotency guarantee. This strategy, while not as strong as achievable
  with Kafka's transactional API, is enough to ensure exactly-once processing in a single producer session considering
  the following cases:
  - Lack of acknowledgement from the Kafka broker -- the producer waits for broker acknowledgement (acks) before producing more.
  - Recoverable errors stemming from network jitters -- the producer uses idempotent writes to prevent duplicates.
  To further improve the exactly-once semantics, a transactional producer should be used instead.
- Writes to the 3 partitions are balanced with a custom simple hashing strategy (since we're only writing serial numbers),
  and messages within each partition are guaranteed to be written in order.

#### Usage
```
$ ./build/publish -h
Usage:
  -batch-size int
        number of events to write in a batch (default 100)
  -events int
        number of events to write (default 10000000)
  -kafka-host string
        kafka host (default "localhost")
  -kafka-port int
        kafka port (default 9092)
  -no-logs
        no logging to stdout
  -partitions int
        number of partitions (default 3)
  -topic string
        topic to write to (default "test-numbers")
```

### Consumer
The `consume` binary implements a Kafka consumer that uses a consumer group (configure with the `--group` flag) to consume
messages from a kafka topic. Each partition in the topic is processed concurrently by partition-specific consumers.

The consumer meets the following requirements:
- Load numbers from all partitions in the Kafka topic.
- Guarantee exactly-once semantics by committing records only once they're processed, and keeping track of consumed
  offsets to prevent re-consumption on failure to commit.

#### Usage
```bash
$ ./build/consume -h
Usage:
  -events-to-consume int
        number of events to consume. >=0 for unlimited. (default 10000000)
  -group string
        consumer group to use for consumption (default "test-group")
  -kafka-host string
        kafka host (default "localhost")
  -kafka-port int
        kafka port (default 9092)
  -no-logs
        no logging to stdout
  -topic string
        topic to consume (default "test-numbers")
```

## Benchmarking
The Makefile defines 2 benchmarks:
- `make bench` is a specialised benchmark testing the publisher and subscriber in isolation, with customized throughput metrics
  for both in `reads/sec` and `writes/sec`.
- `make bench-full` is a complete benchmark for the project spec -- it tests the publisher and consumer in parallel, with
   10,000,000 reads and writes in each iteration of the benchmark and outputs the statistics around how much time
   the operation takes across multiple iterations.
