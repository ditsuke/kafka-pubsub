GO := go
DOCKER := docker
DOCKER_COMPOSE := docker-compose

.PHONY: build
build: # Build the publish and consume binaries
	mkdir -p build
	$(GO) build -o=build/publish cmd/publish.go
	$(GO) build -o=build/consume cmd/consume.go

up: # Spin up Kafka locally with docker-compose
	$(DOCKER_COMPOSE) up -d

down: # Take down Kafka running in docker
	$(DOCKER_COMPOSE) down

bench: build # Run the benchmark
	$(GO) run cmd/benchmark.go

bench-full: build # Run the full benchmark. ! this target is not portable (--network=host is only supported on Linux!)
	$(DOCKER) run -v ${PWD}:/ps -w /ps --network host --rm -it alpine:latest \
	sh ./bin/bench-full-alpine.sh
