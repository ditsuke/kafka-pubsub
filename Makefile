GO := go
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

