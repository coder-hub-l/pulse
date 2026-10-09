.PHONY: test test-race build up down clean bench

build:
	go build -o bin/pulse-api.exe ./cmd/api
	go build -o bin/pulse-worker.exe ./cmd/worker
	go build -o bin/pulse-forecast.exe ./cmd/forecast
	go build -o bin/loadtest.exe ./cmd/loadtest

test:
	go test -v ./...

test-race:
	go test -v -race ./...

bench:
	./bin/loadtest.exe -url http://localhost:8080/v1/events -c 50 -n 50000 -keys 1000

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down -v

clean:
	rm -rf bin/

