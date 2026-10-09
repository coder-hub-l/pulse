.PHONY: build test bench up down clean

build:
	go build -o bin/pulse ./cmd/pulse

test:
	go test ./... -v -count=1

bench:
	go test ./... -bench=. -benchmem

up:
	docker compose up --build -d

down:
	docker compose down -v

clean:
	rm -rf bin/
