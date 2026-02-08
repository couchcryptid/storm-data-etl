.PHONY: build run test test-cover lint fmt clean

build:
	go build -o bin/etl ./cmd/etl

run:
	go run ./cmd/etl

test:
	go test ./... -v -race -count=1

test-cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .
	goimports -w .

clean:
	rm -rf bin/ coverage.out
