VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
BIN := bin/teamscache

.PHONY: setup test lint format build run clean

setup: ## Download dependencies and tools
	go mod download

test: ## Run the unit tests
	go test -race ./...

lint: ## Run the linters and check formatting
	go tool golangci-lint run ./...
	@test -z "$$(go tool gofumpt -extra -l .)" || (echo "Run 'make format' for:"; go tool gofumpt -extra -l .; exit 1)

format: ## Format the code
	go tool gofumpt -extra -w .

build: ## Build the binary into bin/
	CGO_ENABLED=0 GOFLAGS=-trimpath go build -ldflags="-s -w -X main.version=$(VERSION)" -o $(BIN) ./cmd/teamscache

run: build ## Build and run, for example: make run ARGS="overview"
	./$(BIN) $(ARGS)

clean: ## Remove build output
	rm -rf bin
