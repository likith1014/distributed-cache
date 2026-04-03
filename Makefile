.PHONY: build test bench lint proto docker run clean

# ── Build ──────────────────────────────────────────────────────
build:
	@echo "Building server..."
	go build -ldflags="-w -s" -o bin/server ./cmd/server
	@echo "Building CLI..."
	go build -ldflags="-w -s" -o bin/cache-cli ./cmd/cli
	@echo "Done. Binaries in ./bin/"

# ── Test ───────────────────────────────────────────────────────
test:
	go test ./... -v -race -timeout 30s

test-short:
	go test ./... -short -race

coverage:
	go test ./... -coverprofile=coverage.out -race
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# ── Benchmarks ─────────────────────────────────────────────────
bench:
	go test ./bench/... -bench=. -benchtime=10s -benchmem -count=3

bench-lru:
	go test ./internal/cache/... -bench=BenchmarkLRU -benchtime=10s -benchmem

bench-hash:
	go test ./bench/... -bench=BenchmarkConsistentHash -benchtime=10s -benchmem

# ── Lint ───────────────────────────────────────────────────────
lint:
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

vet:
	go vet ./...

# ── Proto ──────────────────────────────────────────────────────
proto:
	@which protoc > /dev/null || (echo "Install protoc from https://grpc.io/docs/protoc-installation/" && exit 1)
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/cache.proto
	@echo "Generated proto files in proto/cachepb/"

# ── Docker ─────────────────────────────────────────────────────
docker-build:
	docker build -t distributed-cache:latest .

docker-cluster:
	docker-compose up --build

docker-down:
	docker-compose down -v

# ── Run (single node) ──────────────────────────────────────────
run:
	go run ./cmd/server --node-id node-1 --grpc-port 7071 --http-port 8081

run-lfu:
	CONFIG_CACHE_POLICY=lfu go run ./cmd/server --node-id node-1

# ── Health check ───────────────────────────────────────────────
health:
	curl -s http://localhost:8081/health | python3 -m json.tool

stats:
	curl -s http://localhost:8081/stats | python3 -m json.tool

metrics:
	curl -s http://localhost:8081/metrics | grep cache_

# ── Clean ──────────────────────────────────────────────────────
clean:
	rm -rf bin/ data/ coverage.out coverage.html
	go clean -cache

# ── Help ───────────────────────────────────────────────────────
help:
	@echo "distributed-cache — Makefile targets:"
	@echo ""
	@echo "  make build          Build server and CLI binaries"
	@echo "  make test           Run all tests with race detector"
	@echo "  make bench          Run all benchmarks (10s each)"
	@echo "  make coverage       Generate HTML coverage report"
	@echo "  make docker-cluster Spin up 4-node cluster + Prometheus"
	@echo "  make run            Start single node on port 7071/8081"
	@echo "  make health         Health check running node"
	@echo "  make stats          Show cache statistics"
	@echo "  make proto          Regenerate gRPC code from .proto"
	@echo "  make lint           Run golangci-lint"
	@echo "  make clean          Remove build artifacts"
