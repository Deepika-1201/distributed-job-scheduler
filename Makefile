GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
LOCAL_DB ?= postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable

# Pinned protobuf toolchain, installed into .tools by `make tools` (macOS arm64 or Linux x86-64).
PROTOC_VERSION     := 36.2
PROTOC_GEN_GO      := v1.36.12
PROTOC_GEN_GO_GRPC := v1.6.2
TOOLS              := $(CURDIR)/.tools
PROTOC_OS          := $(if $(filter Darwin,$(shell uname -s)),osx-aarch_64,linux-x86_64)
GO_DIRS            := cmd internal pkg

.PHONY: build test test-short test-race cover vet fmt fmt-check lint tidy run migrate up down clean tools proto demo-worker

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/jobscheduler ./cmd/jobscheduler

demo-worker:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/demo-worker ./cmd/demo-worker

test:
	$(GO) test ./...

test-short:
	$(GO) test -short ./...

test-race:
	$(GO) test -race ./...

cover:
	$(GO) test -coverprofile=coverage.out ./... && $(GO) tool cover -func=coverage.out | tail -1

vet:
	$(GO) vet ./...

fmt:
	gofmt -w $(GO_DIRS)

fmt-check:
	@out=$$(gofmt -l $(GO_DIRS)); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

lint: fmt-check vet

tidy:
	$(GO) mod tidy

run: build
	JS_LOG_FORMAT=text JS_DATABASE_URL="$${JS_DATABASE_URL:-$(LOCAL_DB)}" ./bin/jobscheduler

migrate: build
	JS_LOG_FORMAT=text JS_DATABASE_URL="$${JS_DATABASE_URL:-$(LOCAL_DB)}" ./bin/jobscheduler migrate

up:
	docker compose up --build

down:
	docker compose down

clean:
	rm -rf bin coverage.out

tools:
	mkdir -p "$(TOOLS)/bin"
	curl -fsSL -o "$(TOOLS)/protoc.zip" https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(PROTOC_OS).zip
	unzip -oq "$(TOOLS)/protoc.zip" -d "$(TOOLS)/protoc" && rm "$(TOOLS)/protoc.zip"
	GOBIN="$(TOOLS)/bin" $(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO)
	GOBIN="$(TOOLS)/bin" $(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC)

# Regenerates pkg/workerpb from proto/; the output is committed.
proto:
	PATH="$(TOOLS)/bin:$$PATH" "$(TOOLS)/protoc/bin/protoc" -I proto \
		--go_out=. --go_opt=module=jobscheduler \
		--go-grpc_out=. --go-grpc_opt=module=jobscheduler \
		proto/jobscheduler/worker/v1/worker.proto
