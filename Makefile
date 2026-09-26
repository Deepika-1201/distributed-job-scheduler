GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
LOCAL_DB ?= postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable

.PHONY: build test test-short test-race cover vet fmt fmt-check lint tidy run migrate up down clean

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/jobscheduler ./cmd/jobscheduler

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
	gofmt -w cmd internal

fmt-check:
	@out=$$(gofmt -l cmd internal); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

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
