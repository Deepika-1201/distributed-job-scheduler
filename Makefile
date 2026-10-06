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
K6_VERSION         := 2.3.0
K6_BUILD           := k6-v$(K6_VERSION)-$(if $(filter Darwin,$(shell uname -s)),macos-arm64,linux-amd64)
TERRAFORM_VERSION  := 1.16.5
TERRAFORM_OS       := $(if $(filter Darwin,$(shell uname -s)),darwin_arm64,linux_amd64)
TF_ROOTS           := deploy/terraform/bootstrap deploy/terraform/env
GO_DIRS            := cmd internal pkg loadtest

.PHONY: build test test-short test-race cover vet fmt fmt-check lint tidy run migrate up down clean tools proto demo-worker k6 loadtest loadtest-smoke terraform tf-check

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
	$(MAKE) k6

# Installs the pinned k6 into .tools unless it is already there.
k6:
	@if [ ! -x "$(TOOLS)/bin/k6" ]; then \
		mkdir -p "$(TOOLS)/bin" && cd "$(TOOLS)" && \
		if [ "$$(uname -s)" = Darwin ]; then \
			curl -fsSL -o k6.zip https://github.com/grafana/k6/releases/download/v$(K6_VERSION)/$(K6_BUILD).zip && unzip -oq k6.zip && rm k6.zip; \
		else \
			curl -fsSL https://github.com/grafana/k6/releases/download/v$(K6_VERSION)/$(K6_BUILD).tar.gz | tar xz; \
		fi && mv $(K6_BUILD)/k6 bin/k6 && rm -rf $(K6_BUILD); \
	fi

# The load-test gate (LLD §19); its settings are environment variables, see loadtest/run.sh.
loadtest: k6
	K6="$(TOOLS)/bin/k6" ./loadtest/run.sh

loadtest-smoke: k6
	K6="$(TOOLS)/bin/k6" BASE_RATE=50 BURST_RATE=100 WARMUP=10 BURST=20 COOLDOWN=5 WORKERS=8 SLOTS=10 ./loadtest/run.sh

# Regenerates pkg/workerpb from proto/; the output is committed.
proto:
	PATH="$(TOOLS)/bin:$$PATH" "$(TOOLS)/protoc/bin/protoc" -I proto \
		--go_out=. --go_opt=module=jobscheduler \
		--go-grpc_out=. --go-grpc_opt=module=jobscheduler \
		proto/jobscheduler/worker/v1/worker.proto

# Installs the pinned Terraform into .tools unless it is already there.
terraform:
	@if [ ! -x "$(TOOLS)/bin/terraform" ]; then \
		mkdir -p "$(TOOLS)/bin" && cd "$(TOOLS)" && \
		curl -fsSL -o terraform.zip https://releases.hashicorp.com/terraform/$(TERRAFORM_VERSION)/terraform_$(TERRAFORM_VERSION)_$(TERRAFORM_OS).zip && \
		unzip -oq terraform.zip terraform -d bin && rm terraform.zip; \
	fi

# Formats and validates the Terraform roots without touching an AWS account (LLD §22.7).
tf-check: terraform
	"$(TOOLS)/bin/terraform" fmt -check -recursive deploy/terraform
	@for root in $(TF_ROOTS); do \
		echo "validate $$root" && \
		"$(TOOLS)/bin/terraform" -chdir=$$root init -backend=false -input=false >/dev/null && \
		"$(TOOLS)/bin/terraform" -chdir=$$root validate -no-color || exit 1; \
	done
