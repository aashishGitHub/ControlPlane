.PHONY: up down logs deps test test-race check fmt fmt-check lint worker-restart race-demo

# Windows resolves bare `bash` to WSL's C:\Windows\System32\bash.exe, which may
# have no distro installed. Use Git Bash explicitly; Linux/macOS keep plain bash.
ifeq ($(OS),Windows_NT)
BASH ?= "C:/Program Files/Git/bin/bash.exe"
else
BASH ?= bash
endif

# Pinned tool images. `make lint` and `make test-race` run in containers so the
# quality gate works identically on Windows (no local gcc for -race) and on
# Linux/macOS, without requiring reviewers to install extra binaries.
GO_IMAGE ?= golang:1.25
GOLANGCI_LINT_IMAGE ?= golangci/golangci-lint:v2.15.0-alpine

# compose.yaml sets `name: controlplane`, so the default network is fixed.
COMPOSE_NETWORK ?= controlplane_default

# Integration tests inside a container must use Compose service DNS names, not
# localhost. These are local-dev credentials from compose.yaml, not secrets.
TEST_DATABASE_URL ?= postgres://controlplane:localdev@postgres:5432/controlplane?sslmode=disable
TEST_AMQP_URL ?= amqp://guest:guest@rabbitmq:5672/
TEST_RABBIT_MGMT_URL ?= http://guest:guest@rabbitmq:15672

# Worker fault-injection knobs. `export` passes them into the compose recipe, so
# `make worker-restart FAIL_RATE=1` works without POSIX inline env assignments
# (which cmd.exe on Windows does not support).
FAIL_RATE ?= 0
MIN_DELAY_MS ?= 100
MAX_DELAY_MS ?= 800
DUPLICATE_RATE ?= 0
REORDER ?= false
export FAIL_RATE MIN_DELAY_MS MAX_DELAY_MS DUPLICATE_RATE REORDER

## up: build and boot the full stack (postgres, rabbitmq, controlplane, worker)
up:
	docker compose up -d --build --wait

## deps: start only Postgres and RabbitMQ (used by the test targets)
deps:
	docker compose up -d --wait postgres rabbitmq

## worker-restart: rebuild/recreate only the worker; `make worker-restart FAIL_RATE=1`
worker-restart:
	docker compose up -d --build --force-recreate worker

## race-demo: 50 concurrent POSTs / 50 concurrent PATCHes; exactly one winner each
race-demo:
	$(BASH) scripts/race-demo.sh

## down: stop and remove containers + volumes (fresh state next `make up`)
down:
	docker compose down -v

## logs: follow logs of every running service
logs:
	docker compose logs -f

## test: run the Go test suite against Postgres + RabbitMQ (started by `deps`)
test: deps
	go test ./... -count=1

## test-race: run the full suite under the race detector in Linux (needs cgo).
## The container joins the Compose network and uses service DNS + the dedicated
## test database/vhost, so it cannot collide with the running dev stack.
test-race: deps
	docker run --rm --network $(COMPOSE_NETWORK) \
		-v "$(CURDIR):/src" -w /src \
		-e DATABASE_URL="$(TEST_DATABASE_URL)" \
		-e AMQP_URL="$(TEST_AMQP_URL)" \
		-e RABBIT_MGMT_URL="$(TEST_RABBIT_MGMT_URL)" \
		$(GO_IMAGE) go test -race ./... -count=1

## fmt: reformat all Go source in place
fmt:
	gofmt -w .

## fmt-check: fail if any file is not gofmt-formatted.
## Implemented with Make functions (no POSIX test/[ ]) so it works in both
## cmd.exe and sh; the old recipe only worked under a POSIX shell.
fmt-check:
	@$(if $(shell gofmt -l .),$(error not gofmt-formatted: $(shell gofmt -l .)),echo gofmt: clean)

## lint: golangci-lint (errcheck, govet, staticcheck, unused, gosec, ...)
lint:
	docker run --rm -v "$(CURDIR):/src" -w /src \
		$(GOLANGCI_LINT_IMAGE) golangci-lint run

## check: the full quality gate (NFR-4) - run before every commit
check: fmt-check lint test test-race
