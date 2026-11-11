.PHONY: up down logs test test-race check fmt fmt-check lint worker-restart race-demo

# Windows resolves bare `bash` to WSL's C:\Windows\System32\bash.exe, which may
# have no distro installed. Use Git Bash explicitly; Linux/macOS keep plain bash.
ifeq ($(OS),Windows_NT)
BASH ?= "C:/Program Files/Git/bin/bash.exe"
else
BASH ?= bash
endif

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

## test: run the Go test suite. No -race here: the race detector needs cgo/gcc,
## which Windows doesn't ship by default. Race detection runs in the Linux-based
## test container instead (see `test-race`, wired into `make check` in M5), which
## already has a C toolchain - so the one check that most needs to catch real
## concurrency bugs still runs, just not on every local Windows `make test`.
test:
	go test ./...

## test-race: run with the race detector - requires cgo (gcc). Run inside the
## Linux container in CI/`make check`, or locally on Linux/macOS, or on Windows
## after installing a gcc toolchain (e.g. WinLibs/MSYS2).
test-race:
	go test -race ./...

## fmt: reformat all Go source in place
fmt:
	gofmt -w .

## fmt-check: fail if any file is not gofmt-formatted (used by `make check`)
fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt-formatted:"; echo "$$unformatted"; exit 1; \
	fi

## lint: static analysis - wired up fully in M5 (golangci-lint, govulncheck, gitleaks)
lint:
	@echo "lint: placeholder until M5"

## check: everything the quality gate requires (NFR-5) - run before every commit
check: fmt-check lint test
