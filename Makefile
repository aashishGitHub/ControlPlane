.PHONY: up down logs test check fmt fmt-check lint

## up: build and boot the full stack (postgres, rabbitmq, controlplane[, worker from M4])
up:
	docker compose up -d --build --wait

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
