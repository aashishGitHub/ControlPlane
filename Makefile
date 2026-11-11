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

## test: run the Go test suite with the race detector
test:
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
