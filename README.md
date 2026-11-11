# Tenant Provisioning Control Plane

A small, production-shaped control plane for asynchronous tenant provisioning. It
demonstrates the mechanics that matter when an HTTP request creates a durable
record, a worker performs long-running work, and progress arrives over a broker:
transactional state changes, an outbox, idempotent consumption, optimistic
locking, ordered state transitions, and retries.

The implementation is intentionally local-first: one `make up` boots Postgres,
RabbitMQ, the API, the outbox relay, the progress consumer, and a worker
simulator. The core flow is covered by an in-process lifecycle test against real
Postgres and RabbitMQ.

**Current status:** implementation through the worker milestone is complete and
the full lifecycle test is green. The remaining M5 work is the quality gate:
lint/security checks, race container, and cold-clone verification.

## Contents

- [What it does](#what-it-does)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [HTTP API](#http-api)
- [Testing](#testing)
- [Manual verification and fault injection](#manual-verification-and-fault-injection)
- [Technical design](#technical-design)
- [Configuration](#configuration)
- [Project layout](#project-layout)
- [Known limitations and hardening work](#known-limitations-and-hardening-work)
- [Further documentation](#further-documentation)

## What it does

A tenant is a record with a lifecycle:

```text
POST /v1/tenants
  -> tenant: provisioning
  -> deploy task: accepted

worker completes deploy
  -> task: in_progress -> done
  -> tenant: active

PATCH /v1/tenants/{id}
  -> tenant: updating
  -> update task: accepted
  -> update completes -> tenant: active

DELETE /v1/tenants/{id}
  -> tenant: destroying
  -> destroy task: accepted
  -> destroy completes -> tenant: destroyed
```

Key properties:

- The tenant/task state machines are enforced in the database, not by an
  application mutex.
- Every API mutation writes the tenant and its task in one transaction.
- Task rows double as the outbox: `published_at IS NULL` means “not yet relayed”.
- Worker progress is applied idempotently and tolerates duplicate or reordered
  delivery.
- Destroyed tenants remain as tombstones, so a slug cannot be reused after a
  tenant is destroyed.
- The worker is stateless: it has no database connection and only talks to
  RabbitMQ.

## Architecture

```mermaid
flowchart LR
    Client -->|HTTP| API[controlplane<br/>HTTP API + relay + consumer]
    API -->|SQL| DB[(Postgres<br/>controlplane)]
    DB -->|task row, published_at NULL| Relay[outbox relay]
    Relay -->|publish + confirm| Tasks[cp.tasks exchange]
    Tasks -->|task.deploy/update/destroy| WorkerQueue[worker.tasks]
    WorkerQueue --> Worker[worker simulator]
    Worker -->|task.update.*| Updates[cp.task-updates exchange]
    Updates --> UpdateQueue[cp.task-updates]
    UpdateQueue --> Consumer[consumer]
    Consumer -->|apply in one tx| API
    UpdateQueue -->|poison / delivery limit| DLQ[cp.task-updates.dlq]
```

The `controlplane` process is one deployable unit containing the HTTP API, the
outbox relay, and the progress consumer. The worker is a separate process and can
be scaled independently with competing consumers.

### Layering

| Layer | Responsibility |
|---|---|
| `internal/domain` | Pure state machines, slug rule, error codes, timestamps. No I/O. |
| `internal/store` | Postgres queries, schema, transaction helper. |
| `internal/service` | Use cases and transaction boundaries. The only code that writes tenant/task state. |
| `internal/httpapi` | HTTP adapter, validation, error envelope, pagination. |
| `internal/messaging` | AMQP connection manager, topology, publisher confirms, consumer. |
| `internal/outbox` | Relay loop that publishes committed task rows. |
| `internal/worker` | Stateless worker simulator. |
| `internal/testutil` | Dedicated test database and cross-package advisory lock. |

## Quick start

### Prerequisites

- Go 1.25 or newer
- Docker with Docker Compose
- GNU Make
- `curl`
- On Windows: Git Bash for `make race-demo` and the shell scripts

### Boot the stack

```bash
make up
```

This runs `docker compose up -d --build --wait` and starts:

| Service | Address |
|---|---|
| Control plane API | http://localhost:8080 |
| Postgres | localhost:5432 (`controlplane` database) |
| RabbitMQ AMQP | localhost:5672 |
| RabbitMQ management UI | http://localhost:15672 (`guest` / `guest`) |

The schema is applied idempotently at startup; there is no separate migration
step.

### Check health

```bash
curl -sS http://localhost:8080/healthz
```

Expected:

```json
{"status":"ok"}
```

The health endpoint checks both Postgres and RabbitMQ and returns `503` with a
`dependency` field if either is unavailable.

### Create a tenant

```bash
curl -sS -i -X POST http://localhost:8080/v1/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug":"acme","name":"Acme Corp"}'
```

Expected: `201 Created`, a `Location: /v1/tenants/<tenant-id>` header, and a body
containing both the new tenant (`provisioning`, version `1`) and its deploy task
(`accepted`).

### Watch it become active

```bash
curl -sS http://localhost:8080/v1/tenants/<tenant-id>
```

With the worker running, the tenant moves to `active` after the simulator delay.
The deploy task moves to `done`, and the tenant version advances to `2` because
terminal task outcomes also increment the version.

### Update and delete

Fetch the current version before either operation:

```bash
curl -sS http://localhost:8080/v1/tenants/<tenant-id>
```

```bash
# PATCH requires the current version.
curl -sS -i -X PATCH http://localhost:8080/v1/tenants/<tenant-id> \
  -H 'Content-Type: application/json' \
  -d '{"name":"Acme Inc","version":2}'
```

```bash
# DELETE also requires the current version.
curl -sS -i -X DELETE http://localhost:8080/v1/tenants/<tenant-id> \
  -H 'Content-Type: application/json' \
  -d '{"version":4}'
```

PATCH and DELETE return `202 Accepted`: they record the desired next state and
create a task, but the worker finishes the transition asynchronously. The
response body already shows `updating` or `destroying`, so the caller does not
have to guess. The fixed version numbers in the examples are for the sequence
shown: create leaves the tenant at version 2, PATCH moves it to 3, and the
completed update leaves it at 4. Always use the version returned by the latest
GET.

### Stop the stack

```bash
make down
```

`make down` removes containers **and the Postgres volume**. Use it when you want
a completely fresh state.

## HTTP API

Base URL: `http://localhost:8080`

| Method | Path | Success | Notes |
|---|---|---|---|
| `POST` | `/v1/tenants` | `201` | Body `{slug, name}`; sets `Location` |
| `GET` | `/v1/tenants` | `200` | `?status=&limit=&cursor=` |
| `GET` | `/v1/tenants/{id}` | `200` | Readable after destruction |
| `PATCH` | `/v1/tenants/{id}` | `202` | Body `{name, version}`; version required |
| `DELETE` | `/v1/tenants/{id}` | `202` | Body `{version}`; version required |
| `GET` | `/v1/tenants/{id}/tasks` | `200` | Newest-first task history |
| `GET` | `/v1/tasks` | `200` | `?status=&type=&limit=&cursor=` |
| `GET` | `/v1/tasks/{id}` | `200` | |
| `GET` | `/healthz` | `200` / `503` | Checks Postgres and RabbitMQ |

Mutating endpoints return:

```json
{
  "tenant": {
    "id": "...",
    "slug": "acme",
    "name": "Acme Corp",
    "status": "provisioning",
    "version": 1,
    "created_at": "2026-01-01T00:00:00.000Z",
    "updated_at": "2026-01-01T00:00:00.000Z"
  },
  "task": {
    "id": "...",
    "tenant_id": "...",
    "type": "deploy",
    "status": "accepted",
    "created_at": "2026-01-01T00:00:00.000Z",
    "updated_at": "2026-01-01T00:00:00.000Z"
  }
}
```

A failed task includes an `error` string on the task wire object.

### Validation rules

| Field | Rule |
|---|---|
| `slug` | `^[a-z][-a-z0-9]{1,26}[a-z0-9]$` (3-28 chars, starts with a letter, ends with a letter or digit) |
| `name` | Must not be blank |
| `version` | Required for PATCH and DELETE; must match the current tenant version |

The version is an optimistic-concurrency token. It increments on every API
mutation and every terminal task outcome. A stale version produces
`tenant_version_conflict`; an operation against a tenant in the wrong status
produces `tenant_update_not_allowed`.

### Errors

The error envelope is:

```json
{
  "error": {
    "code": "tenant_version_conflict",
    "message": "tenant version conflict",
    "details": {},
    "request_id": "..."
  }
}
```

| Code | HTTP status |
|---|---|
| `tenant_not_found`, `task_not_found` | `404` |
| `tenant_already_exists` | `409` |
| `tenant_update_not_allowed` | `409` |
| `tenant_version_conflict` | `409` |
| `validation_error` | `422` |
| `malformed_request` | `400` |
| `internal_error` | `500` |

### Pagination

`GET /v1/tenants` and `GET /v1/tasks` use keyset pagination on the UUIDv7 `id`.
Pass the returned `next_cursor` back as `?cursor=`. Default page size is 20;
maximum is 100. An absent or invalid cursor starts from the beginning.

## Testing

### Prerequisites

Integration tests need the Postgres and RabbitMQ services from `make up`.
They do **not** use the development database or the default RabbitMQ vhost.

```bash
make up
```

### Run the complete suite

```bash
go test ./... -count=1
```

On PowerShell, to show only the final lines:

```powershell
go test ./... -count=1 2>&1 | Select-Object -Last 40
```

Formatting and static analysis:

```bash
gofmt -l .
go vet ./...
```

Both should produce no output. `go test ./...` is the fastest way to check the
whole suite; `make test` runs the same suite.

### Test layers

| Area | Command | What it proves |
|---|---|---|
| Domain | `go test ./internal/domain/... -count=1` | Slug rule, timestamp format, task rank, tenant transitions |
| Service/concurrency | `go test ./internal/service/... -count=1` | 50-way create-slug race and 50-way version race |
| Messaging/outbox | `go test ./internal/messaging/... -count=1` | Publisher confirms, outbox commit/rollback, duplicate/stale updates, poison/DLQ, topology |
| Full lifecycle | `go test ./internal/messaging -run TestFullLifecycleInProcess -count=1 -v` | Create -> active -> patch -> active -> delete -> destroyed -> recreate 409 |
| Race detector | `make test-race` | Data races in Go code; requires cgo/gcc |

The full lifecycle test boots the real relay, consumer, and worker in-process
against Postgres and RabbitMQ. It does not start an HTTP server; the HTTP
endpoints are exercised with the manual `curl` flow.

### Test isolation

The compose stack and the tests intentionally use different resources:

| Resource | Development stack | Tests |
|---|---|---|
| Postgres database | `controlplane` | `controlplane_test` |
| RabbitMQ vhost | `/` | `controlplane_test` |
| Relay/consumer | Compose services | In-process test goroutines |

`internal/testutil.OpenDB` creates `controlplane_test` if needed, applies the
schema, and holds a session-level Postgres advisory lock for the test. The lock
serializes test packages, which otherwise run in parallel and all truncate the
same tables.

The dedicated database and vhost matter: without them, the running compose relay
can claim a test-created outbox row and publish it to the development vhost, where
the in-process test worker is not listening. That was the source of an
intermittent lifecycle-test failure.

Requirements for the test database:

- The configured Postgres user must be able to create a database. The local
  compose user is a superuser, so this works out of the box.
- `DATABASE_URL` can point at another Postgres instance, but a fixed test database
  name will be created there.
- The test vhost is reset once per test binary through the RabbitMQ management
  API. Its URL can be overridden with `RABBIT_MGMT_URL`.

### Enabling test logs

The default test logger discards output. Set `TEST_LOG` to see relay, consumer,
and worker logs:

```powershell
$env:TEST_LOG = "1"
go test ./internal/messaging -run TestFullLifecycleInProcess -count=1 -v
Remove-Item Env:TEST_LOG
```

On bash:

```bash
TEST_LOG=1 go test ./internal/messaging -run TestFullLifecycleInProcess -count=1 -v
```

### Quality gate status

`make test` runs `go test ./...`. The full `make check` gate is part of the
unfinished M5 polish: `lint` is still a placeholder, and the current Windows
`fmt-check` recipe uses POSIX shell syntax. Use `make test`, `go vet ./...`, and
`gofmt -l .` locally until that gate is finished. `make test-race` needs a C
toolchain and is intended for Linux/macOS or the M5 test container.

## Manual verification and fault injection

These scenarios exercise the running compose stack, not the Go test suite.

### Concurrent create and update races

```bash
make race-demo
```

The default is 50 concurrent requests:

1. 50 `POST`s with the same slug: exactly one `201`, 49 `409`.
2. 50 `PATCH`es with the same version: exactly one `202`, 49 `409`.

The database unique constraint and conditional `UPDATE` are the arbiters. No
application-level mutex is involved. Expected summary:

```text
created=1 already_exists=49
accepted=1 rejected=49
```

### Duplicate, stale, and poison updates

Get a task and tenant ID from the API first:

```bash
curl -sS http://localhost:8080/v1/tenants/<tenant-id>/tasks
```

Then publish a fault-injection update through the RabbitMQ management API:

```bash
# Same terminal update twice: inbox makes the second copy a no-op.
scripts/publish-update.sh duplicate <task-id> <tenant-id>

# Late in_progress after done: rank guard prevents regression.
scripts/publish-update.sh stale <task-id> <tenant-id>

# Truncated JSON: consumer dead-letters it and keeps processing.
scripts/publish-update.sh poison <task-id> <tenant-id>
```

On Windows, run these in Git Bash or invoke:

```powershell
& "C:\Program Files\Git\bin\bash.exe" scripts/publish-update.sh duplicate <task-id> <tenant-id>
```

Expected observations:

- Duplicate and stale updates leave the tenant version unchanged.
- Poison increments `cp.task-updates.dlq` by one.
- The consumer continues processing valid messages after the poison message.

### Failure-rate recovery

`FAIL_RATE` applies to every task type, including destroy. This is deliberate:
a broken worker can fail cleanup too.

```bash
make worker-restart FAIL_RATE=1
```

Create a tenant; the deploy task fails and the tenant moves to `failed`. A
DELETE at the same failure rate also fails. To recover:

```bash
make worker-restart FAIL_RATE=0
```

Then DELETE the failed tenant using its current version; the destroy task
succeeds and the tenant becomes `destroyed`.

### Other useful commands

```bash
make logs             # follow all service logs
make worker-restart   # rebuild/recreate only the worker
make fmt              # gofmt -w .
make test             # go test ./...
make down             # remove containers and volumes
```

## Technical design

### Task and tenant state machines

Tasks are forward-only by rank:

| Status | Rank |
|---|---:|
| `accepted` | 0 |
| `in_progress` | 1 |
| `done` | 2 (terminal) |
| `failed` | 2 (terminal) |

A worker update is applied only when `rank(new) > rank(current)`. This gives
idempotency and order tolerance with one comparison. A duplicate terminal update
has the same rank and is stale. A late `in_progress` after `done` is stale.
`accepted -> done` is allowed if `in_progress` was lost; the first terminal
outcome wins.

Tenant transitions:

| From | Event | To |
|---|---|---|
| none | `POST /v1/tenants` | `provisioning` |
| `provisioning` | deploy done | `active` |
| `provisioning` | deploy failed | `failed` |
| `active` | `PATCH` | `updating` |
| `updating` | update done | `active` |
| `updating` | update failed | `failed` |
| `active`, `failed` | `DELETE` | `destroying` |
| `destroying` | destroy done | `destroyed` |
| `destroying` | destroy failed | `failed` |

PATCH is allowed only from `active`. DELETE is allowed from `active` or `failed`.
Rows are never deleted; `destroyed` is a terminal status. That makes the
`UNIQUE(slug)` constraint automatically enforce “unique including destroyed
tenants”.

### Transactional outbox

Each mutating API transaction writes:

1. the tenant row;
2. a task row with `published_at = NULL`;
3. then commits.

A relay loop claims unpublished rows with:

```sql
SELECT ... FROM tasks
WHERE published_at IS NULL
ORDER BY id
LIMIT 100
FOR UPDATE SKIP LOCKED;
```

For each row it publishes to `cp.tasks` with publisher confirms, then sets
`published_at` in the same transaction. A crash before the commit can republish
the task on restart; that is the accepted at-least-once cost. A publish is never
issued before the database commit, and never on rollback. `SKIP LOCKED` lets
multiple control-plane replicas relay without claiming the same row.

### Idempotent consumer and inbox

Worker updates carry a deterministic `message_id`:

```text
UUIDv5(task_id + "/" + status)
```

The consumer transaction:

1. Inserts the message into `processed_messages` with
   `ON CONFLICT DO NOTHING`. If no row was inserted, the update is a duplicate:
   commit and ack as a no-op.
2. Loads the task and resolves its tenant from the database. The message’s
   `tenant_id` is not trusted for state changes.
3. Locks the tenant, then the task. This matches the API path’s lock order and
   avoids ABBA deadlocks.
4. Applies the update only if the task is not terminal and the new rank is
   higher. Otherwise the message is stale: commit and ack.
5. On a terminal update, applies the tenant outcome in the same transaction,
   guarded on the expected in-flight status and incrementing the tenant version.

The inbox row and the state change commit or roll back together. A lost ack
causes redelivery, which becomes a duplicate no-op. A crash before commit leaves
no inbox row, so the update can be applied on retry.

### Messaging topology

| Name | Type | Purpose |
|---|---|---|
| `cp.tasks` | direct exchange | Outbound task routing |
| `worker.tasks` | quorum queue | Task work; bound to `task.deploy`, `task.update`, `task.destroy` |
| `cp.task-updates` | direct exchange | Inbound progress routing |
| `cp.task-updates` | quorum queue | Progress updates; bound to `task.update.<status>`; `x-delivery-limit=5`; dead-letters to the DLQ |
| `cp.task-updates.dlq` | durable queue | Poison messages and exhausted retries |

Publishes use `mandatory=true`. A missing binding produces a broker return and a
publish error instead of a silent drop. The publisher waits for a broker confirm
before reporting success.

### Consumer resilience

| Failure | Handling |
|---|---|
| Malformed JSON or invalid schema | `Nack(requeue=false)` -> DLQ |
| Unknown task/tenant or validation error | Permanent -> DLQ |
| Handler panic | Recovered, then `Nack(requeue=false)` -> DLQ |
| Transient DB error | Up to three in-process attempts with backoff, then requeue |
| Repeated redelivery | `cp.task-updates` delivery limit of 5 -> DLQ |
| AMQP connection drop | Connection manager reconnects with capped backoff and re-declares topology |

### Worker behavior

The worker:

- consumes `worker.tasks` with prefetch 1;
- validates schema version, UUIDs, and task type;
- simulates a delay, then publishes `in_progress` and a terminal update;
- publishes `in_progress` and terminal updates with deterministic IDs;
- acks the task only after the terminal update is confirmed;
- uses a separate consume channel and publish channel;
- reconnects with a 250 ms to 10 s backoff.

Fault-injection settings:

| Setting | Behavior |
|---|---|
| `FAIL_RATE` | Fraction of tasks that end `failed` |
| `DUPLICATE_RATE` | Fraction of terminal updates published twice |
| `REORDER` | Publish terminal before `in_progress` |
| `MIN_DELAY_MS` / `MAX_DELAY_MS` | Simulated processing delay range |

The worker has no database connection. It trusts the task message; the consumer
is the final authority on task existence, tenant mapping, and state transitions.

### Concurrency controls

| Race | Mechanism |
|---|---|
| Two creates with the same slug | `UNIQUE(slug)`; Postgres error `23505` -> `tenant_already_exists` |
| Two PATCHes with the same version | Conditional `UPDATE ... WHERE version = $v AND status = 'active'`; zero rows -> re-read and disambiguate |
| Two DELETE/update lifecycle operations | The same conditional-update pattern and tenant status guards |
| Two open tasks for one tenant | Partial unique index on `(tenant_id)` where task status is `accepted` or `in_progress` |
| Two relay replicas claiming one task | `FOR UPDATE SKIP LOCKED` |
| Duplicate worker update | Deterministic `message_id` + inbox primary key |
| Reordered worker update | Task rank guard |
| Stale terminal outcome | Tenant update guarded on the expected in-flight status |

### Pagination and timestamps

- IDs are UUIDv7, so `id` is time-ordered and can serve as a keyset cursor.
- Lists use `id > cursor`, not `OFFSET`; pages do not drift under concurrent
  writes.
- Timestamps are rendered as `2006-01-02T15:04:05.000Z` (ISO-8601, milliseconds,
  UTC). The wire type deliberately does not use Go’s default `time.Time` JSON
  format.

### Operational behavior

- JSON logs include `task_id`, `tenant_id`, and message IDs where applicable.
- The API has a 5-second `ReadHeaderTimeout` and a 10-second graceful shutdown.
- The worker and API are built as static, non-root, distroless images.
- The worker service has no healthcheck; it exposes no HTTP endpoint.
  `docker compose up --wait` treats a service without a healthcheck as ready once
  it is running.
- RabbitMQ is pinned to 3.13 with the management plugin for local inspection.

## Configuration

Compose supplies the service configuration. The defaults are local-development
only; do not expose these ports or credentials publicly.

### Control plane (`cmd/controlplane`)

| Variable | Default / source | Purpose |
|---|---|---|
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `DATABASE_URL` | Compose Postgres URL | Postgres connection string |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection string |

### Worker (`cmd/worker`)

| Variable | Default | Purpose |
|---|---|---|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection string |
| `FAIL_RATE` | `0` | Fraction of tasks that fail, `0..1` |
| `MIN_DELAY_MS` | `100` | Minimum simulated delay |
| `MAX_DELAY_MS` | `800` | Maximum simulated delay |
| `DUPLICATE_RATE` | `0` | Fraction of terminal updates published twice |
| `REORDER` | `false` | Publish terminal before `in_progress` |

The worker also accepts the corresponding flags: `--fail-rate`,
`--min-delay-ms`, `--max-delay-ms`, `--duplicate-rate`, and `--reorder`.
Precedence is flag > environment variable > built-in default. Invalid
environment values currently fall back silently to the default.

Compose uses these Postgres variables from `.env` if present:

```text
POSTGRES_USER=controlplane
POSTGRES_PASSWORD=localdev
POSTGRES_DB=controlplane
```

## Project layout

```text
.
|-- cmd/
|   |-- controlplane/       API, outbox relay, progress consumer
|   `-- worker/             worker simulator entrypoint
|-- internal/
|   |-- domain/             pure state machines, errors, slug, timestamps
|   |-- store/              Postgres schema and queries
|   |-- service/            use cases and transactions
|   |-- httpapi/            chi routes, handlers, wire types
|   |-- messaging/          AMQP topology, publisher, consumer, reconnect
|   |-- outbox/             outbox relay loop
|   |-- worker/             worker simulator
|   `-- testutil/           test database and advisory lock
|-- scripts/
|   |-- race-demo.sh        concurrent create/update proof
|   `-- publish-update.sh   duplicate/stale/poison injection
|-- docs/
|   |-- PRD.md              requirements and scope
|   `-- DESIGN.md           schema, contracts, decisions
|-- compose.yaml
|-- Dockerfile
|-- Makefile
`-- README.md
```

## Known limitations and hardening work

This is a take-home implementation, not a production deployment:

- No authentication, authorization, TLS, or per-tenant RBAC.
- Local RabbitMQ credentials (`guest`/`guest`) and the default vhost are used by
  Compose. The tests use a separate vhost.
- The schema is applied with idempotent `CREATE ... IF NOT EXISTS` statements.
  There is no migration/versioning tool for evolving an existing database.
- `worker.tasks` has no dead-letter queue, no declared delivery limit, and no
  per-delivery panic recovery. A malformed task is logged and discarded. A
  worker panic can cause repeated redelivery; this is a hardening gap.
- The worker publishes `in_progress` after the simulated delay rather than before
  long-running work. A real provisioner should publish progress first.
- The worker reconnect backoff does not reset after a long successful consume
  period.
- There is no stuck-task reaper, heartbeat/lease, or provider reconciliation.
- `FAIL_RATE=1` fails deploy, update, and destroy tasks. Recover failed tenants by
  lowering the rate and retrying DELETE.
- Metrics and tracing are not implemented; observability is JSON logs plus the
  RabbitMQ management UI.
- There are no dedicated unit tests for `internal/httpapi` or `internal/worker`.
  They are exercised through the live API and the in-process lifecycle test.
- `make check` is not finished: `lint` is a placeholder and the Windows
  `fmt-check` recipe needs a portable shell wrapper. `make test-race` needs a C
  toolchain and is intended for the M5 Linux container or Linux/macOS.

## Further documentation

- [`docs/PRD.md`](docs/PRD.md) — problem statement, functional and non-functional
  requirements, scope.
- [`docs/DESIGN.md`](docs/DESIGN.md) — schema, state machines, API contract,
  messaging contracts, failure matrix, and the full decisions table.

## License

No license has been added.
