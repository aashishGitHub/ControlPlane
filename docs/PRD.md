# PRD — Tenant Provisioning Control Plane

## 1. Problem

Tenants are provisioned asynchronously: the control plane records a tenant and emits a
task; a worker pool does the actual provisioning (minutes, not milliseconds) and reports
progress back over a broker. The control plane is the single source of truth for tenant
and task state, and must stay correct under concurrent requests, worker retries, and
out-of-order or duplicate delivery.

## 2. Personas

- **Signup service** — calls the tenant API synchronously on customer signup; needs a
  fast, unambiguous response and a durable record even if it never checks back.
- **Worker fleet** — consumes tasks, does long-running work, reports progress; may crash,
  retry, redeliver, or run several replicas at once.
- **Platform operator** — queries tenant/task state for support and debugging; needs
  structured errors and an audit-friendly task history.

## 3. Goals

- Correct, race-free tenant/task state machines (the brief's primary grading axis).
- At-least-once messaging in both directions, made safe by idempotency and ordering
  rules on the consumer side, not by trying to achieve exactly-once.
- One boot command, one test command, local-only dependencies.
- Explainable by a single engineer in a live walkthrough — every non-obvious line
  traces to a named pattern and a decision below.

## 4. Non-goals (out of scope)

- AuthN/AuthZ, multi-region, autoscaling the worker fleet.
- A reconciler/reaper for tasks stuck mid-flight (noted as future work, §8).
- Retrying from `failed` (the brief only requires `failed → destroying`).
- Exactly-once delivery — explicitly impossible per the brief's tips; we design for
  at-least-once + idempotent apply instead.

## 5. Functional requirements

| ID | Requirement |
|----|-------------|
| FR-1 | `POST /v1/tenants` creates a tenant (`provisioning`) and a `deploy` task, atomically, exactly-once per unique slug. |
| FR-2 | `GET /v1/tenants`, `GET /v1/tenants/{id}` — list/read, including `destroyed` tenants. |
| FR-3 | `PATCH /v1/tenants/{id}` updates `name`, only from `active`, requires `version`, moves tenant to `updating`, creates an `update` task. |
| FR-4 | `DELETE /v1/tenants/{id}` only from `active` or `failed`, moves tenant to `destroying`, creates a `destroy` task. |
| FR-5 | `GET /v1/tasks`, `GET /v1/tasks/{id}`, `GET /v1/tenants/{id}/tasks` — read-only task access. |
| FR-6 | Every mutating call returns a structured error from the fixed vocabulary: `tenant_not_found`, `tenant_already_exists`, `tenant_update_not_allowed`, `tenant_version_conflict`, `task_not_found`. |
| FR-7 | Every state-changing call publishes exactly one task event, after commit, never on rollback. |
| FR-8 | The consumer applies worker progress idempotently, order-tolerantly, and survives transient broker failures and poison messages without dying. |
| FR-9 | A worker simulator drives `accepted → in_progress → done\|failed` with configurable delay and failure rate, restart-tunable. |

## 6. Non-functional requirements

| ID | Requirement |
|----|-------------|
| NFR-1 | Two concurrent `PATCH`es with the same `version` — exactly one succeeds. |
| NFR-2 | Two concurrent `POST`es with the same `slug` — exactly one succeeds. |
| NFR-3 | Tenants survive a process restart (durable Postgres storage). |
| NFR-4 | Single `make up` boots Postgres, RabbitMQ, the control plane, and the worker. |
| NFR-5 | `make check` runs formatting, linting, a security scanner, and the test suite. |
| NFR-6 | No secrets committed; local credentials only, via `.env` (git-ignored). |

## 7. Multi-tenant considerations

(Design background from operating a multi-tenant control plane — Couchbase Capella-style.)

- **One in-flight operation per tenant.** Enforced by the tenant status guards
  (`active`→`updating`, etc.) plus a partial unique index as a belt-and-suspenders
  invariant — never by an external lock service. See DESIGN.md §4.6.
- **Slug tombstoning.** Tenant rows are never hard-deleted; `destroyed` is a terminal
  status, and the `UNIQUE(slug)` constraint on a never-deleted row set is what makes
  "unique including against destroyed tenants" trivially true.
- **Desired state vs. observed status.** `PATCH` writes the desired `name` immediately;
  `status` reflects the in-progress reconciliation driven by the worker. This mirrors
  Kubernetes spec/status split and Capella's "config" vs. "deployment status" model.
- **Tenant-scoped observability.** Every log line and error carries `tenant_id` and
  `task_id`, so one noisy or broken tenant is debuggable without grepping global logs.
- **Blast radius.** A worker crash loses at most the in-flight task (it's redelivered);
  a control-plane restart loses nothing (Postgres is durable, the outbox recovers
  unpublished tasks).

## 8. Future work (explicitly deferred)

- A reconciler that re-publishes tasks stuck in `accepted`/`in_progress` past a TTL.
- Retry-from-`failed` for `deploy`/`update` (today only `destroy` is allowed from `failed`).
- Per-tenant rate limiting and worker sharding/consistent hashing for noisy-neighbor isolation.
- AuthN/AuthZ and per-tenant RBAC.

## 9. Success criteria / traceability

Every FR/NFR above maps to at least one automated test; see DESIGN.md's test section
and the milestone table in `plan.md`. "Done" = `make check` green + the manual
verification script in `plan.md` §Verification passes on a cold clone.
