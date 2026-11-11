-- Schema for the control plane. Applied idempotently at startup (CREATE ... IF NOT
-- EXISTS) instead of via a migration tool (DESIGN.md says: no goose, one file).
-- See docs/DESIGN.md §2 for the rationale behind each constraint below.

CREATE TABLE IF NOT EXISTS tenants (
    id          uuid PRIMARY KEY,
    slug        text NOT NULL UNIQUE
                  CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    name        text NOT NULL CHECK (btrim(name) <> ''),
    status      text NOT NULL CHECK (status IN
                  ('provisioning','active','updating','destroying','destroyed','failed')),
    version     bigint NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tasks (
    id           uuid PRIMARY KEY,
    tenant_id    uuid NOT NULL REFERENCES tenants(id),
    type         text NOT NULL CHECK (type IN ('deploy','update','destroy')),
    status       text NOT NULL CHECK (status IN ('accepted','in_progress','done','failed')),
    error        text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX IF NOT EXISTS tasks_unpublished_idx ON tasks (id) WHERE published_at IS NULL;

-- Safety net, not the concurrency mechanism (the service's conditional UPDATEs are):
-- at most one open task per tenant.
CREATE UNIQUE INDEX IF NOT EXISTS tasks_one_open_per_tenant
    ON tasks (tenant_id) WHERE status IN ('accepted','in_progress');

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id   text PRIMARY KEY,
    task_id      uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
