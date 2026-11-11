package service_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/store"
)

// These are integration tests: they need the real Postgres from `make up`
// (DESIGN.md's concurrency guarantees are a DB-level property - a mock can't
// prove a unique-index race or a row-lock race, only Postgres can). Connect via
// DATABASE_URL, defaulting to the compose.yaml host-mapped port so `go test`
// works straight after `make up` with no extra setup.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = "postgres://controlplane:localdev@localhost:5432/controlplane?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := store.Open(ctx, url)
	require.NoError(t, err, "is `make up` running? Postgres must be reachable at %s", url)

	t.Cleanup(pool.Close)
	return pool
}

// truncateAll resets state between tests. Order matters: tasks references
// tenants.
func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `TRUNCATE tasks, processed_messages, tenants`)
	require.NoError(t, err)
}
