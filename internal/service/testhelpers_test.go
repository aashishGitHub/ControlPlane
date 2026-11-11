package service_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aashishGithub/controlplane/internal/testutil"
)

// These are integration tests: they need the real Postgres from `make up`
// (DESIGN.md's concurrency guarantees are a DB-level property - a mock can't
// prove a unique-index race or a row-lock race, only Postgres can). testutil
// opens a dedicated controlplane_test database so the running compose stack can
// share the host without sharing state.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return testutil.OpenDB(t)
}

func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	testutil.TruncateAll(t, pool)
}
