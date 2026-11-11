// Package testutil holds integration-test infrastructure shared by test
// packages: a dedicated database and a cross-package advisory lock.
//
// Why a separate database: `make up` runs the real controlplane, whose outbox
// relay polls the same Postgres the tests use. Sharing the dev database would let
// the running relay claim test-created outbox rows and publish them to the dev
// broker, making tests intermittently hang. Tests therefore run against
// controlplane_test, while the compose stack stays on controlplane.
package testutil

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/store"
)

const (
	testDatabase = "controlplane_test"
	// advisoryLockKey serializes tests across packages: `go test ./...` runs
	// packages in parallel and they all truncate the same tables.
	advisoryLockKey int64 = 0x4d334d33
)

// OpenDB opens (creating if needed) the dedicated test database, applies the
// schema, and holds a cross-package advisory lock for the duration of the test.
func OpenDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	base := databaseURL()
	ensureDatabase(t, base)

	ctx := context.Background()
	pool, err := store.Open(ctx, withDatabase(t, base, testDatabase))
	require.NoError(t, err, "is `make up` running? Postgres must be reachable at %s", base)

	t.Cleanup(pool.Close)
	acquireLock(t, pool)
	return pool
}

// TruncateAll resets state between tests. Order matters: tasks references tenants.
// processed_messages has no FK by design, but is truncated in the same statement.
func TruncateAll(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `TRUNCATE tasks, processed_messages, tenants`)
	require.NoError(t, err)
}

func databaseURL() string {
	raw := os.Getenv("DATABASE_URL")
	if raw == "" {
		raw = "postgres://controlplane:localdev@localhost:5432/controlplane?sslmode=disable"
	}
	return raw
}

func withDatabase(t testing.TB, raw, name string) string {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}

func ensureDatabase(t testing.TB, base string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adminURL := withDatabase(t, base, "postgres")
	conn, err := pgx.Connect(ctx, adminURL)
	require.NoError(t, err, "is `make up` running? Postgres must be reachable at %s", adminURL)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `CREATE DATABASE "`+testDatabase+`"`)
	if err == nil {
		return
	}
	var pgErr *pgconn.PgError
	// 42P04 duplicate_database: another test package created it first.
	require.True(t, errors.As(err, &pgErr) && pgErr.Code == "42P04",
		"create test database %s: %v", testDatabase, err)
}

func acquireLock(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
		conn.Release()
	})
}
