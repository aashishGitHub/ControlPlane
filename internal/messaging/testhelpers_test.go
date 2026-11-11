package messaging_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/store"
)

// integrationLockKey must match internal/service/testhelpers_test.go: `go test
// ./...` runs packages in parallel and every integration test truncates the same
// tables. One cross-package advisory lock keeps them serialized.
const integrationLockKey int64 = 0x4d334d33

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

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
	acquireIntegrationLock(t, pool)
	truncateAll(t, pool)
	return pool
}

func acquireIntegrationLock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, integrationLockKey)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, integrationLockKey)
		conn.Release()
	})
}

func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `TRUNCATE tasks, processed_messages, tenants`)
	require.NoError(t, err)
}

func testConn(t *testing.T) *messaging.ConnManager {
	t.Helper()
	url := os.Getenv("AMQP_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	cm, err := messaging.NewConnManager(messaging.Config{
		URL:               url,
		ReconnectInterval: 250 * time.Millisecond,
		Log:               testLogger(),
	})
	require.NoError(t, err, "is `make up` running? RabbitMQ must be reachable at %s", url)
	t.Cleanup(func() { _ = cm.Close() })

	ch := testChannel(t, cm)
	defer ch.Close()
	for _, queue := range []string{messaging.QueueWorkerTasks, messaging.QueueTaskUpdates, messaging.QueueTaskUpdatesDLQ} {
		_, err := ch.QueuePurge(queue, false)
		require.NoError(t, err)
	}
	return cm
}

func testChannel(t *testing.T, cm *messaging.ConnManager) *amqp.Channel {
	t.Helper()
	ch, err := cm.Channel()
	require.NoError(t, err)
	return ch
}

// requireMessage polls the queue until one message arrives. ch.Get is used
// deliberately: the tests consume from the DLQ/main queue explicitly instead of
// relying on a background consumer and racing it.
func requireMessage(t *testing.T, ch *amqp.Channel, queue string, timeout time.Duration) amqp.Delivery {
	t.Helper()
	var got amqp.Delivery
	require.Eventually(t, func() bool {
		delivery, ok, err := ch.Get(queue, true)
		if err != nil {
			return false
		}
		if ok {
			got = delivery
			return true
		}
		return false
	}, timeout, 25*time.Millisecond, "expected a message on %s", queue)
	return got
}

func requireNoMessage(t *testing.T, ch *amqp.Channel, queue string) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	delivery, ok, err := ch.Get(queue, true)
	require.NoError(t, err)
	require.False(t, ok, "unexpected message on %s: %s", queue, delivery.Body)
}
