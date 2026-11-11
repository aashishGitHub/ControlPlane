package messaging_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/testutil"
)

// testVhost isolates the broker tests from a running `make up` stack. The real
// controlplane/worker consume from "/", so sharing it would make these tests race
// a production consumer for messages (and flake intermittently).
const testVhost = "controlplane_test"

var (
	vhostOnce sync.Once
	vhostErr  error
)

func testLogger() *slog.Logger {
	if os.Getenv("TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := testutil.OpenDB(t)
	truncateAll(t, pool)
	return pool
}

func truncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	testutil.TruncateAll(t, pool)
}

func testConn(t *testing.T) *messaging.ConnManager {
	t.Helper()
	vhostOnce.Do(func() { vhostErr = resetTestVhost() })
	require.NoError(t, vhostErr, "failed to create RabbitMQ test vhost %q", testVhost)

	amqpURL := testAMQPURL(t)
	cm, err := messaging.NewConnManager(messaging.Config{
		URL:               amqpURL,
		ReconnectInterval: 250 * time.Millisecond,
		Log:               testLogger(),
	})
	require.NoError(t, err, "is `make up` running? RabbitMQ must be reachable at %s", amqpURL)
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

func amqpBaseURL() string {
	raw := os.Getenv("AMQP_URL")
	if raw == "" {
		raw = "amqp://guest:guest@localhost:5672/"
	}
	return raw
}

func testAMQPURL(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(amqpBaseURL())
	require.NoError(t, err)
	u.Path = "/" + testVhost
	return u.String()
}

// resetTestVhost recreates a dedicated vhost via the management HTTP API. The
// default "/" vhost is left alone: it belongs to the running compose stack.
// RABBIT_MGMT_URL overrides the default http://localhost:15672.
func resetTestVhost() error {
	u, err := url.Parse(amqpBaseURL())
	if err != nil {
		return err
	}
	username := "guest"
	password := "guest"
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			username = name
		}
		if pass, ok := u.User.Password(); ok {
			password = pass
		}
	}

	mgmtURL := os.Getenv("RABBIT_MGMT_URL")
	if mgmtURL == "" {
		mgmtURL = "http://localhost:15672"
	}
	client := &http.Client{Timeout: 5 * time.Second}

	do := func(method, path, body string, allowed ...int) error {
		req, err := http.NewRequest(method, mgmtURL+path, bytes.NewBufferString(body))
		if err != nil {
			return err
		}
		req.SetBasicAuth(username, password)
		if body != "" {
			req.Header.Set("content-type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		for _, status := range allowed {
			if resp.StatusCode == status {
				return nil
			}
		}
		payload, _ := io.ReadAll(resp.Body)
		return &httpError{method: method, path: path, status: resp.StatusCode, body: string(payload)}
	}

	if err := do(http.MethodDelete, "/api/vhosts/"+testVhost, "", http.StatusNoContent, http.StatusNotFound); err != nil {
		return err
	}
	if err := do(http.MethodPut, "/api/vhosts/"+testVhost, "", http.StatusCreated, http.StatusNoContent); err != nil {
		return err
	}
	permissions := `{"configure":".*","write":".*","read":".*"}`
	permissionPath := "/api/permissions/" + testVhost + "/" + url.PathEscape(username)
	return do(http.MethodPut, permissionPath, permissions, http.StatusCreated, http.StatusNoContent)
}

type httpError struct {
	method string
	path   string
	status int
	body   string
}

func (e *httpError) Error() string {
	return e.method + " " + e.path + ": unexpected status " + http.StatusText(e.status) + ": " + e.body
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
