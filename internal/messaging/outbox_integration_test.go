package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/outbox"
	"github.com/aashishGithub/controlplane/internal/service"
	"github.com/aashishGithub/controlplane/internal/store"
)

// TestRelayPublishesCommittedTaskOnce proves the happy path: one committed
// tenant/task transaction yields exactly one broker message, the payload matches
// the wire contract, published_at is set, and a second relay pass publishes
// nothing.
func TestRelayPublishesCommittedTaskOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := testPool(t)
	cm := testConn(t)
	svc := service.New(pool)

	tenant, task, err := svc.CreateTenant(ctx, "relay-once", "Relay Once")
	require.NoError(t, err)

	relay := outbox.NewRelay(pool, cm, testLogger())
	published, err := relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, published)

	ch := testChannel(t, cm)
	defer ch.Close()
	delivery := requireMessage(t, ch, messaging.QueueWorkerTasks, 5*time.Second)

	var wire messaging.TaskMsg
	require.NoError(t, json.Unmarshal(delivery.Body, &wire))
	require.Equal(t, messaging.SchemaVersion, wire.SchemaVersion)
	require.Equal(t, task.ID.String(), wire.ID)
	require.Equal(t, tenant.ID.String(), wire.TenantID)
	require.Equal(t, string(domain.TaskDeploy), wire.Type)
	require.Equal(t, string(domain.TaskAccepted), wire.Status)
	require.Equal(t, tenant.Slug, wire.Tenant.Slug)
	require.Equal(t, tenant.Name, wire.Tenant.Name)
	require.NotEmpty(t, wire.CreatedAt)

	stored, err := store.GetTaskByID(ctx, pool, task.ID)
	require.NoError(t, err)
	require.NotNil(t, stored.PublishedAt, "confirmed delivery must mark the outbox row")

	again, err := relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, again)
	requireNoMessage(t, ch, messaging.QueueWorkerTasks)
}

// TestRelayRollbackPublishesNothing proves the outbox invariant: the relay only
// sees committed rows, so a rolled-back request transaction yields no event.
func TestRelayRollbackPublishesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := testPool(t)
	cm := testConn(t)
	relay := outbox.NewRelay(pool, cm, testLogger())

	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	err := store.WithTx(ctx, pool, func(db store.DB) error {
		if err := store.CreateTenant(ctx, db, store.Tenant{
			ID:      tenantID,
			Slug:    "rollback-slug",
			Name:    "Rollback",
			Status:  domain.TenantProvisioning,
			Version: 1,
		}); err != nil {
			return err
		}
		if err := store.CreateTask(ctx, db, store.Task{
			ID:       taskID,
			TenantID: tenantID,
			Type:     domain.TaskDeploy,
			Status:   domain.TaskAccepted,
		}); err != nil {
			return err
		}
		return errors.New("forced rollback")
	})
	require.Error(t, err)

	_, err = store.GetTenantByID(ctx, pool, tenantID)
	require.ErrorIs(t, err, store.ErrNoRows)
	_, err = store.GetTaskByID(ctx, pool, taskID)
	require.ErrorIs(t, err, store.ErrNoRows)

	published, err := relay.RunOnce(ctx)
	require.NoError(t, err)
	require.Zero(t, published)

	ch := testChannel(t, cm)
	defer ch.Close()
	requireNoMessage(t, ch, messaging.QueueWorkerTasks)
}

// TestRelayClaimSkipsLockedRows proves FOR UPDATE SKIP LOCKED at the DB level:
// while one transaction holds the claimed row, a second claim in another live
// transaction must return zero rows instead of double-claiming it.
func TestRelayClaimSkipsLockedRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := testPool(t)
	svc := service.New(pool)
	_, _, err := svc.CreateTenant(ctx, "skip-locked", "Skip Locked")
	require.NoError(t, err)

	first, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer first.Rollback(ctx)

	claimedFirst, err := store.ClaimUnpublishedTasks(ctx, first, 10)
	require.NoError(t, err)
	require.Len(t, claimedFirst, 1)

	second, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer second.Rollback(ctx)

	claimedSecond, err := store.ClaimUnpublishedTasks(ctx, second, 10)
	require.NoError(t, err)
	require.Empty(t, claimedSecond, "a concurrently locked row must be skipped, not double-claimed")
}
