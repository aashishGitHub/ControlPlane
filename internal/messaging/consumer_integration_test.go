package messaging_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/service"
	"github.com/aashishGithub/controlplane/internal/store"
)

func testApply(t *testing.T, svc *service.Service, taskID uuid.UUID, status domain.TaskStatus, reason *string) service.ApplyResult {
	t.Helper()
	result, err := svc.ApplyTaskUpdate(context.Background(), service.TaskUpdate{
		MessageID: messaging.NewMessageID(taskID, string(status)),
		TaskID:    taskID,
		Status:    status,
		Reason:    reason,
	})
	require.NoError(t, err)
	return result
}

// TestApplyTaskUpdateDuplicateIsNoOp proves the inbox half of effectively-once:
// the same deterministic message_id twice applies once, and the tenant version
// advances exactly once.
func TestApplyTaskUpdateDuplicateIsNoOp(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	svc := service.New(pool)

	tenant, task, err := svc.CreateTenant(ctx, "duplicate-update", "Duplicate")
	require.NoError(t, err)

	update := service.TaskUpdate{
		MessageID: messaging.NewMessageID(task.ID, string(domain.TaskDone)),
		TaskID:    task.ID,
		Status:    domain.TaskDone,
	}

	result, err := svc.ApplyTaskUpdate(ctx, update)
	require.NoError(t, err)
	require.Equal(t, service.ApplyApplied, result)

	afterFirst, err := svc.GetTenant(ctx, tenant.ID)
	require.NoError(t, err)
	require.Equal(t, domain.TenantActive, afterFirst.Status)
	require.Equal(t, tenant.Version+1, afterFirst.Version, "terminal outcome bumps the version once")

	result, err = svc.ApplyTaskUpdate(ctx, update)
	require.NoError(t, err)
	require.Equal(t, service.ApplyDuplicate, result)

	afterSecond, err := svc.GetTenant(ctx, tenant.ID)
	require.NoError(t, err)
	require.Equal(t, afterFirst.Version, afterSecond.Version, "duplicate must not bump the version again")

	storedTask, err := svc.GetTask(ctx, task.ID)
	require.NoError(t, err)
	require.Equal(t, domain.TaskDone, storedTask.Status)
}

// TestApplyTaskUpdateStaleDoesNotRegress proves the rank half: a reordered
// in_progress update arriving after done is acked as stale and changes nothing.
func TestApplyTaskUpdateStaleDoesNotRegress(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	svc := service.New(pool)

	tenant, task, err := svc.CreateTenant(ctx, "stale-update", "Stale")
	require.NoError(t, err)

	require.Equal(t, service.ApplyApplied, testApply(t, svc, task.ID, domain.TaskDone, nil))

	result, err := svc.ApplyTaskUpdate(ctx, service.TaskUpdate{
		MessageID: messaging.NewMessageID(task.ID, string(domain.TaskInProgress)),
		TaskID:    task.ID,
		Status:    domain.TaskInProgress,
	})
	require.NoError(t, err)
	require.Equal(t, service.ApplyStale, result)

	storedTask, err := svc.GetTask(ctx, task.ID)
	require.NoError(t, err)
	require.Equal(t, domain.TaskDone, storedTask.Status)

	storedTenant, err := svc.GetTenant(ctx, tenant.ID)
	require.NoError(t, err)
	require.Equal(t, domain.TenantActive, storedTenant.Status)
	require.Equal(t, tenant.Version+1, storedTenant.Version)
}

// TestApplyTaskUpdateTerminalMappings walks every done/failed task-type mapping
// from DESIGN.md §3, including the version bumps on both the API and consumer
// paths.
func TestApplyTaskUpdateTerminalMappings(t *testing.T) {
	pool := testPool(t)
	svc := service.New(pool)
	ctx := context.Background()

	t.Run("deploy done activates", func(t *testing.T) {
		truncateAll(t, pool)
		tenant, task, err := svc.CreateTenant(ctx, "deploy-done", "Deploy Done")
		require.NoError(t, err)

		require.Equal(t, service.ApplyApplied, testApply(t, svc, task.ID, domain.TaskDone, nil))

		stored, err := svc.GetTenant(ctx, tenant.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TenantActive, stored.Status)
		require.Equal(t, int64(2), stored.Version)
	})

	t.Run("deploy failed stores reason", func(t *testing.T) {
		truncateAll(t, pool)
		tenant, task, err := svc.CreateTenant(ctx, "deploy-failed", "Deploy Failed")
		require.NoError(t, err)

		reason := "worker exploded"
		require.Equal(t, service.ApplyApplied, testApply(t, svc, task.ID, domain.TaskFailed, &reason))

		stored, err := svc.GetTenant(ctx, tenant.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TenantFailed, stored.Status)
		require.Equal(t, int64(2), stored.Version)

		storedTask, err := svc.GetTask(ctx, task.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TaskFailed, storedTask.Status)
		require.NotNil(t, storedTask.Error)
		require.Equal(t, reason, *storedTask.Error)
	})

	t.Run("update done reactivates", func(t *testing.T) {
		truncateAll(t, pool)
		tenant, deployTask, err := svc.CreateTenant(ctx, "update-done", "Update Done")
		require.NoError(t, err)
		require.Equal(t, service.ApplyApplied, testApply(t, svc, deployTask.ID, domain.TaskDone, nil))

		tenant, updateTask, err := svc.UpdateTenant(ctx, tenant.ID, "Renamed", 2)
		require.NoError(t, err)
		require.Equal(t, domain.TenantUpdating, tenant.Status)

		require.Equal(t, service.ApplyApplied, testApply(t, svc, updateTask.ID, domain.TaskDone, nil))

		stored, err := svc.GetTenant(ctx, tenant.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TenantActive, stored.Status)
		require.Equal(t, int64(4), stored.Version)
	})

	t.Run("update failed fails", func(t *testing.T) {
		truncateAll(t, pool)
		tenant, deployTask, err := svc.CreateTenant(ctx, "update-failed", "Update Failed")
		require.NoError(t, err)
		require.Equal(t, service.ApplyApplied, testApply(t, svc, deployTask.ID, domain.TaskDone, nil))

		tenant, updateTask, err := svc.UpdateTenant(ctx, tenant.ID, "Renamed", 2)
		require.NoError(t, err)
		require.Equal(t, domain.TenantUpdating, tenant.Status)

		reason := "update task failed"
		require.Equal(t, service.ApplyApplied, testApply(t, svc, updateTask.ID, domain.TaskFailed, &reason))

		stored, err := svc.GetTenant(ctx, tenant.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TenantFailed, stored.Status)
		require.Equal(t, int64(4), stored.Version)
	})

	t.Run("destroy done destroys", func(t *testing.T) {
		truncateAll(t, pool)
		tenant, deployTask, err := svc.CreateTenant(ctx, "destroy-done", "Destroy Done")
		require.NoError(t, err)
		require.Equal(t, service.ApplyApplied, testApply(t, svc, deployTask.ID, domain.TaskDone, nil))

		tenant, destroyTask, err := svc.DeleteTenant(ctx, tenant.ID, 2)
		require.NoError(t, err)
		require.Equal(t, domain.TenantDestroying, tenant.Status)

		require.Equal(t, service.ApplyApplied, testApply(t, svc, destroyTask.ID, domain.TaskDone, nil))

		stored, err := svc.GetTenant(ctx, tenant.ID)
		require.NoError(t, err)
		require.Equal(t, domain.TenantDestroyed, stored.Status)
		require.Equal(t, int64(4), stored.Version)
	})
}

// TestApplyTaskUpdateDropsOutcomeWhenTenantMovedOn proves the expected-status
// guard: a terminal task update still succeeds, but the tenant outcome is dropped
// when the tenant is no longer in the type's in-flight status.
func TestApplyTaskUpdateDropsOutcomeWhenTenantMovedOn(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	svc := service.New(pool)

	tenant, deployTask, err := svc.CreateTenant(ctx, "guarded-outcome", "Guarded")
	require.NoError(t, err)
	require.Equal(t, service.ApplyApplied, testApply(t, svc, deployTask.ID, domain.TaskDone, nil))

	strayTask := store.Task{
		ID:       uuid.Must(uuid.NewV7()),
		TenantID: tenant.ID,
		Type:     domain.TaskDeploy,
		Status:   domain.TaskAccepted,
	}
	require.NoError(t, store.CreateTask(ctx, pool, strayTask))

	require.Equal(t, service.ApplyApplied, testApply(t, svc, strayTask.ID, domain.TaskFailed, nil))

	storedTenant, err := svc.GetTenant(ctx, tenant.ID)
	require.NoError(t, err)
	require.Equal(t, domain.TenantActive, storedTenant.Status, "stale deploy outcome must not overwrite active")
	require.Equal(t, int64(2), storedTenant.Version)

	storedTask, err := svc.GetTask(ctx, strayTask.ID)
	require.NoError(t, err)
	require.Equal(t, domain.TaskFailed, storedTask.Status)
}

// TestConsumerDeadLettersPoisonAndKeepsProcessing is the M3 exit criterion: bad
// JSON and an unknown task each land in the DLQ, and a following valid update is
// still applied by the same consumer loop.
func TestConsumerDeadLettersPoisonAndKeepsProcessing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := testPool(t)
	cm := testConn(t)
	svc := service.New(pool)

	tenant, task, err := svc.CreateTenant(ctx, "poison-consumer", "Poison")
	require.NoError(t, err)

	go messaging.ConsumeUpdates(ctx, cm, testLogger(), func(ctx context.Context, update messaging.TaskUpdate) error {
		_, err := svc.ApplyTaskUpdate(ctx, service.TaskUpdate{
			MessageID: update.MessageID,
			TaskID:    update.TaskID,
			Status:    update.Status,
			Reason:    update.Reason,
		})
		return err
	})

	ch := testChannel(t, cm)
	defer ch.Close()

	require.NoError(t, ch.PublishWithContext(ctx, messaging.ExchangeTaskUpdates,
		messaging.UpdateRoutingKey(string(domain.TaskDone)), false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(`{"schema_version":1,`),
		}))
	badJSON := requireMessage(t, ch, messaging.QueueTaskUpdatesDLQ, 5*time.Second)
	require.Equal(t, `{"schema_version":1,`, string(badJSON.Body))

	unknownTaskID := uuid.Must(uuid.NewV7())
	unknown := messaging.TaskUpdateMsg{
		SchemaVersion: messaging.SchemaVersion,
		MessageID:     messaging.NewMessageID(unknownTaskID, string(domain.TaskDone)),
		TaskID:        unknownTaskID.String(),
		TenantID:      tenant.ID.String(),
		Status:        string(domain.TaskDone),
		OccurredAt:    domain.FormatTimestamp(time.Now()),
	}
	body, err := json.Marshal(unknown)
	require.NoError(t, err)
	require.NoError(t, ch.PublishWithContext(ctx, messaging.ExchangeTaskUpdates,
		messaging.UpdateRoutingKey(unknown.Status), false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		}))
	unknownDelivery := requireMessage(t, ch, messaging.QueueTaskUpdatesDLQ, 5*time.Second)
	require.Equal(t, body, unknownDelivery.Body)

	valid := messaging.TaskUpdateMsg{
		SchemaVersion: messaging.SchemaVersion,
		MessageID:     messaging.NewMessageID(task.ID, string(domain.TaskDone)),
		TaskID:        task.ID.String(),
		TenantID:      tenant.ID.String(),
		Status:        string(domain.TaskDone),
		OccurredAt:    domain.FormatTimestamp(time.Now()),
	}
	require.NoError(t, ch.PublishWithContext(ctx, messaging.ExchangeTaskUpdates,
		messaging.UpdateRoutingKey(valid.Status), false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        mustJSON(t, valid),
		}))

	require.Eventually(t, func() bool {
		storedTask, err := svc.GetTask(ctx, task.ID)
		return err == nil && storedTask.Status == domain.TaskDone
	}, 5*time.Second, 25*time.Millisecond, "consumer must keep processing after poison messages")

	storedTenant, err := svc.GetTenant(ctx, tenant.ID)
	require.NoError(t, err)
	require.Equal(t, domain.TenantActive, storedTenant.Status)
}

// TestTopologyRoutesBothExchanges proves every binding the relay and worker rely
// on: task.<type> reaches worker.tasks, and task.update.<status> reaches
// cp.task-updates. A missing binding would still look like a successful publish
// (confirms are not routing guarantees), so this test is the safety net.
func TestTopologyRoutesBothExchanges(t *testing.T) {
	ctx := context.Background()
	testPool(t)
	cm := testConn(t)
	ch := testChannel(t, cm)
	defer ch.Close()

	publisher, err := messaging.NewPublisher(ch)
	require.NoError(t, err)

	taskMsg := messaging.TaskMsg{
		SchemaVersion: messaging.SchemaVersion,
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          string(domain.TaskDeploy),
		TenantID:      uuid.Must(uuid.NewV7()).String(),
		Status:        string(domain.TaskAccepted),
		CreatedAt:     domain.FormatTimestamp(time.Now()),
	}
	taskMsg.Tenant.Slug = "routing"
	taskMsg.Tenant.Name = "Routing"
	require.NoError(t, publisher.PublishTask(ctx, taskMsg))
	taskDelivery := requireMessage(t, ch, messaging.QueueWorkerTasks, 5*time.Second)
	var routedTask messaging.TaskMsg
	require.NoError(t, json.Unmarshal(taskDelivery.Body, &routedTask))
	require.Equal(t, taskMsg.ID, routedTask.ID)

	updateMsg := messaging.TaskUpdateMsg{
		SchemaVersion: messaging.SchemaVersion,
		MessageID:     messaging.NewMessageID(uuid.Must(uuid.NewV7()), string(domain.TaskInProgress)),
		TaskID:        uuid.Must(uuid.NewV7()).String(),
		TenantID:      uuid.Must(uuid.NewV7()).String(),
		Status:        string(domain.TaskInProgress),
		OccurredAt:    domain.FormatTimestamp(time.Now()),
	}
	require.NoError(t, publisher.PublishUpdate(ctx, updateMsg))
	updateDelivery := requireMessage(t, ch, messaging.QueueTaskUpdates, 5*time.Second)
	var routedUpdate messaging.TaskUpdateMsg
	require.NoError(t, json.Unmarshal(updateDelivery.Body, &routedUpdate))
	require.Equal(t, updateMsg.MessageID, routedUpdate.MessageID)

	unroutable := taskMsg
	unroutable.Type = "not-a-task-type"
	require.Error(t, publisher.PublishTask(ctx, unroutable),
		"mandatory publish must surface an unroutable message instead of silently dropping it")
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	body, err := json.Marshal(v)
	require.NoError(t, err)
	return body
}
