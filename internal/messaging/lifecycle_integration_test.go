package messaging_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/outbox"
	"github.com/aashishGithub/controlplane/internal/service"
	"github.com/aashishGithub/controlplane/internal/worker"
)

// TestFullLifecycleInProcess is the M4 exit criterion: one test boots the relay,
// consumer and worker in-process, then drives create -> active -> patch -> active
// -> delete -> destroyed -> re-create(409) with no manual steps.
func TestFullLifecycleInProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := testPool(t)
	cm := testConn(t)

	relay := outbox.NewRelay(pool, cm, testLogger())
	svc := service.New(pool, service.WithNotifier(relay.Notify))

	go relay.Run(ctx)
	go messaging.ConsumeUpdates(ctx, cm, testLogger(), func(ctx context.Context, update messaging.TaskUpdate) error {
		_, err := svc.ApplyTaskUpdate(ctx, service.TaskUpdate{
			MessageID: update.MessageID,
			TaskID:    update.TaskID,
			Status:    update.Status,
			Reason:    update.Reason,
		})
		return err
	})

	w, err := worker.New(cm, testLogger(), worker.Options{
		FailRate:      0,
		MinDelay:      0,
		MaxDelay:      5 * time.Millisecond,
		DuplicateRate: 0,
		Reorder:       false,
	})
	require.NoError(t, err)
	go w.Run(ctx)

	const slug = "lifecycle-tenant"
	tenant, deployTask, err := svc.CreateTenant(ctx, slug, "Lifecycle")
	require.NoError(t, err)
	require.Equal(t, domain.TenantProvisioning, tenant.Status)

	require.Eventually(t, func() bool {
		current, err := svc.GetTenant(ctx, tenant.ID)
		return err == nil && current.Status == domain.TenantActive
	}, 15*time.Second, 25*time.Millisecond, "deploy task must drive provisioning -> active")
	require.Eventually(t, func() bool {
		task, err := svc.GetTask(ctx, deployTask.ID)
		return err == nil && task.Status == domain.TaskDone
	}, 5*time.Second, 25*time.Millisecond, "deploy task must be done")

	tenant, err = svc.GetTenant(ctx, tenant.ID)
	require.NoError(t, err)
	tenant, updateTask, err := svc.UpdateTenant(ctx, tenant.ID, "Lifecycle Renamed", tenant.Version)
	require.NoError(t, err)
	require.Equal(t, domain.TenantUpdating, tenant.Status)

	require.Eventually(t, func() bool {
		current, err := svc.GetTenant(ctx, tenant.ID)
		return err == nil && current.Status == domain.TenantActive && current.Name == "Lifecycle Renamed"
	}, 15*time.Second, 25*time.Millisecond, "update task must drive updating -> active")
	require.Eventually(t, func() bool {
		task, err := svc.GetTask(ctx, updateTask.ID)
		return err == nil && task.Status == domain.TaskDone
	}, 5*time.Second, 25*time.Millisecond, "update task must be done")

	tenant, err = svc.GetTenant(ctx, tenant.ID)
	require.NoError(t, err)
	tenant, destroyTask, err := svc.DeleteTenant(ctx, tenant.ID, tenant.Version)
	require.NoError(t, err)
	require.Equal(t, domain.TenantDestroying, tenant.Status)

	require.Eventually(t, func() bool {
		current, err := svc.GetTenant(ctx, tenant.ID)
		return err == nil && current.Status == domain.TenantDestroyed
	}, 15*time.Second, 25*time.Millisecond, "destroy task must drive destroying -> destroyed")
	require.Eventually(t, func() bool {
		task, err := svc.GetTask(ctx, destroyTask.ID)
		return err == nil && task.Status == domain.TaskDone
	}, 5*time.Second, 25*time.Millisecond, "destroy task must be done")

	_, _, err = svc.CreateTenant(ctx, slug, "Lifecycle Again")
	var domainErr *domain.Error
	require.True(t, errors.As(err, &domainErr))
	require.Equal(t, domain.ErrTenantAlreadyExists, domainErr.Code,
		"destroyed tenants are tombstoned, so the slug stays taken")
}
