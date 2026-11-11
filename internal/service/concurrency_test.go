package service_test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/service"
)

// TestCreateTenantSlugRace proves DESIGN.md §5's claim: the unique index, not an
// app-level lock, is the arbiter of the create-slug race. 50 goroutines race to
// create the same slug; exactly one must win.
func TestCreateTenantSlugRace(t *testing.T) {
	pool := testPool(t)
	truncateAll(t, pool)
	svc := service.New(pool)

	const n = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes, conflicts := 0, 0

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := svc.CreateTenant(context.Background(), "race-slug", "Race Tenant")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case isCode(err, domain.ErrTenantAlreadyExists):
				conflicts++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, 1, successes, "exactly one create must win the slug race")
	require.Equal(t, n-1, conflicts, "every loser must see tenant_already_exists")
}

// TestUpdateTenantVersionRace proves the conditional-UPDATE optimistic lock
// (DESIGN.md §5): 50 concurrent PATCHes all quoting the tenant's current
// version; exactly one must win, every other must see tenant_version_conflict -
// never a lost update, never a deadlock.
func TestUpdateTenantVersionRace(t *testing.T) {
	pool := testPool(t)
	truncateAll(t, pool)
	svc := service.New(pool)

	tenant, _, err := svc.CreateTenant(context.Background(), "update-race", "Before")
	require.NoError(t, err)
	activateTenant(t, pool, tenant.ID)

	const n = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes, rejections := 0, 0

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := svc.UpdateTenant(context.Background(), tenant.ID, "After", tenant.Version)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			// The conditional UPDATE changes status (active -> updating) and
			// version together, atomically. So a loser re-reading the row
			// after losing the race sees status already moved to "updating",
			// not "still active, just a stale version" - it's classified
			// tenant_update_not_allowed, per the disambiguation order in
			// DESIGN.md §5. tenant_version_conflict is the code for a
			// different scenario: a stale version quoted against a tenant
			// that's currently idle (active), not 50 simultaneous racers.
			case isCode(err, domain.ErrTenantUpdateNotAllowed), isCode(err, domain.ErrTenantVersionConflict):
				rejections++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, 1, successes, "exactly one update must win the version race")
	require.Equal(t, n-1, rejections, "every loser must be rejected, not silently lost")

	final, err := svc.GetTenant(context.Background(), tenant.ID)
	require.NoError(t, err)
	require.Equal(t, tenant.Version+1, final.Version, "version must advance by exactly 1, not n")
}

func isCode(err error, code domain.ErrorCode) bool {
	derr, ok := err.(*domain.Error)
	return ok && derr.Code == code
}

// activateTenant jumps a tenant straight to "active" via raw SQL - a stand-in
// for the M3 consumer applying a "deploy done" outcome, which doesn't exist yet.
// It must also close out the tenant's open deploy task: tasks_one_open_per_tenant
// (DESIGN.md §2) allows at most one open task per tenant, and the real consumer
// would mark the deploy task done in the same transaction it activates the
// tenant - leaving it "accepted" here would make this test's own setup collide
// with the update task that UpdateTenant creates.
func activateTenant(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `UPDATE tenants SET status = 'active' WHERE id = $1`, id)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE tasks SET status = 'done' WHERE tenant_id = $1 AND status = 'accepted'`, id)
	require.NoError(t, err)
}
