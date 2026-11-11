package store

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
)

// UnpublishedTask is a task row claimed for relay, joined with the tenant data
// the outbound wire message needs. Joining in the claim query means payload
// construction and the claim succeed or fail together: the relay never publishes
// a partial message because the tenant lookup failed after the claim.
type UnpublishedTask struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	Type       domain.TaskType
	Status     domain.TaskStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
	TenantSlug string
	TenantName string
}

// ClaimUnpublishedTasks locks up to limit unpublished task rows and returns them.
//
// Pattern: transactional outbox claim with FOR UPDATE SKIP LOCKED (DESIGN.md §6
// Outbound). Why: the row lock is transaction-scoped, so the caller MUST keep the
// surrounding transaction open through publish + MarkTaskPublished. Otherwise the
// lock is released when this statement ends, two relay replicas can claim the same
// rows, and both publish. SKIP LOCKED lets concurrent replicas take disjoint
// batches instead of blocking on each other.
func ClaimUnpublishedTasks(ctx context.Context, db DB, limit int) ([]UnpublishedTask, error) {
	rows, err := db.Query(ctx, `
		SELECT t.id, t.tenant_id, t.type, t.status, t.created_at, t.updated_at,
		       tn.slug, tn.name
		FROM tasks t
		JOIN tenants tn ON tn.id = t.tenant_id
		WHERE t.published_at IS NULL
		ORDER BY t.id
		LIMIT $1
		FOR UPDATE OF t SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []UnpublishedTask
	for rows.Next() {
		var t UnpublishedTask
		if err := rows.Scan(&t.ID, &t.TenantID, &t.Type, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.TenantSlug, &t.TenantName); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// MarkTaskPublished records the broker confirm. It must run in the same
// transaction as ClaimUnpublishedTasks, while the row lock is still held.
func MarkTaskPublished(ctx context.Context, db DB, id uuid.UUID) error {
	_, err := db.Exec(ctx, `UPDATE tasks SET published_at = now() WHERE id = $1`, id)
	return err
}
