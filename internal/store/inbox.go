package store

import (
	"context"

	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
)

// InsertProcessedMessage claims a message_id in the inbox.
//
// Pattern: idempotent consumer / transactional inbox (DESIGN.md §6 Inbound).
// Why: ON CONFLICT DO NOTHING plus the primary key serializes duplicate deliveries
// on the message_id: a concurrent duplicate blocks until the first transaction ends,
// then sees 0 rows. inserted == false means "already processed" -> ack as a no-op.
// Insert-first (rather than after the state change) prevents two deliveries from
// both doing state work before discovering the duplication.
func InsertProcessedMessage(ctx context.Context, db DB, messageID string, taskID uuid.UUID) (inserted bool, err error) {
	tag, err := db.Exec(ctx, `
		INSERT INTO processed_messages (message_id, task_id)
		VALUES ($1, $2)
		ON CONFLICT (message_id) DO NOTHING`, messageID, taskID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// UpdateTaskStatus applies a worker-reported status, persisting the failure reason
// for failed updates (DESIGN.md §6 Inbound step 5 / §7).
func UpdateTaskStatus(ctx context.Context, db DB, id uuid.UUID, status domain.TaskStatus, reason *string) error {
	_, err := db.Exec(ctx, `
		UPDATE tasks
		SET status = $2, error = $3, updated_at = now()
		WHERE id = $1`, id, status, reason)
	return err
}

// UpdateTenantStatusGuarded applies a terminal task outcome to its tenant, guarded
// on the expected in-flight status.
//
// Pattern: optimistic guard / zero-rows-means-stale (DESIGN.md §6 Inbound step 5,
// §7 row 5). Why: if the tenant has already moved on (a newer lifecycle action),
// the WHERE matches zero rows and the older outcome is dropped instead of corrupting
// newer state. version advances on every mutation so clients holding the old
// optimistic-lock token get tenant_version_conflict.
func UpdateTenantStatusGuarded(ctx context.Context, db DB, tenantID uuid.UUID, expected, next domain.TenantStatus) (matched bool, err error) {
	tag, err := db.Exec(ctx, `
		UPDATE tenants
		SET status = $3, version = version + 1, updated_at = now()
		WHERE id = $1 AND status = $2`, tenantID, expected, next)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
