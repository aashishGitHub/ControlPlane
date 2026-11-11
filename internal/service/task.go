package service

import (
	"context"

	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/store"
)

// TaskUpdate is a validated inbound worker-progress update. It deliberately does
// not carry tenant_id: the consumer looks the tenant up from the task row and does
// not trust the message's claim (blast-radius defense, DESIGN.md §6 Inbound).
type TaskUpdate struct {
	MessageID string
	TaskID    uuid.UUID
	Status    domain.TaskStatus
	Reason    *string
}

// ApplyResult tells the consumer what happened without treating a no-op as an
// error. Duplicate and stale are both successful acks; only a returned error is
// classified poison/transient.
type ApplyResult string

const (
	ApplyApplied   ApplyResult = "applied"
	ApplyDuplicate ApplyResult = "duplicate"
	ApplyStale     ApplyResult = "stale"
)

// ApplyTaskUpdate applies one worker-progress message in a single transaction.
//
// Pattern: idempotent consumer + transactional inbox + rank guard (DESIGN.md §6
// Inbound, §7 rows 3-4). Why one transaction: committing the inbox row separately
// from the state change would let a crash mark the message processed while the
// update was lost forever. Why insert the inbox row first: it serializes concurrent
// duplicates on the message_id primary key. Lock order is tenant -> task, matching
// the API path, so the two cannot deadlock.
func (s *Service) ApplyTaskUpdate(ctx context.Context, update TaskUpdate) (ApplyResult, error) {
	result := ApplyStale

	err := store.WithTx(ctx, s.pool, func(db store.DB) error {
		inserted, err := store.InsertProcessedMessage(ctx, db, update.MessageID, update.TaskID)
		if err != nil {
			return err
		}
		if !inserted {
			result = ApplyDuplicate
			return nil
		}

		task, err := store.GetTaskByID(ctx, db, update.TaskID)
		if err == store.ErrNoRows {
			return domain.NewError(domain.ErrTaskNotFound, "task not found")
		}
		if err != nil {
			return err
		}

		if _, err := store.GetTenantForUpdate(ctx, db, task.TenantID); err == store.ErrNoRows {
			return domain.NewError(domain.ErrTenantNotFound, "tenant not found")
		} else if err != nil {
			return err
		}

		locked, err := store.GetTaskForUpdate(ctx, db, update.TaskID)
		if err == store.ErrNoRows {
			return domain.NewError(domain.ErrTaskNotFound, "task not found")
		}
		if err != nil {
			return err
		}

		if domain.IsTaskTerminal(locked.Status) || !domain.TaskUpdateAllowed(locked.Status, update.Status) {
			result = ApplyStale
			return nil
		}

		reason := update.Reason
		if update.Status != domain.TaskFailed {
			reason = nil
		}
		if err := store.UpdateTaskStatus(ctx, db, locked.ID, update.Status, reason); err != nil {
			return err
		}

		if domain.IsTaskTerminal(update.Status) {
			expected, next, ok := domain.TenantOutcome(locked.Type, update.Status)
			if ok {
				if _, err := store.UpdateTenantStatusGuarded(ctx, db, locked.TenantID, expected, next); err != nil {
					return err
				}
			}
		}

		result = ApplyApplied
		return nil
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

func (s *Service) GetTask(ctx context.Context, id uuid.UUID) (store.Task, error) {
	t, err := store.GetTaskByID(ctx, s.pool, id)
	if err == store.ErrNoRows {
		return store.Task{}, domain.NewError(domain.ErrTaskNotFound, "task not found")
	}
	return t, err
}

func (s *Service) ListTasks(ctx context.Context, status, taskType string, cursor uuid.UUID, limit int) ([]store.Task, error) {
	return store.ListTasks(ctx, s.pool, status, taskType, cursor, limit)
}
