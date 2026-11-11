package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	relayBatchSize    = 100
	relayPollInterval = 500 * time.Millisecond
	maxBatchesPerWake = 10
)

// Relay drains unpublished task rows to RabbitMQ.
//
// Pattern: transactional outbox relay (DESIGN.md §6 Outbound, §7 row 2). Why:
// claim + publish + mark share one transaction, so the FOR UPDATE SKIP LOCKED
// locks stay held across the broker I/O. Concurrent replicas claim disjoint
// batches; a crash between confirm and commit only causes a duplicate publish on
// restart (at-least-once), never a publish before commit and never a lost event.
type Relay struct {
	pool *pgxpool.Pool
	cm   *messaging.ConnManager
	log  *slog.Logger
	wake chan struct{}
}

func NewRelay(pool *pgxpool.Pool, cm *messaging.ConnManager, log *slog.Logger) *Relay {
	if log == nil {
		log = slog.Default()
	}
	return &Relay{
		pool: pool,
		cm:   cm,
		log:  log,
		wake: make(chan struct{}, 1),
	}
}

// Notify is the post-commit fast path. Non-blocking: a pending wake-up already
// covers this commit, and the 500 ms poll is the safety net for missed nudges.
func (r *Relay) Notify() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run polls until ctx is cancelled, draining a few batches per wake so a burst of
// commits does not wait one poll interval per 100 rows.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(relayPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.wake:
		}

		for batch := 0; batch < maxBatchesPerWake; batch++ {
			published, err := r.RunOnce(ctx)
			if err != nil {
				if ctx.Err() == nil {
					r.log.Error("outbox: relay batch failed", "error", err)
				}
				break
			}
			if published < relayBatchSize {
				break
			}
		}
	}
}

// RunOnce claims and publishes at most relayBatchSize rows, returning how many
// were published. Exported so tests call one deterministic batch instead of
// racing a background loop.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	ch, err := r.cm.Channel()
	if err != nil {
		return 0, fmt.Errorf("outbox: channel: %w", err)
	}
	defer ch.Close()

	publisher, err := messaging.NewPublisher(ch)
	if err != nil {
		return 0, err
	}

	published := 0
	err = store.WithTx(ctx, r.pool, func(db store.DB) error {
		tasks, err := store.ClaimUnpublishedTasks(ctx, db, relayBatchSize)
		if err != nil {
			return fmt.Errorf("outbox: claim: %w", err)
		}
		for _, task := range tasks {
			msg := messaging.TaskMsg{
				SchemaVersion: messaging.SchemaVersion,
				ID:            task.ID.String(),
				Type:          string(task.Type),
				TenantID:      task.TenantID.String(),
				Status:        string(task.Status),
				CreatedAt:     domain.FormatTimestamp(task.CreatedAt),
			}
			msg.Tenant.Slug = task.TenantSlug
			msg.Tenant.Name = task.TenantName

			if err := publisher.PublishTask(ctx, msg); err != nil {
				return fmt.Errorf("outbox: publish task %s: %w", task.ID, err)
			}
			if err := store.MarkTaskPublished(ctx, db, task.ID); err != nil {
				return fmt.Errorf("outbox: mark task %s published: %w", task.ID, err)
			}
			published++
		}
		return nil
	})
	if err != nil {
		return published, err
	}
	return published, nil
}
