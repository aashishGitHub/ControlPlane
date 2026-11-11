package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/messaging"
)

// Options controls the simulator's behavior. Every field also has a CLI flag and
// an env-var default in cmd/worker.
type Options struct {
	FailRate      float64
	MinDelay      time.Duration
	MaxDelay      time.Duration
	DuplicateRate float64
	Reorder       bool
}

func DefaultOptions() Options {
	return Options{
		FailRate:      0,
		MinDelay:      100 * time.Millisecond,
		MaxDelay:      800 * time.Millisecond,
		DuplicateRate: 0,
		Reorder:       false,
	}
}

func (o Options) Validate() error {
	if o.FailRate < 0 || o.FailRate > 1 {
		return fmt.Errorf("fail-rate must be between 0 and 1, got %v", o.FailRate)
	}
	if o.DuplicateRate < 0 || o.DuplicateRate > 1 {
		return fmt.Errorf("duplicate-rate must be between 0 and 1, got %v", o.DuplicateRate)
	}
	if o.MinDelay < 0 || o.MaxDelay < 0 {
		return fmt.Errorf("delays must be non-negative")
	}
	if o.MinDelay > o.MaxDelay {
		return fmt.Errorf("min-delay-ms must be <= max-delay-ms")
	}
	return nil
}

type Worker struct {
	cm   *messaging.ConnManager
	log  *slog.Logger
	opts Options
}

func New(cm *messaging.ConnManager, log *slog.Logger, opts Options) (*Worker, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &Worker{cm: cm, log: log, opts: opts}, nil
}

// Run consumes worker.tasks until ctx is cancelled, reconnecting on channel or
// connection loss.
func (w *Worker) Run(ctx context.Context) {
	delay := 250 * time.Millisecond
	for ctx.Err() == nil {
		err := w.consumeOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.log.Error("worker: consumer stopped", "error", err)
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < 10*time.Second {
			delay *= 2
		}
	}
}

func (w *Worker) consumeOnce(ctx context.Context) error {
	consumeCh, err := w.cm.Channel()
	if err != nil {
		return err
	}
	defer consumeCh.Close()

	publishCh, err := w.cm.Channel()
	if err != nil {
		return err
	}
	defer publishCh.Close()

	publisher, err := messaging.NewPublisher(publishCh)
	if err != nil {
		return err
	}

	// Prefetch 1: tasks are sequential, long-running, and the worker wants at most
	// one unacked task in flight so "crash mid-task" means exactly one redelivery.
	if err := consumeCh.Qos(1, 0, false); err != nil {
		return fmt.Errorf("worker: qos: %w", err)
	}
	deliveries, err := consumeCh.Consume(messaging.QueueWorkerTasks, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("worker: consume %s: %w", messaging.QueueWorkerTasks, err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			w.handleDelivery(ctx, delivery, publisher)
		}
	}
}

func (w *Worker) handleDelivery(ctx context.Context, delivery amqp.Delivery, publisher *messaging.Publisher) {
	var task messaging.TaskMsg
	if err := json.Unmarshal(delivery.Body, &task); err != nil {
		w.log.Warn("worker: dropping malformed task", "error", err, "body", string(delivery.Body))
		w.nack(delivery, false)
		return
	}
	taskID, tenantID, taskType, err := parseTask(task)
	if err != nil {
		w.log.Warn("worker: dropping invalid task", "error", err, "task_id", task.ID)
		w.nack(delivery, false)
		return
	}

	log := w.log.With("task_id", taskID, "tenant_id", tenantID, "type", taskType)
	if err := w.process(ctx, publisher, taskID, tenantID, taskType); err != nil {
		log.Error("worker: task processing failed; requeueing", "error", err)
		w.nack(delivery, true)
		return
	}
	// Pattern: ack-after-confirm (DESIGN.md §8 "Worker crashes mid-task"). Why:
	// the terminal update is already durably accepted by the broker; if the ack is
	// lost, the broker redelivers the task and the deterministic message_id makes
	// the replay a no-op.
	if err := delivery.Ack(false); err != nil {
		log.Error("worker: ack failed", "error", err)
		return
	}
	log.Info("worker: task complete")
}

func (w *Worker) process(ctx context.Context, publisher *messaging.Publisher, taskID, tenantID uuid.UUID, taskType domain.TaskType) error {
	// Pattern: simulated processing delay. Why: exercises the in-flight tenant
	// statuses (provisioning/updating/destroying) and gives the fault-injection
	// scripts a window to race progress updates.
	if delay := w.randomDelay(); delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	terminal := domain.TaskDone
	var reason *string
	if w.opts.FailRate > 0 && rand.Float64() < w.opts.FailRate {
		terminal = domain.TaskFailed
		msg := fmt.Sprintf("worker simulated failure for %s task", taskType)
		reason = &msg
	}

	publish := func(status domain.TaskStatus) error {
		return publisher.PublishUpdate(ctx, newUpdate(taskID, tenantID, status, reasonFor(status, reason)))
	}

	if w.opts.Reorder {
		// Deliberately out of order: terminal first, in_progress second. The
		// consumer's rank guard must drop the late in_progress (DESIGN.md §3).
		if err := publish(terminal); err != nil {
			return err
		}
		if w.shouldDuplicate() {
			if err := publish(terminal); err != nil {
				return err
			}
		}
		return publish(domain.TaskInProgress)
	}

	if err := publish(domain.TaskInProgress); err != nil {
		return err
	}
	if err := publish(terminal); err != nil {
		return err
	}
	if w.shouldDuplicate() {
		if err := publish(terminal); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) randomDelay() time.Duration {
	if w.opts.MaxDelay <= 0 {
		return 0
	}
	span := w.opts.MaxDelay - w.opts.MinDelay
	if span <= 0 {
		return w.opts.MinDelay
	}
	return w.opts.MinDelay + time.Duration(rand.Int64N(int64(span)+1))
}

func (w *Worker) shouldDuplicate() bool {
	return w.opts.DuplicateRate > 0 && rand.Float64() < w.opts.DuplicateRate
}

func (w *Worker) nack(delivery amqp.Delivery, requeue bool) {
	if err := delivery.Nack(false, requeue); err != nil {
		w.log.Error("worker: nack failed", "error", err, "requeue", requeue)
	}
}

func parseTask(msg messaging.TaskMsg) (uuid.UUID, uuid.UUID, domain.TaskType, error) {
	if msg.SchemaVersion != messaging.SchemaVersion {
		return uuid.Nil, uuid.Nil, "", fmt.Errorf("unsupported schema_version %d", msg.SchemaVersion)
	}
	taskID, err := uuid.Parse(msg.ID)
	if err != nil {
		return uuid.Nil, uuid.Nil, "", fmt.Errorf("task id: %w", err)
	}
	tenantID, err := uuid.Parse(msg.TenantID)
	if err != nil {
		return uuid.Nil, uuid.Nil, "", fmt.Errorf("tenant id: %w", err)
	}
	taskType, ok := domain.ParseTaskType(msg.Type)
	if !ok {
		return uuid.Nil, uuid.Nil, "", fmt.Errorf("unknown task type %q", msg.Type)
	}
	return taskID, tenantID, taskType, nil
}

func newUpdate(taskID, tenantID uuid.UUID, status domain.TaskStatus, reason *string) messaging.TaskUpdateMsg {
	msg := messaging.TaskUpdateMsg{
		SchemaVersion: messaging.SchemaVersion,
		MessageID:     messaging.NewMessageID(taskID, string(status)),
		TaskID:        taskID.String(),
		TenantID:      tenantID.String(),
		Status:        string(status),
		OccurredAt:    domain.FormatTimestamp(time.Now()),
	}
	if reason != nil {
		msg.Error = &messaging.TaskUpdateError{Reason: *reason}
	}
	return msg
}

func reasonFor(status domain.TaskStatus, reason *string) *string {
	if status == domain.TaskFailed {
		return reason
	}
	return nil
}
