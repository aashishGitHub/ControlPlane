package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/aashishGithub/controlplane/internal/domain"
)

const (
	consumerPrefetch   = 10
	consumerMinBackoff = 250 * time.Millisecond
	consumerMaxBackoff = 10 * time.Second
	transientAttempts  = 3
	transientBaseDelay = 100 * time.Millisecond
)

// UpdateHandler applies a validated inbound update. Returning a *domain.Error
// with task_not_found / tenant_not_found / validation_error is classified as
// poison; any other error is treated as transient.
type UpdateHandler func(ctx context.Context, update TaskUpdate) error

// ConsumeUpdates consumes cp.task-updates until ctx is cancelled, reconnecting
// with backoff whenever the channel or connection drops.
//
// Ack policy: ack only after the handler's DB transaction commits. If the ack is
// lost, the broker redelivers and the inbox turns the replay into a no-op. A
// poison message is Nack(requeue=false) and dead-letters; a transient error is
// retried in-process a bounded number of times, then Nack(requeue=true) and the
// queue's x-delivery-limit caps the broker-side loop.
func ConsumeUpdates(ctx context.Context, cm *ConnManager, log *slog.Logger, handle UpdateHandler) {
	if log == nil {
		log = slog.Default()
	}
	delay := consumerMinBackoff
	for ctx.Err() == nil {
		if err := consumeOnce(ctx, cm, log, handle); err != nil && ctx.Err() == nil {
			log.Error("messaging: consumer stopped", "error", err)
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(withJitter(delay)):
		}
		delay = minDuration(delay*2, consumerMaxBackoff)
	}
}

func consumeOnce(ctx context.Context, cm *ConnManager, log *slog.Logger, handle UpdateHandler) error {
	ch, err := cm.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(consumerPrefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := ch.Consume(QueueTaskUpdates, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", QueueTaskUpdates, err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			processDelivery(ctx, delivery, log, handle)
		}
	}
}

func processDelivery(ctx context.Context, delivery amqp.Delivery, log *slog.Logger, handle UpdateHandler) {
	var wire TaskUpdateMsg
	if err := json.Unmarshal(delivery.Body, &wire); err != nil {
		log.Warn("messaging: dead-lettering malformed update", "error", err, "body", string(delivery.Body))
		nack(delivery, false, log)
		return
	}
	update, err := wire.Parse()
	if err != nil {
		log.Warn("messaging: dead-lettering invalid update",
			"error", err, "message_id", wire.MessageID, "task_id", wire.TaskID)
		nack(delivery, false, log)
		return
	}

	logger := log.With("message_id", update.MessageID, "task_id", update.TaskID, "tenant_id", update.TenantID)

	err = withTransientRetry(ctx, func() error {
		return invokeHandler(ctx, handle, update)
	})
	if err != nil {
		var handlerErr *handlerPanicError
		if errors.As(err, &handlerErr) {
			logger.Error("messaging: dead-lettering update after handler panic", "panic", handlerErr.Value)
			nack(delivery, false, log)
			return
		}
		if isPermanent(err) {
			logger.Warn("messaging: dead-lettering permanent update failure", "error", err)
			nack(delivery, false, log)
			return
		}
		logger.Error("messaging: transient update failure; requeueing", "error", err)
		nack(delivery, true, log)
		return
	}

	if err := delivery.Ack(false); err != nil {
		logger.Error("messaging: ack failed", "error", err)
		return
	}
	logger.Info("messaging: task update applied", "status", update.Status)
}

func nack(delivery amqp.Delivery, requeue bool, log *slog.Logger) {
	if err := delivery.Nack(false, requeue); err != nil {
		log.Error("messaging: nack failed", "error", err, "requeue", requeue)
	}
}

// withTransientRetry gives connection blips a few in-process chances before the
// message goes back to the broker. Permanent errors short-circuit immediately.
func withTransientRetry(ctx context.Context, fn func() error) error {
	delay := transientBaseDelay
	var err error
	for attempt := 0; attempt < transientAttempts; attempt++ {
		err = fn()
		if err == nil || isPermanent(err) || ctx.Err() != nil {
			return err
		}
		if attempt == transientAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(withJitter(delay)):
		}
		delay *= 2
	}
	return err
}

// isPermanent classifies domain validation/missing-row errors as poison. Anything
// else (connection reset, timeout, pool exhaustion) is transient.
func isPermanent(err error) bool {
	var domainErr *domain.Error
	if !errors.As(err, &domainErr) {
		return false
	}
	switch domainErr.Code {
	case domain.ErrTaskNotFound, domain.ErrTenantNotFound, domain.ErrValidation:
		return true
	default:
		return false
	}
}

type handlerPanicError struct {
	Value any
}

func (e *handlerPanicError) Error() string { return fmt.Sprintf("handler panic: %v", e.Value) }

func invokeHandler(ctx context.Context, handle UpdateHandler, update TaskUpdate) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &handlerPanicError{Value: recovered}
		}
	}()
	return handle(ctx, update)
}
