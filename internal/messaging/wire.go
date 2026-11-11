package messaging

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
)

// SchemaVersion is the only inbound/outbound message schema this build accepts.
// Unknown versions are poison, not transient: the payload will not become valid
// on retry (DESIGN.md §6 Resilience).
const SchemaVersion = 1

// TaskMsg is the outbound wire format published to cp.tasks for workers
// (DESIGN.md §6 Outbound).
type TaskMsg struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Type          string `json:"type"`
	TenantID      string `json:"tenant_id"`
	Status        string `json:"status"`
	CreatedAt     string `json:"created_at"`
	Tenant        struct {
		Slug string `json:"slug"`
		Name string `json:"name"`
	} `json:"tenant"`
}

// TaskUpdateError is the optional failure detail on an inbound update.
type TaskUpdateError struct {
	Reason string `json:"reason"`
}

// TaskUpdateMsg mirrors the inbound worker-progress contract (DESIGN.md §6
// Inbound).
type TaskUpdateMsg struct {
	SchemaVersion int              `json:"schema_version"`
	MessageID     string           `json:"message_id"`
	TaskID        string           `json:"task_id"`
	TenantID      string           `json:"tenant_id"`
	Status        string           `json:"status"`
	OccurredAt    string           `json:"occurred_at"`
	Error         *TaskUpdateError `json:"error,omitempty"`
}

// TaskUpdate is a validated inbound message: typed IDs/status so the consumer and
// service layers never re-parse strings or trust an unknown status.
type TaskUpdate struct {
	MessageID string
	TaskID    uuid.UUID
	TenantID  uuid.UUID
	Status    domain.TaskStatus
	Reason    *string
}

// NewMessageID builds the deterministic idempotency key UUIDv5(task_id + "/" +
// status) (DESIGN.md §6 Inbound).
//
// Pattern: deterministic message identity. Why: a worker retry or broker
// redelivery of the same logical update must produce the same inbox key; a random
// UUID per attempt would defeat deduplication. Including status distinguishes done
// from failed, and including task_id scopes the key to one task.
func NewMessageID(taskID uuid.UUID, status string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(taskID.String()+"/"+status)).String()
}

// Parse validates and converts the wire message. Invalid schema version, IDs, or
// status are permanent errors: the consumer dead-letters them rather than letting
// a bad value reach the rank map (an unknown status would otherwise look like rank
// 0 = accepted).
func (m TaskUpdateMsg) Parse() (TaskUpdate, error) {
	if m.SchemaVersion != SchemaVersion {
		return TaskUpdate{}, fmt.Errorf("unsupported schema_version %d", m.SchemaVersion)
	}
	if m.MessageID == "" {
		return TaskUpdate{}, fmt.Errorf("message_id is required")
	}
	if _, err := uuid.Parse(m.MessageID); err != nil {
		return TaskUpdate{}, fmt.Errorf("message_id is not a UUID: %w", err)
	}
	taskID, err := uuid.Parse(m.TaskID)
	if err != nil {
		return TaskUpdate{}, fmt.Errorf("task_id is not a UUID: %w", err)
	}
	tenantID, err := uuid.Parse(m.TenantID)
	if err != nil {
		return TaskUpdate{}, fmt.Errorf("tenant_id is not a UUID: %w", err)
	}
	status, ok := domain.ParseTaskStatus(m.Status)
	if !ok {
		return TaskUpdate{}, fmt.Errorf("unknown status %q", m.Status)
	}

	var reason *string
	if status == domain.TaskFailed && m.Error != nil && m.Error.Reason != "" {
		r := m.Error.Reason
		reason = &r
	}
	return TaskUpdate{
		MessageID: m.MessageID,
		TaskID:    taskID,
		TenantID:  tenantID,
		Status:    status,
		Reason:    reason,
	}, nil
}

// TaskRoutingKey is the outbound routing key for a task type: task.<type>.
func TaskRoutingKey(taskType string) string { return "task." + taskType }

// UpdateRoutingKey is the inbound routing key for a task status:
// task.update.<status>.
func UpdateRoutingKey(status string) string { return "task.update." + status }
