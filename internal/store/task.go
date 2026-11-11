package store

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
)

// Task mirrors the tasks table. PublishedAt is nil until the outbox relay (M3)
// confirms delivery to the broker.
type Task struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	Type        domain.TaskType
	Status      domain.TaskStatus
	Error       *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	PublishedAt *time.Time
}

// CreateTask inserts a new task row with published_at = NULL - it becomes the
// outbox row the relay (M3) picks up after this transaction commits (DESIGN.md
// §6 Outbound). The partial unique index tasks_one_open_per_tenant is the
// safety-net that rejects a second open task for the same tenant; the service
// layer's own status guard is the primary mechanism (DESIGN.md §2).
func CreateTask(ctx context.Context, db DB, t Task) error {
	_, err := db.Exec(ctx, `
		INSERT INTO tasks (id, tenant_id, type, status)
		VALUES ($1, $2, $3, $4)`,
		t.ID, t.TenantID, t.Type, t.Status)
	return err
}

func GetTaskByID(ctx context.Context, db DB, id uuid.UUID) (Task, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, type, status, error, created_at, updated_at, published_at
		FROM tasks WHERE id = $1`, id)
	return scanTask(row)
}

// ListTasksByTenant returns a tenant's tasks newest-first (no pagination - a
// tenant has at most a handful of tasks, one per lifecycle transition).
func ListTasksByTenant(ctx context.Context, db DB, tenantID uuid.UUID) ([]Task, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, status, error, created_at, updated_at, published_at
		FROM tasks WHERE tenant_id = $1 ORDER BY id DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectTasks(rows)
}

// ListTasks is the global task listing (GET /v1/tasks), keyset-paginated like
// ListTenants, optionally filtered by status and/or type.
func ListTasks(ctx context.Context, db DB, status, taskType string, cursor uuid.UUID, limit int) ([]Task, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, status, error, created_at, updated_at, published_at
		FROM tasks
		WHERE id > $1 AND ($2 = '' OR status = $2) AND ($3 = '' OR type = $3)
		ORDER BY id
		LIMIT $4`, cursor, status, taskType, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectTasks(rows)
}

func collectTasks(rows pgxRows) ([]Task, error) {
	var out []Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// pgxRows is the subset of pgx.Rows collectTasks/ListTenants-style helpers need -
// kept local to avoid importing pgx here just for the type name.
type pgxRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanTask(row rowScanner) (Task, error) {
	var t Task
	err := row.Scan(&t.ID, &t.TenantID, &t.Type, &t.Status, &t.Error, &t.CreatedAt, &t.UpdatedAt, &t.PublishedAt)
	return t, err
}
