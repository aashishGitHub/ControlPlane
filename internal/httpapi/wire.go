package httpapi

import (
	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/store"
)

// Wire types are the HTTP-facing shape (DESIGN.md §2 timestamp format via
// domain.Timestamp). Keeping them separate from store.Tenant/store.Task means a
// column rename in the DB never silently changes the API contract.

type tenantWire struct {
	ID        uuid.UUID           `json:"id"`
	Slug      string              `json:"slug"`
	Name      string              `json:"name"`
	Status    domain.TenantStatus `json:"status"`
	Version   int64               `json:"version"`
	CreatedAt domain.Timestamp    `json:"created_at"`
	UpdatedAt domain.Timestamp    `json:"updated_at"`
}

func tenantToWire(t store.Tenant) tenantWire {
	return tenantWire{
		ID:        t.ID,
		Slug:      t.Slug,
		Name:      t.Name,
		Status:    t.Status,
		Version:   t.Version,
		CreatedAt: domain.NewTimestamp(t.CreatedAt),
		UpdatedAt: domain.NewTimestamp(t.UpdatedAt),
	}
}

type taskWire struct {
	ID        uuid.UUID         `json:"id"`
	TenantID  uuid.UUID         `json:"tenant_id"`
	Type      domain.TaskType   `json:"type"`
	Status    domain.TaskStatus `json:"status"`
	Error     *string           `json:"error,omitempty"`
	CreatedAt domain.Timestamp  `json:"created_at"`
	UpdatedAt domain.Timestamp  `json:"updated_at"`
}

func taskToWire(t store.Task) taskWire {
	return taskWire{
		ID:        t.ID,
		TenantID:  t.TenantID,
		Type:      t.Type,
		Status:    t.Status,
		Error:     t.Error,
		CreatedAt: domain.NewTimestamp(t.CreatedAt),
		UpdatedAt: domain.NewTimestamp(t.UpdatedAt),
	}
}

// mutationResponse is the shape every create/patch/delete endpoint returns
// (DESIGN.md §4: "Mutating responses: {tenant, task}") - the client sees both
// the tenant's new state and the task that will drive it forward.
type mutationResponse struct {
	Tenant tenantWire `json:"tenant"`
	Task   taskWire   `json:"task"`
}

type tenantListResponse struct {
	Tenants    []tenantWire `json:"tenants"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

type taskListResponse struct {
	Tasks      []taskWire `json:"tasks"`
	NextCursor string     `json:"next_cursor,omitempty"`
}
