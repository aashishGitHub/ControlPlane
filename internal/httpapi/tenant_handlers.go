package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
)

type createTenantRequest struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

// handleCreateTenant is POST /v1/tenants. 201 + Location, per DESIGN.md §4,
// because the tenant row exists synchronously by the time this responds (unlike
// PATCH/DELETE, which only *start* async work).
func (a *API) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	details := map[string]any{}
	if !domain.ValidSlug(req.Slug) {
		details["slug"] = "must match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$"
	}
	if blank(req.Name) {
		details["name"] = "must not be blank"
	}
	if len(details) > 0 {
		validationError(w, r, details)
		return
	}

	tenant, task, err := a.service.CreateTenant(r.Context(), req.Slug, req.Name)
	if err != nil {
		writeError(w, r, err)
		return
	}

	w.Header().Set("Location", "/v1/tenants/"+tenant.ID.String())
	writeJSON(w, http.StatusCreated, mutationResponse{Tenant: tenantToWire(tenant), Task: taskToWire(task)})
}

func (a *API) handleListTenants(w http.ResponseWriter, r *http.Request) {
	cursor, limit := parseCursor(r), parseLimit(r)
	tenants, err := a.service.ListTenants(r.Context(), r.URL.Query().Get("status"), cursor, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}

	wire := make([]tenantWire, len(tenants))
	var lastID uuid.UUID
	for i, t := range tenants {
		wire[i] = tenantToWire(t)
		lastID = t.ID
	}
	writeJSON(w, http.StatusOK, tenantListResponse{Tenants: wire, NextCursor: nextCursor(lastID, len(tenants), limit)})
}

func (a *API) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	id, err := parseTenantID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	tenant, err := a.service.GetTenant(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, tenantToWire(tenant))
}

type patchTenantRequest struct {
	Name    string `json:"name"`
	Version *int64 `json:"version"`
}

// handleUpdateTenant is PATCH /v1/tenants/{id}. 202: it only starts the async
// update task - DESIGN.md §4.
func (a *API) handleUpdateTenant(w http.ResponseWriter, r *http.Request) {
	id, err := parseTenantID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	var req patchTenantRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	details := map[string]any{}
	if blank(req.Name) {
		details["name"] = "must not be blank"
	}
	if req.Version == nil {
		details["version"] = "is required"
	}
	if len(details) > 0 {
		validationError(w, r, details)
		return
	}

	tenant, task, err := a.service.UpdateTenant(r.Context(), id, req.Name, *req.Version)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, mutationResponse{Tenant: tenantToWire(tenant), Task: taskToWire(task)})
}

type deleteTenantRequest struct {
	Version *int64 `json:"version"`
}

func (a *API) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	id, err := parseTenantID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	var req deleteTenantRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	if req.Version == nil {
		validationError(w, r, map[string]any{"version": "is required"})
		return
	}

	tenant, task, err := a.service.DeleteTenant(r.Context(), id, *req.Version)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, mutationResponse{Tenant: tenantToWire(tenant), Task: taskToWire(task)})
}

func (a *API) handleListTenantTasks(w http.ResponseWriter, r *http.Request) {
	id, err := parseTenantID(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	tasks, err := a.service.ListTenantTasks(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	wire := make([]taskWire, len(tasks))
	for i, t := range tasks {
		wire[i] = taskToWire(t)
	}
	writeJSON(w, http.StatusOK, taskListResponse{Tasks: wire})
}

func parseTenantID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		return uuid.Nil, domain.NewError(domain.ErrTenantNotFound, "tenant not found")
	}
	return id, nil
}

func blank(s string) bool {
	for _, r := range s {
		if r != ' ' && r != '\t' && r != '\n' {
			return false
		}
	}
	return true
}

// decodeJSON decodes the request body, mapping any JSON error to
// malformed_request (DESIGN.md §4) - a syntax/type error is the client's fault,
// not a 422 (which is reserved for a well-formed body that fails business rules).
func decodeJSON(r *http.Request, dst any) error {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return domain.NewError(domain.ErrMalformedRequest, "request body is required")
		}
		return domain.NewError(domain.ErrMalformedRequest, "request body is not valid JSON: "+err.Error())
	}
	return nil
}
