package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
)

func (a *API) handleListTasks(w http.ResponseWriter, r *http.Request) {
	cursor, limit := parseCursor(r), parseLimit(r)
	tasks, err := a.service.ListTasks(r.Context(), r.URL.Query().Get("status"), r.URL.Query().Get("type"), cursor, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	wire := make([]taskWire, len(tasks))
	var lastID uuid.UUID
	for i, t := range tasks {
		wire[i] = taskToWire(t)
		lastID = t.ID
	}
	writeJSON(w, http.StatusOK, taskListResponse{Tasks: wire, NextCursor: nextCursor(lastID, len(tasks), limit)})
}

func (a *API) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, domain.NewError(domain.ErrTaskNotFound, "task not found"))
		return
	}
	task, err := a.service.GetTask(r.Context(), id)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, taskToWire(task))
}
