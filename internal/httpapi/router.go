package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aashishGithub/controlplane/internal/service"
)

// API wires the service layer to chi routes (DESIGN.md §4's route table). pool is
// held only for /healthz - every other handler goes through service.
type API struct {
	service *service.Service
	pool    *pgxpool.Pool
}

func New(svc *service.Service, pool *pgxpool.Pool) http.Handler {
	a := &API{service: svc, pool: pool}

	r := chi.NewRouter()
	// Pattern: per-request ID (DESIGN.md §4 error envelope's request_id field).
	// Why: a client reporting an error by request_id lets us find the exact
	// request in logs without correlating on timestamp/path, which is ambiguous
	// under concurrent identical requests (exactly the case this API has to
	// handle well - see the slug/version race tests).
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", a.handleHealthz)

	r.Route("/v1/tenants", func(r chi.Router) {
		r.Post("/", a.handleCreateTenant)
		r.Get("/", a.handleListTenants)
		r.Get("/{id}", a.handleGetTenant)
		r.Patch("/{id}", a.handleUpdateTenant)
		r.Delete("/{id}", a.handleDeleteTenant)
		r.Get("/{id}/tasks", a.handleListTenantTasks)
	})

	r.Route("/v1/tasks", func(r chi.Router) {
		r.Get("/", a.handleListTasks)
		r.Get("/{id}", a.handleGetTask)
	})

	return r
}

// handleHealthz checks the DB. The broker check (DESIGN.md: "checks DB +
// broker") is added in M3 once messaging exists.
func (a *API) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if err := a.pool.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
