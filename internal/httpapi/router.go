package httpapi

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aashishGithub/controlplane/internal/service"
)

// BrokerHealth is the health probe the router needs; *messaging.ConnManager
// satisfies it. An interface keeps httpapi from importing the AMQP adapter.
type BrokerHealth interface {
	Ping(ctx context.Context) error
}

// API wires the service layer to chi routes (DESIGN.md §4's route table). pool is
// held only for /healthz - every other handler goes through service.
type API struct {
	service *service.Service
	pool    *pgxpool.Pool
	broker  BrokerHealth
}

func New(svc *service.Service, pool *pgxpool.Pool, broker BrokerHealth) http.Handler {
	a := &API{service: svc, pool: pool, broker: broker}

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

// handleHealthz checks both dependencies per DESIGN.md §4: DB + broker. The outbox
// decouples API writes from broker availability, but the contract says /healthz
// reports both rather than silently checking only Postgres.
func (a *API) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if err := a.pool.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "dependency": "postgres"})
		return
	}
	if a.broker != nil {
		if err := a.broker.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "dependency": "rabbitmq"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
