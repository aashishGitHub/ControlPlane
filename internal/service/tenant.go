package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aashishGithub/controlplane/internal/domain"
	"github.com/aashishGithub/controlplane/internal/store"
)

// Service is the use-case layer. It holds the pool (not a tx) because each
// method opens its own transaction - that's the transaction-boundary rule from
// DESIGN.md §1.
type Service struct {
	pool     *pgxpool.Pool
	notifier func()
}

// Option customizes Service. Options keep New's call sites simple while letting
// main wire the outbox's post-commit nudge without making the service layer
// depend on the messaging/outbox packages.
type Option func(*Service)

// WithNotifier registers a post-commit wake-up (the outbox relay's Notify). It is
// called only after a mutation transaction commits, never on rollback or error.
func WithNotifier(notify func()) Option {
	return func(s *Service) {
		if notify != nil {
			s.notifier = notify
		}
	}
}

func New(pool *pgxpool.Pool, opts ...Option) *Service {
	s := &Service{pool: pool}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// notify post-commit nudges the relay. The 500 ms poll remains the safety net if
// this is a no-op or the process crashes between commit and notify (DESIGN.md §6).
func (s *Service) notify() {
	if s.notifier != nil {
		s.notifier()
	}
}

// CreateTenant inserts the tenant (provisioning) and its deploy task (accepted)
// in one transaction. A duplicate slug surfaces as the DB's 23505 unique
// violation (DESIGN.md §5) rather than a pre-check SELECT, so there's no
// read-then-write race window: whichever INSERT the DB commits first wins, full
// stop.
func (s *Service) CreateTenant(ctx context.Context, slug, name string) (store.Tenant, store.Task, error) {
	tenant := store.Tenant{
		ID:      mustUUIDv7(),
		Slug:    slug,
		Name:    name,
		Status:  domain.TenantProvisioning,
		Version: 1,
	}
	task := store.Task{
		ID:       mustUUIDv7(),
		TenantID: tenant.ID,
		Type:     domain.TaskDeploy,
		Status:   domain.TaskAccepted,
	}

	err := store.WithTx(ctx, s.pool, func(db store.DB) error {
		if err := store.CreateTenant(ctx, db, tenant); err != nil {
			return err
		}
		return store.CreateTask(ctx, db, task)
	})
	if err != nil {
		if store.IsUniqueViolation(err) {
			return store.Tenant{}, store.Task{}, domain.NewError(domain.ErrTenantAlreadyExists,
				"a tenant with this slug already exists")
		}
		return store.Tenant{}, store.Task{}, err
	}
	s.notify()

	tenant, err = refetchTenant(ctx, s.pool, tenant.ID)
	return tenant, task, err
}

// UpdateTenant runs the conditional UPDATE (DESIGN.md §5) and, if it matched,
// creates the update task in the same transaction. If it didn't match, it
// re-reads the tenant to disambiguate *why*: missing row, wrong status, or a
// stale version - the zero-rows result alone can't tell those apart.
func (s *Service) UpdateTenant(ctx context.Context, id uuid.UUID, name string, version int64) (store.Tenant, store.Task, error) {
	var tenant store.Tenant
	task := store.Task{
		ID:       mustUUIDv7(),
		TenantID: id,
		Type:     domain.TaskUpdate,
		Status:   domain.TaskAccepted,
	}

	err := store.WithTx(ctx, s.pool, func(db store.DB) error {
		updated, matched, err := store.UpdateTenant(ctx, db, id, name, version)
		if err != nil {
			return err
		}
		if !matched {
			return disambiguateNoMatch(ctx, db, id, version, domain.CanUpdateTenant, domain.ErrTenantUpdateNotAllowed)
		}
		tenant = updated
		return store.CreateTask(ctx, db, task)
	})
	if err != nil {
		return store.Tenant{}, store.Task{}, err
	}
	s.notify()
	return tenant, task, nil
}

// DeleteTenant is the same shape as UpdateTenant with the destroy task/status.
func (s *Service) DeleteTenant(ctx context.Context, id uuid.UUID, version int64) (store.Tenant, store.Task, error) {
	var tenant store.Tenant
	task := store.Task{
		ID:       mustUUIDv7(),
		TenantID: id,
		Type:     domain.TaskDestroy,
		Status:   domain.TaskAccepted,
	}

	err := store.WithTx(ctx, s.pool, func(db store.DB) error {
		updated, matched, err := store.DeleteTenant(ctx, db, id, version)
		if err != nil {
			return err
		}
		if !matched {
			return disambiguateNoMatch(ctx, db, id, version, domain.CanDeleteTenant, domain.ErrTenantUpdateNotAllowed)
		}
		tenant = updated
		return store.CreateTask(ctx, db, task)
	})
	if err != nil {
		return store.Tenant{}, store.Task{}, err
	}
	s.notify()
	return tenant, task, nil
}

func (s *Service) GetTenant(ctx context.Context, id uuid.UUID) (store.Tenant, error) {
	t, err := store.GetTenantByID(ctx, s.pool, id)
	if err == store.ErrNoRows {
		return store.Tenant{}, domain.NewError(domain.ErrTenantNotFound, "tenant not found")
	}
	return t, err
}

func (s *Service) ListTenants(ctx context.Context, status string, cursor uuid.UUID, limit int) ([]store.Tenant, error) {
	return store.ListTenants(ctx, s.pool, status, cursor, limit)
}

func (s *Service) ListTenantTasks(ctx context.Context, tenantID uuid.UUID) ([]store.Task, error) {
	return store.ListTasksByTenant(ctx, s.pool, tenantID)
}

// disambiguateNoMatch re-reads the tenant after a conditional UPDATE matched zero
// rows and decides which of the three possible reasons applies, in the order
// DESIGN.md §5 specifies: missing row -> not_found; wrong status -> the
// caller-supplied notAllowedCode; otherwise (status ok, so the version must have
// differed) -> version_conflict.
func disambiguateNoMatch(ctx context.Context, db store.DB, id uuid.UUID, version int64, canApply func(domain.TenantStatus) bool, notAllowedCode domain.ErrorCode) error {
	current, err := store.GetTenantByID(ctx, db, id)
	if err == store.ErrNoRows {
		return domain.NewError(domain.ErrTenantNotFound, "tenant not found")
	}
	if err != nil {
		return err
	}
	if !canApply(current.Status) {
		return domain.NewError(notAllowedCode, "tenant status does not allow this operation")
	}
	return domain.NewError(domain.ErrTenantVersionConflict, "tenant version conflict")
}

// refetchTenant re-reads a just-created/updated tenant so the response reflects
// exactly what's committed (e.g. DB-assigned timestamps), rather than echoing
// back the in-memory struct the caller built before the INSERT.
func refetchTenant(ctx context.Context, db store.DB, id uuid.UUID) (store.Tenant, error) {
	return store.GetTenantByID(ctx, db, id)
}

func mustUUIDv7() uuid.UUID {
	id, err := uuid.NewV7()
	if err != nil {
		// uuid.NewV7 only fails if the system's CSPRNG is broken - unrecoverable.
		panic(err)
	}
	return id
}
