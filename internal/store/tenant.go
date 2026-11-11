package store

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/aashishGithub/controlplane/internal/domain"
)

// Tenant mirrors the tenants table. The service layer converts to/from the HTTP
// wire type; store stays ignorant of JSON.
type Tenant struct {
	ID        uuid.UUID
	Slug      string
	Name      string
	Status    domain.TenantStatus
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CreateTenant inserts a new tenant row. On a duplicate slug, Postgres returns a
// 23505 unique_violation (check with IsUniqueViolation) - see DESIGN.md §5: the
// unique index is the arbiter, there's no pre-check SELECT.
func CreateTenant(ctx context.Context, db DB, t Tenant) error {
	_, err := db.Exec(ctx, `
		INSERT INTO tenants (id, slug, name, status, version)
		VALUES ($1, $2, $3, $4, $5)`,
		t.ID, t.Slug, t.Name, t.Status, t.Version)
	return err
}

func GetTenantByID(ctx context.Context, db DB, id uuid.UUID) (Tenant, error) {
	row := db.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants WHERE id = $1`, id)
	return scanTenant(row)
}

// GetTenantForUpdate locks the tenant row. The consumer takes this lock before the
// task lock (DESIGN.md §6 Inbound step 3) to match the API path's tenant -> task
// lock order and avoid deadlocks; it also stabilizes the expected-status guard
// while the terminal outcome is applied.
func GetTenantForUpdate(ctx context.Context, db DB, id uuid.UUID) (Tenant, error) {
	row := db.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants WHERE id = $1
		FOR UPDATE`, id)
	return scanTenant(row)
}

// ListTenants returns up to limit tenants with id > cursor (zero UUID means "from
// the start"), optionally filtered by status, ordered by id. DESIGN.md §4: id is
// UUIDv7 (time-ordered), so this keyset is equivalent to "created after cursor"
// without the page-drift OFFSET pagination has under concurrent writes.
func ListTenants(ctx context.Context, db DB, status string, cursor uuid.UUID, limit int) ([]Tenant, error) {
	rows, err := db.Query(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants
		WHERE id > $1 AND ($2 = '' OR status = $2)
		ORDER BY id
		LIMIT $3`, cursor, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Tenant
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query, after
// Next()), so scanTenant works for both a single lookup and a list.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanTenant(row rowScanner) (Tenant, error) {
	var t Tenant
	err := row.Scan(&t.ID, &t.Slug, &t.Name, &t.Status, &t.Version, &t.CreatedAt, &t.UpdatedAt)
	return t, err
}

// UpdateTenant is the PATCH mechanism (DESIGN.md §5): a single conditional UPDATE,
// not a SELECT-then-UPDATE. matched is false when the row didn't satisfy
// id/version/status - the caller (service layer) then does a fresh GetTenantByID
// to disambiguate 404 / update_not_allowed / version_conflict, since the zero-rows
// result alone can't tell those three apart.
func UpdateTenant(ctx context.Context, db DB, id uuid.UUID, name string, version int64) (Tenant, bool, error) {
	row := db.QueryRow(ctx, `
		UPDATE tenants
		SET name = $2, status = 'updating', version = version + 1, updated_at = now()
		WHERE id = $1 AND version = $3 AND status = 'active'
		RETURNING id, slug, name, status, version, created_at, updated_at`,
		id, name, version)
	t, err := scanTenant(row)
	if err == ErrNoRows {
		return Tenant{}, false, nil
	}
	if err != nil {
		return Tenant{}, false, err
	}
	return t, true, nil
}

// DeleteTenant is the same conditional-UPDATE shape as UpdateTenant, moving the
// tenant to "destroying" from either "active" or "failed" (DESIGN.md §5: a tenant
// stuck in failed can still be torn down).
func DeleteTenant(ctx context.Context, db DB, id uuid.UUID, version int64) (Tenant, bool, error) {
	row := db.QueryRow(ctx, `
		UPDATE tenants
		SET status = 'destroying', version = version + 1, updated_at = now()
		WHERE id = $1 AND version = $2 AND status IN ('active', 'failed')
		RETURNING id, slug, name, status, version, created_at, updated_at`,
		id, version)
	t, err := scanTenant(row)
	if err == ErrNoRows {
		return Tenant{}, false, nil
	}
	if err != nil {
		return Tenant{}, false, err
	}
	return t, true, nil
}
