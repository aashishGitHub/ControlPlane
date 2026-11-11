package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// DB is the subset of pgx's query surface that both *pgxpool.Pool and pgx.Tx
// satisfy. Store functions take a DB instead of a concrete pool or tx so the same
// function runs standalone (simple reads, e.g. a GET handler) or as one statement
// inside a caller-owned transaction (the service layer composing a multi-step use
// case) - the function itself never decides where the transaction boundary is.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ErrNoRows is pgx.ErrNoRows re-exported so callers outside this package don't
// need to import pgx just to check for "not found".
var ErrNoRows = pgx.ErrNoRows

// IsUniqueViolation reports whether err is a Postgres unique_violation (pg code
// 23505) - e.g. a duplicate slug. DESIGN.md §5: the DB's unique index is the
// arbiter of the create-slug race, so the service layer maps this one error code
// to domain.ErrTenantAlreadyExists instead of pre-checking "does this slug exist".
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// Open connects to Postgres and applies the embedded schema (idempotent:
// CREATE ... IF NOT EXISTS). One call does both so every entrypoint (the
// controlplane binary, integration tests) boots from a guaranteed-migrated DB
// without a separate migration step.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}
	return pool, nil
}

// WithTx runs fn inside a single transaction: begin, fn, then commit on success or
// rollback on error/panic. The service layer uses this to own transaction
// boundaries (DESIGN.md §1) while every store function stays transaction-agnostic
// (it just takes a DB).
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(db DB) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if already committed

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("store: commit: %w", err)
	}
	return nil
}
