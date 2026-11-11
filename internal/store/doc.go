// Package store is the Postgres adapter: schema application, the pgx connection
// pool, and the query functions the service layer composes into transactions. No
// query here opens its own transaction — every multi-statement unit of work is a
// method on *Tx (see pool.go), so the service layer (which owns transaction
// boundaries per DESIGN.md §1) decides where a tx starts and commits, not this
// package.
//
// Patterns used here:
//   - Embedded schema, applied idempotently (CREATE ... IF NOT EXISTS) at startup,
//     instead of a migration tool — DESIGN.md says the schema is small enough that
//     a migration framework would be pure overhead for this project.
//   - Conditional UPDATE for optimistic locking (DESIGN.md §5): no SELECT-then-UPDATE
//     race window, because there's no read-then-write gap.
package store
