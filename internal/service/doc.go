// Package service holds the use cases (CreateTenant, UpdateTenant, DeleteTenant;
// ApplyTaskUpdate arrives in M3) and owns every transaction boundary (DESIGN.md
// §1): each use case opens exactly one store.WithTx, does its reads/writes, and
// commits - callers (httpapi, the consumer) never see a store.DB or a pgx type.
//
// Patterns used here:
//   - Transactional Outbox: CreateTenant/UpdateTenant/DeleteTenant each write their
//     task row in the same transaction as the tenant write, so the task (the
//     outbox row) exists if and only if the tenant change committed.
//   - Zero-rows-means-stop: a conditional UPDATE matching 0 rows is not an error,
//     it's a result the caller disambiguates by re-reading (see tenant.go).
package service
