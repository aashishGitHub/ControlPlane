// Package service holds the use cases (CreateTenant, UpdateTenant, DeleteTenant,
// ApplyTaskUpdate) and owns every transaction boundary (DESIGN.md §1): each use
// case opens exactly one store.WithTx, does its reads/writes, and commits -
// callers (httpapi, the consumer) never see a store.DB or a pgx type.
//
// Patterns used here:
//   - Transactional Outbox: CreateTenant/UpdateTenant/DeleteTenant each write their
//     task row in the same transaction as the tenant write, so the task (the
//     outbox row) exists if and only if the tenant change committed.
//   - Zero-rows-means-stop: a conditional UPDATE matching 0 rows is not an error,
//     it's a result the caller disambiguates by re-reading (see tenant.go).
//   - Idempotent consumer + transactional inbox: ApplyTaskUpdate inserts the
//     message_id in processed_messages and applies the state change in one
//     transaction, so a crash can never mark a message processed without its
//     effect (DESIGN.md §6 Inbound, §7 row 3).
//   - Rank guard: an update applies iff rank(new) > rank(current); duplicates,
//     stale/reordered updates, and first-terminal-wins fall out in one comparison
//     (DESIGN.md §3, §7 row 4).
package service
