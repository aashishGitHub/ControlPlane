// Package outbox is the relay loop bridging store -> messaging.
//
// Patterns used here:
//   - Transactional outbox: the tasks table is the outbox; published_at IS NULL
//     means "not yet confirmed by the broker" (DESIGN.md §6 Outbound).
//   - Claim with FOR UPDATE SKIP LOCKED, held across publish + mark in one
//     transaction (DESIGN.md §7 row 2).
//   - Publisher confirms before published_at is set; at-least-once delivery.
//   - Post-commit nudge with a 500 ms poll as the missed-nudge safety net
//     (DESIGN.md §6 / §7).
package outbox
