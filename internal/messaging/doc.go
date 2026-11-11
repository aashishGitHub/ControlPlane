// Package messaging is the AMQP adapter: connection lifecycle, topology,
// publisher confirms, and the idempotent consumer.
//
// Patterns used here:
//   - Connection supervisor with exponential backoff + jitter (connection.go).
//   - Publisher confirms: mark outbox rows published only after broker ack
//     (publisher.go).
//   - Idempotent consumer: deterministic message_id + inbox + rank guard; ack
//     only after commit (consumer.go; inbox/rank live in service/store).
//   - Poison vs transient classification: permanent failures dead-letter,
//     transient failures retry then requeue (consumer.go).
//   - Idempotent topology declaration, re-run after every reconnect
//     (topology.go).
//
// DESIGN.md §6 is the contract; §7 rows 2, 3, 4 are the relevant decisions.
package messaging
