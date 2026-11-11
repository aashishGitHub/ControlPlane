// Package worker is the worker simulator: it consumes outbound task messages,
// waits a simulated processing time, and publishes progress updates back to the
// control plane.
//
// Patterns used here:
//   - Deterministic update IDs: UUIDv5(task_id/status), so worker retries and
//     duplicates collapse in the consumer's inbox (DESIGN.md §6 Inbound).
//   - Ack-after-confirm: the task message is acked only after the terminal update
//     was confirmed by the broker; a crash mid-task leaves the task message
//     unacked and the broker redelivers it (DESIGN.md §8).
//   - Fault injection by configuration: fail-rate, duplicate-rate and reorder are
//     flags/env vars, not code paths that only tests can reach (plan.md M4).
package worker
