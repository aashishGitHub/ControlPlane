// Package domain holds the pure business rules of the control plane: the tenant and
// task state machines, slug validation, the fixed error-code vocabulary, and the
// wire timestamp format. Nothing in this package touches a database, an HTTP request,
// or a broker — that's deliberate (Ports and Adapters / hexagonal architecture): the
// rules that actually have to be correct (state transitions, concurrency semantics)
// are testable with zero setup, no containers, no mocks.
//
// Patterns used here:
//   - Table-driven State Machine: transitions are expressed as maps/lookup tables,
//     not scattered if/else chains, so adding or auditing a transition is a one-line
//     diff (see statemachine.go).
//   - Value Object: Slug and TaskStatus validate themselves at construction so invalid
//     states are unrepresentable past the boundary.
package domain
