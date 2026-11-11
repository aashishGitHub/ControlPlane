// Package httpapi is the HTTP adapter (DESIGN.md §4): chi routing, request
// decoding/validation, the {tenant, task} response shape, the error envelope, and
// keyset pagination. It talks only to *service.Service - no store or pgx types
// cross into this package.
//
// Patterns used here:
//   - Error envelope with a fixed code vocabulary (domain.ErrorCode) mapped to
//     HTTP status in one table (errors.go), so adding a new domain error never
//     means hunting through handlers for a missing case.
//   - Keyset pagination (opaque cursor = last-seen UUIDv7 id), not OFFSET.
package httpapi
