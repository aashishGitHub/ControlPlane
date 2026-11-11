package httpapi

import (
	"net/http"
	"strconv"

	"github.com/google/uuid"
)

const (
	defaultLimit = 20
	maxLimit     = 100
)

// parseCursor reads ?cursor=<uuid>, defaulting to uuid.Nil ("from the start") -
// DESIGN.md §4: the cursor is the opaque last-seen UUIDv7 id, so an invalid or
// absent cursor just restarts from the beginning rather than erroring.
func parseCursor(r *http.Request) uuid.UUID {
	id, err := uuid.Parse(r.URL.Query().Get("cursor"))
	if err != nil {
		return uuid.Nil
	}
	return id
}

func parseLimit(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return defaultLimit
	}
	if n > maxLimit {
		return maxLimit
	}
	return n
}

// nextCursor returns the opaque cursor a client should pass next, or "" once a
// page comes back shorter than the requested limit (the signal there's no more
// data - DESIGN.md §4).
func nextCursor(lastID uuid.UUID, pageLen, limit int) string {
	if pageLen < limit {
		return ""
	}
	return lastID.String()
}
