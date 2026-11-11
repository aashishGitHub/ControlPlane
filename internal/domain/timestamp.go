package domain

import (
	"fmt"
	"strings"
	"time"
)

// wireLayout is the brief's exact timestamp contract: ISO 8601, milliseconds, "Z"
// suffix - e.g. "2024-01-02T15:04:05.123Z". Go's encoding/json default for
// time.Time uses RFC3339Nano with a variable number of fractional digits and
// "+00:00" instead of "Z" for UTC, so the default marshaller does not satisfy the
// brief and a dedicated type is needed.
const wireLayout = "2006-01-02T15:04:05.000Z"

// Timestamp wraps time.Time to control exactly how it's rendered on the wire.
//
// Pattern: Value Object. Why: every API response field that's a timestamp uses
// this type instead of time.Time directly, so the wire format is enforced by the
// type system, not by remembering to call .Format(...) correctly at every call
// site.
type Timestamp time.Time

func NewTimestamp(t time.Time) Timestamp { return Timestamp(t.UTC()) }

func (t Timestamp) Time() time.Time { return time.Time(t) }

func (t Timestamp) MarshalJSON() ([]byte, error) {
	s := time.Time(t).UTC().Format(wireLayout)
	return []byte(`"` + s + `"`), nil
}

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	s := strings.Trim(string(data), `"`)
	parsed, err := time.Parse(wireLayout, s)
	if err != nil {
		// Be lenient on the way in (RFC3339 variants), strict on the way out.
		// Worker/test tooling may send slightly different precision; we only
		// control our own output format, not every producer's input.
		parsed, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return fmt.Errorf("domain: invalid timestamp %q: %w", s, err)
		}
	}
	*t = Timestamp(parsed.UTC())
	return nil
}
