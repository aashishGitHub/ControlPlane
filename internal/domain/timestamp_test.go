package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTimestampMarshalJSON(t *testing.T) {
	// A time with sub-millisecond precision and a non-UTC offset, to prove we
	// truncate to milliseconds and normalize to UTC "Z" regardless of input.
	loc := time.FixedZone("test", 5*60*60) // UTC+5
	in := time.Date(2024, 1, 2, 20, 4, 5, 123_456_789, loc)
	ts := NewTimestamp(in)

	got, err := json.Marshal(ts)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// 20:04:05 +05:00 == 15:04:05 UTC. Nanoseconds truncate to .123 ms.
	want := `"2024-01-02T15:04:05.123Z"`
	if string(got) != want {
		t.Errorf("Marshal = %s, want %s", got, want)
	}
}

func TestTimestampUnmarshalJSON(t *testing.T) {
	var ts Timestamp
	err := json.Unmarshal([]byte(`"2024-01-02T15:04:05.123Z"`), &ts)
	if err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	want := time.Date(2024, 1, 2, 15, 4, 5, 123_000_000, time.UTC)
	if !ts.Time().Equal(want) {
		t.Errorf("Unmarshal = %v, want %v", ts.Time(), want)
	}
}

func TestTimestampRoundTrip(t *testing.T) {
	in := NewTimestamp(time.Now())
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var out Timestamp
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	// Round trip loses sub-millisecond precision by design (the wire format is
	// ms-precision), so compare truncated-to-ms, not exact equality.
	if !in.Time().Truncate(time.Millisecond).Equal(out.Time()) {
		t.Errorf("round trip mismatch: in=%v out=%v", in.Time(), out.Time())
	}
}
