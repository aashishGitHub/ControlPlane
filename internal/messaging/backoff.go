package messaging

import (
	"math/rand/v2"
	"time"
)

// withJitter spreads retries so replicas that lost the broker at the same moment
// do not reconnect in a synchronized storm. Half fixed + half random keeps the
// growth roughly exponential while guaranteeing some spacing (DESIGN.md §6
// Resilience).
func withJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
