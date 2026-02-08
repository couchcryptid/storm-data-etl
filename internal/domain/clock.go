package domain

import "github.com/jonboulle/clockwork"

var clock = clockwork.NewRealClock()

// SetClock swaps the time source for enrichment. Pass nil to reset to real time.
func SetClock(c clockwork.Clock) {
	if c == nil {
		clock = clockwork.NewRealClock()
		return
	}
	clock = c
}
