package pkg

import (
	"time"
)

// TimeProvider provides the current time
type TimeProvider interface {
	Now() time.Time
}

// RealTimeProvider provides the actual current time
type RealTimeProvider struct{}

func (r RealTimeProvider) Now() time.Time {
	return time.Now()
}
