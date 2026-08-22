package lease

import "time"

func timeNow() int64 {
	return time.Now().UnixNano()
}

// IsExpired reports whether a lease passed its deadline.
func (l *Lease) IsExpired(now int64) bool {
	return l.State == Granted && now > l.Until
}
