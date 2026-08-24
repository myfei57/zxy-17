// Package retry implements retry policy and deduplicated attempts.
package retry

// Policy controls how many times a task may be retried.
type Policy struct {
	MaxAttempts int
}

// NewPolicy creates a retry policy with the given attempt cap.
func NewPolicy(maxAttempts int) *Policy {
	return &Policy{MaxAttempts: maxAttempts}
}

// Allowed reports whether another attempt may run.
func (p *Policy) Allowed(attempt int) bool {
	return attempt < p.MaxAttempts
}
