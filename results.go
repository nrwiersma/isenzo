package isenzo

import "time"

// Results represents the results of a match.
type Results struct {
	Ids        []string
	Errs       []error
	Took       time.Duration
	QueriesRun int
}
