package matcher

import (
	"github.com/blevesearch/bleve/search/query"
)

// Factory represents a matcher factory
type Factory func(doc map[string]interface{}) (Matcher, error)

// Matcher represents a query matcher
type Matcher interface {
	// Match matches a query with the matcher.
	Match(id string, q query.Query)

	// Finish closes the matcher and returns the match results.
	Finish() (ids []string, errs []error)
}
