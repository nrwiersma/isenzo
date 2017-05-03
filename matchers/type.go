package matchers

import (
	"github.com/blevesearch/bleve/search/query"
)

// Factory represents a matcher factory
type Factory interface {
	// New creates a new query matcher.
	New(doc interface{}) (Matcher, error)

	// Map maps a document for the matcher.
	Map(doc interface{}) (interface{}, error)
}

// Matcher represents a query matcher
type Matcher interface {
	// Match matches a query with the matcher.
	Match(id string, q query.Query)

	// Finish closes the matcher and returns the match results.
	Finish() (ids []string, errs []error)
}
