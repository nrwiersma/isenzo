package percolator

import (
	"github.com/blevesearch/bleve/search/query"
)

// Rules represents a set of rules.
type Rules []Rule

// Rule represents a percolator rule, consisting of a query and a set of changes.
type Rule struct {
	Query   query.Query
	Changes map[string]interface{}
}
