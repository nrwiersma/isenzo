package isenzo

// Query represents a percolator rule, consisting of a query and a set of changes.
type Query struct {
	Id    string
	Query string
}

// NewQuery creates a new Query.
func NewQuery(id, query string) *Query {
	return &Query{
		Id:    id,
		Query: query,
	}
}
