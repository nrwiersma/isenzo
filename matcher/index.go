package matcher

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
)

type IndexMatcher struct {
	index bleve.Index

	ids  []string
	errs []error
}

func IndexMatcherFactory(m mapping.IndexMapping) Factory {
	return func(doc map[string]interface{}) (Matcher, error) {
		i, err := bleve.NewMemOnly(m)
		if err != nil {
			return nil, err
		}

		if err := i.Index("doc", doc); err != nil {
			return nil, err
		}

		return &IndexMatcher{
			index: i,
			ids:   make([]string, 0),
			errs:  make([]error, 0),
		}, nil
	}
}

// Match matches a query with the matcher.
func (m *IndexMatcher) Match(id string, q query.Query) {
	req := bleve.NewSearchRequest(q)
	result, err := m.index.Search(req)
	if err != nil {
		m.errs = append(m.errs, err)
		return
	}

	if result.Total >= 1 {
		m.ids = append(m.ids, id)
	}
}

// Finish closes the matcher and returns the match results.
func (m *IndexMatcher) Finish() (ids []string, errs []error) {
	m.index.Close()

	return m.ids, m.errs
}
