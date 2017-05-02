package matchers

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
)

// IndexMatcherFactory represents a factory for IndexMatcher.
type IndexMatcherFactory struct {
	mapping mapping.IndexMapping
}

// NewIndexMatcherFactory creates a new IndexMatcherFactory.
func NewIndexMatcherFactory(m mapping.IndexMapping) Factory {
	return &IndexMatcherFactory{
		mapping: m,
	}
}

// New creates a new query matcher.
func (f IndexMatcherFactory) New(doc interface{}) (Matcher, error) {
	i, err := bleve.NewMemOnly(f.mapping)
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

// Map maps a document for the matcher.
func (f IndexMatcherFactory) Map(doc interface{}) interface{} {
	if _, ok := doc.(*document.Document); ok {
		return doc
	}

	d := document.NewDocument("doc")
	f.mapping.MapDocument(d, doc)

	return d
}

// IndexMatcher represents a bleve index matcher.
type IndexMatcher struct {
	index bleve.Index

	ids  []string
	errs []error
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
