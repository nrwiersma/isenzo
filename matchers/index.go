package matchers

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/nrwiersma/isenzo/util"
	"github.com/pkg/errors"
)

// IndexMatcherFactory represents a factory for IndexMatcher.
type IndexMatcherFactory struct {
	mapping mapping.IndexMapping
	pool    *util.Pool
}

// NewIndexMatcherFactory creates a new IndexMatcherFactory.
func NewIndexMatcherFactory(m mapping.IndexMapping) Factory {
	pool := util.NewPool(1024)
	pool.New = func() interface{} {
		i, err := bleve.NewMemOnly(m)
		if err != nil {
			return nil
		}

		return i
	}

	return &IndexMatcherFactory{
		mapping: m,
		pool:    pool,
	}
}

// New creates a new query matcher.
func (f IndexMatcherFactory) New(doc interface{}) (Matcher, error) {
	var err error
	i, ok := f.pool.Get().(bleve.Index)
	if !ok {
		return nil, errors.New("could not create index")
	}

	if doc, err = f.Map(doc); err != nil {
		return nil, err
	}

	index, _, err := i.Advanced()
	if err != nil {
		return nil, err
	}

	if err := index.Update(doc.(*document.Document)); err != nil {
		return nil, err
	}

	return &IndexMatcher{
		index: i,
		closing: func() {
			f.pool.Put(i)
		},
		ids:  make([]string, 0),
		errs: make([]error, 0),
	}, nil
}

// Map maps a document for the matcher.
func (f IndexMatcherFactory) Map(doc interface{}) (interface{}, error) {
	if _, ok := doc.(*document.Document); ok {
		return doc, nil
	}

	d := document.NewDocument("doc")
	if err := f.mapping.MapDocument(d, doc); err != nil {
		return nil, err
	}

	return d, nil
}

// IndexMatcher represents a bleve index matcher.
type IndexMatcher struct {
	index bleve.Index

	closing func()

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

	if m.closing != nil {
		m.closing()
	}

	return m.ids, m.errs
}
