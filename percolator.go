package isenzo

import (
	"sync"
	"time"

	"github.com/bcampbell/qs"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/nrwiersma/isenzo/matcher"
	"github.com/nrwiersma/isenzo/presearcher"
)

type optionsFunc func(*Percolator)

// WithMatcherFactory sets the matcher factory on the Percolator.
func WithMatcherFactory(matcher matcher.Factory) optionsFunc {
	return func(p *Percolator) {
		p.matcher = matcher
	}
}

// Percolator represents a percolator instance.
type Percolator struct {
	cache     map[string]query.Query
	cacheLock sync.RWMutex

	queryIndex presearcher.Index
	matcher    matcher.Factory
}

// NewPercolator creates a new Percolator.
func NewPercolator(opts ...optionsFunc) (*Percolator, error) {
	queryIndex, err := presearcher.NewIndex(bleve.NewIndexMapping())
	if err != nil {
		return nil, err
	}

	p := &Percolator{
		cache:      map[string]query.Query{},
		queryIndex: queryIndex,
	}

	for _, o := range opts {
		o(p)
	}

	if p.matcher == nil {
		p.matcher = matcher.IndexMatcherFactory(bleve.NewIndexMapping())
	}

	return p, nil
}

// Update sets the Rules on the Percolator.
func (p *Percolator) Update(qrys []*Query) {
	p.cacheLock.Lock()
	defer p.cacheLock.Unlock()

	for _, qry := range qrys {
		q, _ := qs.Parse(qry.Query)
		//TODO: Handle this error

		p.cache[qry.Id] = q
	}
}

// Matches matches a document and applies the changes on the first matching Query.
func (p *Percolator) Match(doc map[string]interface{}) (*Results, error) {
	startMatch := time.Now()

	//TODO: Pre-search queries

	m, err := p.matcher(doc)
	if err != nil {
		return nil, err
	}

	// Run queries
	p.cacheLock.RLock()
	defer p.cacheLock.RUnlock()

	for id, qry := range p.cache {
		m.Match(id, qry)
	}

	ids, errs := m.Finish()
	return &Results{
		Ids:        ids,
		Errs:       errs,
		Took:       time.Since(startMatch),
		QueriesRun: len(p.cache),
	}, nil
}
