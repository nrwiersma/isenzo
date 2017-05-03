package isenzo

import (
	"sync"
	"time"

	"github.com/bcampbell/qs"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/nrwiersma/isenzo/matchers"
	"github.com/nrwiersma/isenzo/presearchers"
)

type optionsFunc func(*Percolator)

// WithMatcherFactory sets the matcher factory on the Percolator.
func WithMatcherFactory(matcher matchers.Factory) optionsFunc {
	return func(p *Percolator) {
		p.matcher = matcher
	}
}

// WithPresearcher sets the presearcher on the Percolator.
func WithPresearcher(presearcher presearchers.Presearcher) optionsFunc {
	return func(p *Percolator) {
		p.presearcher = presearcher
	}
}

// Percolator represents a percolator instance.
type Percolator struct {
	cache     map[string]query.Query
	cacheLock sync.RWMutex

	queryIndex  *presearchers.Index
	presearcher presearchers.Presearcher
	matcher     matchers.Factory
}

// NewPercolator creates a new Percolator.
func NewPercolator(opts ...optionsFunc) (*Percolator, error) {
	queryIndex, err := presearchers.NewIndex(bleve.NewIndexMapping())
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
		p.matcher = matchers.NewIndexMatcherFactory(bleve.NewIndexMapping())
	}

	if p.presearcher == nil {
		p.presearcher = &presearchers.TermPresearcher{}
	}

	return p, nil
}

// Update sets the queries on the Percolator.
func (p *Percolator) Update(qrys []Query) error {
	p.cacheLock.Lock()
	defer p.cacheLock.Unlock()

	for _, qry := range qrys {
		q, err := qs.Parse(qry.Query)
		if err != nil {
			return err
		}

		p.cache[qry.Id] = q
	}

	return nil
}

// Matches matches a document and applies the changes on the first matching Query.
func (p *Percolator) Match(doc map[string]interface{}) (*Results, error) {
	startMatch := time.Now()

	//TODO: Pre-search queries

	m, err := p.matcher.New(doc)
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
		QueriesRun: -1,
	}, nil
}
