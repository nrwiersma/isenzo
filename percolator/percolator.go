package percolator

import (
	"sync"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/mapping"
)

// Percolator represents a percolator instance.
type Percolator struct {
	rules *Rules
	lock  sync.RWMutex

	mapping mapping.IndexMapping
}

// New creates a new Percolator.
func New() *Percolator {
	return &Percolator{
		mapping: bleve.NewIndexMapping(),
	}
}

// SetRules sets the Rules on the Percoaltor.
func (rp *Percolator) SetRules(rules *Rules) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	rp.rules = rules
}

// Process percolates a document and applies the changes on the first matching Rule.
func (rp *Percolator) Process(data map[string]interface{}) (map[string]interface{}, []error) {
	index, err := bleve.NewUsing("", rp.mapping, bleve.Config.DefaultIndexType, gtreap.Name, nil)
	if err != nil {
		return data, []error{err}
	}
	defer index.Close()

	if err := index.Index("doc", data); err != nil {
		return data, []error{err}
	}

	rp.lock.RLock()
	defer rp.lock.RUnlock()

	errors := []error{}
	for _, rule := range *rp.rules {
		req := bleve.NewSearchRequest(rule.Query)
		result, err := index.Search(req)
		if err != nil {
			errors = append(errors, err)

			continue
		}

		if result.Total >= 1 {
			data = rp.applyChanges(data, rule.Changes)

			break
		}
	}

	return data, errors
}

func (rp *Percolator) applyChanges(
	data map[string]interface{},
	changes map[string]interface{},
) map[string]interface{} {
	for key, value := range changes {
		if sub, ok := value.(map[string]interface{}); ok {
			data = rp.applyChanges(data, sub)

			continue
		}

		data[key] = value
	}

	return data
}
