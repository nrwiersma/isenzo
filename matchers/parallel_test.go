package matchers_test

import (
	"testing"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/nrwiersma/isenzo/matchers"
)

func TestParallelMatcher(t *testing.T) {
	f :=  matchers.NewParallelMatcherFactory(newWaitMatcherFactory(), 10)
	m, err := f.New(map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected err; got %v", err)
	}

	for i := 0; i < 10; i++ {
		m.Match("", bleve.NewMatchAllQuery())
	}

	ids, errs := m.Finish()
	if len(errs) != 0 {
		t.Fatalf("expected no errors; got %v", errs)
	}

	if len(ids) != 10 {
		t.Fatalf("expected %d results; got %v", 10, len(ids))
	}
}

type waitMatcherFactory struct {}

func newWaitMatcherFactory() matchers.Factory {
	return &waitMatcherFactory{}
}

func (f waitMatcherFactory) New(doc interface{}) (matchers.Matcher, error) {
	return &waitMatcher{
		ids: make([]string, 0),
	}, nil
}

func (f waitMatcherFactory) Map(doc interface{}) (interface{}, error) {
	return doc, nil
}

type waitMatcher struct {
	ids []string
}

// Match matches a query with the matcher.
func (m *waitMatcher) Match(id string, q query.Query) {
	time.Sleep(10 * time.Millisecond)

	m.ids = append(m.ids, id)
}

// Finish closes the matcher and returns the match results.
func (m *waitMatcher) Finish() (ids []string, errs []error) {
	return m.ids, []error{}
}
