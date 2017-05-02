package matchers_test

import (
	"testing"

	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search/query"
	"github.com/nrwiersma/isenzo/matchers"
)

func TestIndexMatcher(t *testing.T) {
	f := matchers.NewIndexMatcherFactory(mapping.NewIndexMapping())
	m, err := f.New(map[string]interface{}{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected err; got %v", err)
	}

	queries := []struct {
		Id    string
		Query query.Query
	}{
		{"1", query.NewQueryStringQuery("foo:bar")},
		{"2", query.NewQueryStringQuery("bar")},
		{"3", query.NewQueryStringQuery("test")},
	}
	for _, q := range queries {
		m.Match(q.Id, q.Query)
	}

	ids, errs := m.Finish()
	if len(errs) != 0 {
		t.Fatalf("expected no errors; got %v", errs)
	}

	if len(ids) != 2 {
		t.Fatalf("expected %d results; got %v", 2, len(ids))
	}
}

func TestIndexMatcher_WithErrors(t *testing.T) {
	f := matchers.NewIndexMatcherFactory(mapping.NewIndexMapping())
	m, err := f.New(map[string]interface{}{"foo": "bar"})
	if err != nil {
		t.Fatalf("unexpected err; got %v", err)
	}

	m.Match("1", query.NewQueryStringQuery("+-"))

	_, errs := m.Finish()
	if len(errs) == 0 {
		t.Fatal("expected errors; got none")
	}
}
