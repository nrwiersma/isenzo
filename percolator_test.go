package isenzo_test

import (
	"strconv"
	"testing"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/keyword"
	"github.com/nrwiersma/isenzo"
	"github.com/nrwiersma/isenzo/matchers"
)

func TestPercolator_Match(t *testing.T) {
	p, err := isenzo.NewPercolator()
	if err != nil {
		t.Fatalf("unexpected err; got %v", err)
	}

	err = p.Update([]isenzo.Query{
		isenzo.NewQuery("1", "foo:bar"),
		isenzo.NewQuery("2", "bar"),
		isenzo.NewQuery("3", "test"),
	})
	if err != nil {
		t.Fatalf("unexpected err; got %v", err)
	}

	data := map[string]interface{}{"foo": "bar"}

	results, err := p.Match(data)
	if err != nil {
		t.Fatalf("unexpected err; got %v", err)
	}

	if len(results.Errs) != 0 {
		t.Fatalf("expected no errors; got %v", results.Errs)
	}

	if len(results.Ids) != 2 {
		t.Fatalf("expected %d results; got %v", 2, len(results.Ids))
	}
}

func TestPercolator_UpdateWithErrors(t *testing.T) {
	p, err := isenzo.NewPercolator()
	if err != nil {
		t.Fatalf("unexpected err; got %v", err)
	}

	err = p.Update([]isenzo.Query{
		isenzo.NewQuery("1", "+-"),
	})
	if err == nil {
		t.Fatal("expected errors; got none")
	}
}

func BenchmarkPercolator_1Rules(b *testing.B) {
	b.ReportAllocs()

	rp := createRoutes(1)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, _ = rp.Match(data)
	}
}

func BenchmarkPercolator_10Rules(b *testing.B) {
	b.ReportAllocs()

	rp := createRoutes(10)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, _ = rp.Match(data)
	}
}

func BenchmarkPercolator_100Rules(b *testing.B) {
	b.ReportAllocs()

	rp := createRoutes(100)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, _ = rp.Match(data)
	}
}

func BenchmarkPercolator_500Rules(b *testing.B) {
	b.ReportAllocs()

	rp := createRoutes(500)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, _ = rp.Match(data)
	}
}

func BenchmarkPercolator_1000Rules(b *testing.B) {
	b.ReportAllocs()

	rp := createRoutes(1000)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, _ = rp.Match(data)
	}
}

func createRoutes(n int) *isenzo.Percolator {
	defaultMapping := bleve.NewIndexMapping()
	defaultMapping.DefaultAnalyzer = keyword.Name
	defaultMapping.StoreDynamic = false

	rp, _ := isenzo.NewPercolator(
		isenzo.WithMatcherFactory(matchers.NewIndexMatcherFactory(defaultMapping)),
	)

	qrys := make([]isenzo.Query, n)
	for i := 0; i < n-1; i++ {
		qrys[i] = isenzo.NewQuery(strconv.Itoa(i), "baz:bat")
	}

	qrys[n-1] = isenzo.NewQuery(strconv.Itoa(n-1), "foo:bar")

	rp.Update(qrys)

	return rp
}
