package percolator

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/search/query"
)

func TestPercolator_SetRules(t *testing.T) {
	rp := &Percolator{}

	rules := &Rules{
		Rule{},
	}
	rp.SetRules(rules)

	if rp.rules == nil {
		t.Fatal("expected rules; got nil")
	}

	if rp.rules != rules {
		t.Fatalf("expected rules %v; got %v", rules, rp.rules)
	}
}

func TestPercolator_Process(t *testing.T) {
	rp := &Percolator{}
	rp.SetRules(&Rules{
		Rule{
			Query: query.NewQueryStringQuery("foo:bar"),
			Changes: map[string]interface{}{
				"baz": 1,
			},
		},
	})

	data := map[string]interface{}{
		"foo": "bar",
	}

	data, errs := rp.Process(data)
	if len(errs) != 0 {
		t.Fatalf("expected no errors; got %v", errs)
	}

	if data["foo"] != "bar" {
		t.Fatalf("expected foo = %v; got %v", "bar", data["foo"])
	}

	if data["baz"] != 1 {
		t.Fatalf("expected baz = %v; got %v", 1, data["baz"])
	}
}

func TestPercolator_ProcessComplexChanges(t *testing.T) {
	rp := &Percolator{}
	rp.SetRules(&Rules{
		Rule{
			Query: query.NewQueryStringQuery("foo:bar"),
			Changes: map[string]interface{}{
				"baz": map[string]interface{}{"bat": 1},
			},
		},
	})

	want := map[string]interface{}{
		"foo": "bar",
		"baz": map[string]interface{}{"bat": 1},
	}
	data := map[string]interface{}{
		"foo": "bar",
	}

	got, errs := rp.Process(data)
	if len(errs) != 0 {
		t.Fatalf("expected no errors; got %v", errs)
	}

	if reflect.DeepEqual(want, got) {
		t.Fatalf("expected %v; got %v", want, got)
	}
}

func TestPercolator_ProcessWithErrors(t *testing.T) {
	rp := &Percolator{}
	rp.SetRules(&Rules{
		Rule{
			Query: query.NewQueryStringQuery("+-"),
		},
	})

	data := map[string]interface{}{
		"foo": "bar",
	}

	_, errs := rp.Process(data)
	if len(errs) != 1 {
		t.Fatal("expected errors; got none")
	}
}

func BenchmarkPercolator_10Rules(b *testing.B) {
	rp := createRoutes(10)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		data, _ = rp.Process(data)
	}
}

func BenchmarkPercolator_100Rules(b *testing.B) {
	rp := createRoutes(100)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		data, _ = rp.Process(data)
	}
}

func BenchmarkPercolator_1000Rules(b *testing.B) {
	rp := createRoutes(1000)
	data := map[string]interface{}{"foo": "bar"}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		data, _ = rp.Process(data)
	}
}

func createRoutes(n int) *Percolator {
	rp := &Percolator{}

	rules := make(Rules, n)
	for i := 0; i < n - 1; i++ {
		rules[i] = Rule{
			Query: query.NewQueryStringQuery("baz:bat"),
			Changes: map[string]interface{}{},
		}
	}

	rules[n - 1] = Rule{
		Query: query.NewQueryStringQuery("foo:bar"),
		Changes: map[string]interface{}{"test": 1},
	}

	rp.SetRules(&rules)

	return rp
}
