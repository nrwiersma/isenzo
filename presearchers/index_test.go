package presearchers_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/collector"
	"github.com/nrwiersma/isenzo/presearchers"
)

func TestCrud(t *testing.T) {
	mapping := mapping.NewIndexMapping()
	index, err := presearchers.NewIndex(mapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doca := document.NewDocument("a")
	mapping.MapDocument(doca, map[string]interface{}{
		"name": "marty",
		"desc": "gophercon india",
	})
	err = index.Index(doca)
	if err != nil {
		t.Error(err)
	}

	docy := document.NewDocument("y")
	mapping.MapDocument(doca, map[string]interface{}{
		"name": "jasper",
		"desc": "clojure",
	})
	err = index.Index(docy)
	if err != nil {
		t.Error(err)
	}

	err = index.Delete("y")
	if err != nil {
		t.Error(err)
	}

	count, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected doc count 1, got %d", count)
	}
}

func TestIndex(t *testing.T) {

	index, err := presearchers.NewIndex(mapping.NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClosedIndex(t *testing.T) {
	index, err := presearchers.NewIndex(mapping.NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = index.Index(document.NewDocument("doc"))
	if err != presearchers.ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	err = index.Delete("test")
	if err != presearchers.ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	_, err = index.DocCount()
	if err != presearchers.ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	_, err = index.Search(bleve.NewTermQuery("test"), collector.NewTopNCollector(10, 0, search.SortOrder{}))
	if err != presearchers.ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}
}

func TestIndexCountMatchSearch(t *testing.T) {
	index, err := presearchers.NewIndex(mapping.NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 200; j++ {
				id := fmt.Sprintf("%d", (i*200)+j)
				doc := document.NewDocument(id)
				doc.AddField(document.NewTextField("body", nil, []byte("match")))
				doc.AddField(document.NewCompositeField("_all", true, []string{}, []string{}))
				err := index.Index(doc)
				if err != nil {
					t.Fatal(err)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// search for something that should match all documents
	sr, err := index.Search(bleve.NewMatchQuery("match"), collector.NewTopNCollector(2000, 0, search.SortOrder{}))
	if err != nil {
		t.Fatal(err)
	}

	// get the index document count
	dc, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}

	// make sure test is working correctly, doc count should 2000
	if dc != 2000 {
		t.Errorf("expected doc count 2000, got %d", dc)
	}

	// make sure our search found all the documents
	if dc != sr.Total {
		t.Errorf("expected search result total %d to match doc count %d", sr.Total, dc)
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}
