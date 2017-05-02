package presearchers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/registry"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
)

var (
	ErrorEmptyID     = errors.New("document ID cannot be empty")
	ErrorIndexClosed = errors.New("index is closed")
)

type Index struct {
	i     index.Index
	m     mapping.IndexMapping
	mutex sync.RWMutex
	open  bool
}

// NewIndex creates a memory-only index.
func NewIndex(mapping mapping.IndexMapping) (*Index, error) {
	// First validate the mapping
	err := mapping.Validate()
	if err != nil {
		return nil, err
	}

	i := &Index{
		m: mapping,
	}

	// Store configuration
	storeConfig := map[string]interface{}{}
	storeConfig["path"] = ""
	storeConfig["create_if_missing"] = false
	storeConfig["error_if_exists"] = false

	// open the index
	indexTypeConstructor := registry.IndexTypeConstructorByName(bleve.Config.DefaultIndexType)
	i.i, err = indexTypeConstructor(bleve.Config.DefaultMemKVStore, storeConfig, index.NewAnalysisQueue(4))
	if err != nil {
		return nil, err
	}
	err = i.i.Open()
	if err != nil {
		return nil, err
	}

	// mark the index as open
	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.open = true

	return i, nil
}

// Index indexes a document.Document.
func (i *Index) Index(doc *document.Document) (err error) {
	if doc.ID == "" {
		return ErrorEmptyID
	}

	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err = i.i.Update(doc)
	return
}

// Delete entries for the specified identifier from
// the index.
func (i *Index) Delete(id string) (err error) {
	if id == "" {
		return ErrorEmptyID
	}

	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return ErrorIndexClosed
	}

	err = i.i.Delete(id)
	return
}

// DocCount returns the number of documents in the
// index.
func (i *Index) DocCount() (count uint64, err error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if !i.open {
		return 0, ErrorIndexClosed
	}

	// open a reader for this search
	indexReader, err := i.i.Reader()
	if err != nil {
		return 0, fmt.Errorf("error opening index reader %v", err)
	}
	defer func() {
		if cerr := indexReader.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	count, err = indexReader.DocCount()
	return
}

// Search executes a search request operation.
func (i *Index) Search(qry query.Query, col search.Collector) (sr *bleve.SearchResult, err error) {
	return i.SearchInContext(context.Background(), qry, col)
}

// SearchInContext executes a search request operation within the provided Context.
func (i *Index) SearchInContext(
	ctx context.Context,
	qry query.Query,
	collector search.Collector,
) (sr *bleve.SearchResult, err error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	searchStart := time.Now()

	if !i.open {
		return nil, ErrorIndexClosed
	}

	// Open a reader for this search
	indexReader, err := i.i.Reader()
	if err != nil {
		return nil, fmt.Errorf("error opening index reader %v", err)
	}
	defer func() {
		if ierr := indexReader.Close(); err == nil && ierr != nil {
			err = ierr
		}
	}()

	// Get the searcher from the query
	searcher, err := qry.Searcher(indexReader, i.m, search.SearcherOptions{
		Explain:            false,
		IncludeTermVectors: false,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if serr := searcher.Close(); err == nil && serr != nil {
			err = serr
		}
	}()

	err = collector.Collect(ctx, searcher, indexReader)
	if err != nil {
		return nil, err
	}

	return &bleve.SearchResult{
		Status: &bleve.SearchStatus{
			Total:      1,
			Failed:     0,
			Successful: 1,
			Errors:     make(map[string]error),
		},
		Total:    collector.Total(),
		MaxScore: collector.MaxScore(),
		Took:     time.Since(searchStart),
		Facets:   collector.FacetResults(),
	}, nil
}

// Close closes the index.
func (i *Index) Close() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.open = false
	return i.i.Close()
}
