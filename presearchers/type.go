package presearchers

import (
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/search/query"
)

type Presearcher interface {
	// BuildQuery builds a query.Query from a document.
	BuildQuery(doc map[string]interface{}) query.Query

	// IndexQuery creates a document.Document from a query.Query.
	IndexQuery(id string, query query.Query) *document.Document
}
