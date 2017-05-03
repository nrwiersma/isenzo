package presearchers

import (
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/search/query"
)

type TermPresearcher struct {
}

// BuildQuery builds a query.Query from a document.
func (p *TermPresearcher) BuildQuery(doc map[string]interface{}) query.Query {
	return query.NewMatchAllQuery()
}

// IndexQuery creates a document.Document from a query.Query.
func (p *TermPresearcher) IndexQuery(id string, query query.Query) *document.Document {
	doc := document.NewDocument(id)

	return doc
}
