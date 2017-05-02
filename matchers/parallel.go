package matchers

import (
	"sync"

	"github.com/blevesearch/bleve/search/query"
)

type task struct {
	Id    string
	Query query.Query
}

// ParallelMatcherFactory represents a factory for ParallelMatcher.
type ParallelMatcherFactory struct {
	factory Factory
	threads int
}

// NewParallelMatcherFactory creates a new ParallelMatcherFactory.
func NewParallelMatcherFactory(factory Factory, threads int) Factory {
	return &ParallelMatcherFactory{
		factory: factory,
		threads: threads,
	}
}

// New creates a new query matcher.
func (f ParallelMatcherFactory) New(doc interface{}) (Matcher, error) {
	m := &ParallelMatcher{
		factory:  f.factory,
		matchers: make([]Matcher, f.threads),
		taskCh:   make(chan task, 1024),
	}

	for i := 0; i < f.threads; i++ {
		matcher, err := f.factory.New(doc)
		if err != nil {
			return nil, err
		}
		m.matchers[i] = matcher

		m.wg.Add(1)
		go func() {
			defer m.wg.Done()

			for t := range m.taskCh {
				matcher.Match(t.Id, t.Query)
			}
		}()
	}

	return m, nil
}

// Map maps a document for the matcher.
func (f ParallelMatcherFactory) Map(doc interface{}) interface{} {
	return f.factory.Map(doc)
}

// ParallelMatcher represents a threaded matcher.
type ParallelMatcher struct {
	factory  Factory
	matchers []Matcher

	taskCh chan task

	wg sync.WaitGroup
}

// Match matches a query with the matcher.
func (m *ParallelMatcher) Match(id string, q query.Query) {
	m.taskCh <- task{
		Id:    id,
		Query: q,
	}
}

// Finish closes the matcher and returns the match results.
func (m *ParallelMatcher) Finish() (ids []string, errs []error) {
	close(m.taskCh)
	m.wg.Wait()

	ids = make([]string, 0)
	errs = make([]error, 0)
	for _, m := range m.matchers {
		i, e := m.Finish()
		ids = append(ids, i...)
		errs = append(errs, e...)
	}

	return
}
