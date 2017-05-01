package matcher

import (
	"sync"

	"github.com/blevesearch/bleve/search/query"
)

type task struct {
	Id    string
	Query query.Query
}

type ParallelMatcher struct {
	factory  Factory
	matchers []Matcher

	taskCh chan task

	wg sync.WaitGroup
}

func ParallelMatcherFactory(f Factory, n int) Factory {
	return func(doc map[string]interface{}) (Matcher, error) {
		m := &ParallelMatcher{
			factory:  f,
			matchers: make([]Matcher, n),
			taskCh:   make(chan task, 1024),
		}

		for i := 0; i < n; i++ {
			matcher, err := f(doc)
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
