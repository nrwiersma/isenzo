package util

// Pool represents an object pool.
type Pool struct {
	pool chan interface{}

	New func() interface{}
}

// NewPool create a new Pool instance.
func NewPool(max int) *Pool {
	return &Pool{
		pool: make(chan interface{}, max),
	}
}

// Get retrieves an item from the pool, otherwise it creates a new item.
func (p *Pool) Get() interface{} {
	var item interface{}

	select {
	case item = <-p.pool:
	default:
		if p.New != nil {
			item = p.New()
		}
	}
	return item
}

// Put adds the item back into the pool.
func (p *Pool) Put(item interface{}) {
	select {
	case p.pool <- item:
	default:
		// let it go, let it go...
	}
}
