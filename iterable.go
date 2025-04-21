package flow

import (
	"context"
	"sync"
)

// Iterable represents a sequence of values that can be traversed. It provides methods
// for checking if more values exist, retrieving the next value, and cleaning up resources.
//
// Example:
//
//	type DBIterator struct {
//		rows    *sql.Rows
//		closed  bool
//	}
//
//	func NewDBIterator(db *sql.DB, query string) (*DBIterator, error) {
//		rows, err := db.Query(query)
//		if err != nil {
//				return nil, err
//		}
//		return &DBIterator{rows: rows}, nil
//	}
//
//	func (dbi *DBIterator) HasNext(ctx context.Context) bool {
//		if dbi.closed {
//				return false
//		}
//		return dbi.rows.Next()
//	}
//
//	func (dbi *DBIterator) Next(ctx context.Context) (any, error) {
//		var data any
//		err := dbi.rows.Scan(&data)
//		return data, err
//	}
//
//	func (dbi *DBIterator) Close(ctx context.Context) {
//		if !dbi.closed {
//				dbi.rows.Close()
//				dbi.closed = true
//		}
//	}
type Iterable interface {
	// HasNext checks if there are more items available in the sequence.
	// The context parameter allows for cancellation of the check operation.
	HasNext(context.Context) bool

	// Next retrieves the next item in the sequence.
	// Returns the next item and any error that occurred during retrieval.
	// The context parameter allows for cancellation of the retrieval operation.
	Next(context.Context) (any, error)

	// Close performs cleanup of any resources used by the Iterable.
	// The context parameter allows for cancellation of the cleanup operation.
	Close(context.Context)
}

// ConcurrentIterable wraps an Iterable to provide thread-safe access to its methods.
// It ensures that HasNext, Next, and Close operations are mutually exclusive by using
// a mutex lock.
type concurrentIterable struct {
	mu sync.Mutex
	Iterable
}

func (ci *concurrentIterable) HasNext(ctx context.Context) bool {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.Iterable.HasNext(ctx)
}

func (ci *concurrentIterable) Next(ctx context.Context) (any, error) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.Iterable.Next(ctx)
}

func (ci *concurrentIterable) Close(ctx context.Context) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.Iterable.Close(ctx)
}

// NewFromIterable creates a [Flow] from an [Iterable] source. It handles the conversion
// of the iteration-based interface to a stream-based flow.
func NewFromIterable(it Iterable) Flow {
	return NewFromSourceFunc(func(ctx context.Context) Stream {
		return func(yield func(any) bool) {
			it = &concurrentIterable{Iterable: it}
			defer it.Close(ctx)
			for it.HasNext(ctx) {
				if ctx.Err() != nil {
					return
				}
				item, err := it.Next(ctx)
				if err != nil {
					SetError(ctx, err)
					continue
				}
				if !yield(item) {
					return
				}
			}
		}
	})
}
