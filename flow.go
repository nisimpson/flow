// Package flow provides primitives for building flow-based processing pipelines.
package flow

import (
	"context"
	"iter"
	"time"
)

// Stream represents a sequence of any values that can be iterated over. It uses
// a pull-based iteration model where consumers request values through a yield
// function. The Stream will continue producing values until either the source is
// exhausted or the yield function returns false. The underlying function of
// Stream has the following signature:
//
//	func(yield func(any) bool)
//
// Example creating a simple stream:
//
//	stream := func(yield func(any) bool) {
//	  for i := 0; i < 5; i++ {
//	    if !yield(i) {
//	      return // Stop if consumer requests
//	    }
//	  }
//	}
//
// Example consuming a stream using a range expression:
//
//	for item := range stream {
//	  fmt.Println(item)
//	  if item == 3 {
//	    break // Stop if condition is met
//	  }
//	}
type Stream = iter.Seq[any]

// Source defines an interface for producing streams of values. A Source is the fundamental
// building block for creating data pipelines. It provides a single method Out that returns
// a [Stream] of values.
type Source interface {
	Out(context.Context) Stream
}

// SourceFunc is a function type that implements the Source interface. It allows regular
// functions to be used as Sources by implementing the Out method.
type SourceFunc func(context.Context) Stream

// Out implements the [Source] interface for [SourceFunc].
func (fn SourceFunc) Out(ctx context.Context) Stream { return fn(ctx) }

// Flow represents a factory function that creates a [Source]. It provides a way to
// create reusable pipeline components that can be composed together.
type Flow func() Source

// NewFromSource creates a [Flow] from an existing [Source], allowing it to be used in
// flow-based processing pipelines.
//
// Example:
//
//	// Create a simple source
//	source := SourceFunc(func(ctx context.Context) Stream {
//	    return func(yield func(any) bool) {
//	        for i := 1; i <= 3; i++ {
//	            if ctx.Err() != nil {
//	                return
//	            }
//	            if !yield(i) {
//	                return
//	            }
//	        }
//	    }
//	})
//
//	// Create a Flow from the source
//	flow := NewFromSource(source)
//
//	// Use the flow in a pipeline
//	ctx := context.Background()
//	for item := range flow.Out(ctx) {
//	    fmt.Println(item)
//	}
func NewFromSource(s Source) Flow {
	return func() Source {
		return s
	}
}

// NewFromSourceFunc creates a [Flow] from a function that produces a [Stream]. This is a convenience
// function that wraps a stream-producing function into a flow, making it easier to create
// flows without explicitly implementing the [Source] interface.
//
// Example:
//
//	// Create a Flow that emits numbers 1 through 5
//	flow := NewFromSourceFunc(func(ctx context.Context) Stream {
//	    return func(yield func(any) bool) {
//	        for i := 1; i <= 5; i++ {
//	            // Check for context cancellation
//	            if ctx.Err() != nil {
//	                return
//	            }
//	            // Emit value, stop if consumer requests
//	            if !yield(i) {
//	                return
//	            }
//	        }
//	    }
//	})
//
//	// Use the flow
//	ctx := context.Background()
//	for item := range flow.Out(ctx) {
//	    fmt.Println(item)
//	}
func NewFromSourceFunc(fn func(context.Context) Stream) Flow {
	return func() Source {
		return SourceFunc(fn)
	}
}

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

// NewFromIterable creates a [Flow] from an [Iterable] source. It handles the conversion
// of the iteration-based interface to a stream-based flow.
func NewFromIterable(it Iterable) Flow {
	return NewFromSourceFunc(func(ctx context.Context) Stream {
		return func(yield func(any) bool) {
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

// NewFromItems creates a [Flow] from a variadic list of items. It provides a convenient way
// to create a flow from a known set of values of any type T.
//
// Example:
//
//	// Create a Flow from integers
//	flow := NewFromItems(1, 2, 3, 4, 5)
//
//	// Use the flow
//	ctx := context.Background()
//	for item := range flow.Out(ctx) {
//	    fmt.Println(item)
//	}
func NewFromItems[T any](items ...T) Flow {
	return NewFromSourceFunc(func(ctx context.Context) Stream {
		return func(yield func(any) bool) {
			for _, item := range items {
				if ctx.Err() != nil {
					return
				}
				if !yield(item) {
					return
				}
			}
		}
	})
}

// NewFromChannel creates a [Flow] from a channel. It provides a way to convert any typed
// channel for use in flow-based processing pipelines.
//
// The flow will continue reading from the channel until it is closed or the
// context is cancelled.
//
// Example:
//
//	// Create a channel and flow
//	ch := make(chan int)
//	flow := NewFromChannel(ch)
//
//	// Send values in a separate goroutine
//	go func() {
//	    defer close(ch)
//	    for i := 1; i <= 5; i++ {
//	        ch <- i
//	    }
//	}()
//
//	// Process values from the flow
//	ctx := context.Background()
//	for item := range flow.Out(ctx) {
//	    fmt.Println(item)
//	}
func NewFromChannel[T any](ch <-chan T) Flow {
	return NewFromSourceFunc(func(ctx context.Context) Stream {
		return func(yield func(any) bool) {
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-ch:
					if !ok {
						return
					}
					if !yield(item) {
						return
					}
				}
			}
		}
	})
}

// NewFromTicker creates a [Flow] that emits timestamps at specified intervals.
// It continues to emit timestamps until either the context is cancelled or
// the consumer stops accepting values.
//
// The timestamps are of type time.Time and represent the actual time when the
// interval elapsed. Note that the actual intervals may be slightly longer than
// specified due to system scheduling and processing delays.
func NewFromTicker(interval time.Duration) Flow {
	return NewFromSourceFunc(func(ctx context.Context) Stream {
		return func(yield func(any) bool) {
			for {
				select {
				case <-ctx.Done():
					return
				case ts := <-time.After(interval):
					if !yield(ts) {
						return
					}
				}
			}
		}
	})
}

// NewFromRange creates a Flow that emits a sequence of integers from start (inclusive)
// to end (exclusive), incrementing by the specified step value.
//
// The function will stop emitting values when either:
//   - The sequence reaches or exceeds the end value
//   - The context is cancelled
//   - The consumer stops accepting values
func NewFromRange(start, end, step int) Flow {
	return NewFromSourceFunc(func(ctx context.Context) Stream {
		return func(yield func(any) bool) {
			for i := start; i < end; i += step {
				if ctx.Err() != nil {
					return
				}
				if !yield(i) {
					return
				}
			}
		}
	})
}

// Merge combines multiple sources into a single [Flow]. It processes each [Source]
// sequentially, emitting all values from one source before moving to the next.
//
// The function will process sources in order until either:
//   - All sources are exhausted
//   - The context is cancelled
//   - The consumer stops accepting values
func Merge(sources ...Source) Flow {
	return NewFromSourceFunc(func(ctx context.Context) Stream {
		return func(yield func(any) bool) {
			for _, src := range sources {
				for item := range src.Out(ctx) {
					if !yield(item) {
						return
					}
				}
			}
		}
	})
}

// Out returns a [Stream] from the [Flow] using the provided context.
func (fn Flow) Out(ctx context.Context) Stream {
	return fn().Out(ctx)
}

// Clone creates a new [Flow] that produces the same values as the original.
func (fn Flow) Clone() Flow {
	return NewFromSource(fn)
}
