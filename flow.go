/*
Package flow provides primitives for building flow-based processing pipelines. It implements
a pull-based streaming model that enables efficient data processing through composable
components.

# Core Concepts

  - A [Stream] represents a sequence of values that can be iterated over using a pull-based
    model. Consumers request values through a yield function, and the stream continues
    producing values until either the source is exhausted or the consumer stops requesting
    values.

  - A [Source] is the fundamental building block for creating data pipelines. It provides
    a single method Out that returns a Stream of values. Sources can be created from
    various inputs including slices, channels, iterables, and time-based events.

  - A [Flow] represents a factory function that creates a Source. Flows provide a way to
    create reusable pipeline components that can be composed together. They support
    operations like merging multiple sources and cloning streams.

# Key Features

  - Context-aware processing with cancellation support
  - Built-in error handling and propagation
  - Type-safe data processing
  - Composable pipeline components
  - Support for both synchronous and asynchronous processing

# Common Source Constructors

  - NewFromItems: Creates a Flow from a variadic list of items
  - NewFromChannel: Creates a Flow from a channel
  - NewFromIterable: Creates a Flow from an Iterable source
  - NewFromTicker: Creates a Flow that emits timestamps at specified intervals
  - NewFromRange: Creates a Flow that emits a sequence of integers
  - Merge: Combines multiple sources into a single Flow

# Example Usage

Creating a simple stream:

	stream := func(yield func(any) bool) {
	    for i := 0; i < 5; i++ {
	        if !yield(i) {
	            return // Stop if consumer requests
	        }
	    }
	}

Creating and using a Flow:

	// Create a Flow that emits numbers 1 through 5
	flow := NewFromSourceFunc(func(ctx context.Context) Stream {
	    return func(yield func(any) bool) {
	        for i := 1; i <= 5; i++ {
	            if ctx.Err() != nil {
	                return
	            }
	            if !yield(i) {
	                return
	            }
	        }
	    }
	})

	// Use the flow
	ctx := context.Background()
	for item := range flow.Stream(ctx) {
	    fmt.Println(item)
	}

# Error Handling

The package includes built-in error handling through context. Errors can be set using
the SetError function and will be propagated through the pipeline:

	if err != nil {
	    SetError(ctx, err)
	    continue
	}

# Thread Safety

All Flow operations are designed to be thread-safe when used with proper context
cancellation. Multiple goroutines can safely consume from the same Flow using
Clone() to create independent copies of the stream.
*/
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
type Stream iter.Seq[any]

// Stream implements [Source], returning itself while ignoring context.
func (s Stream) Stream(ctx context.Context) Stream { return s }

// Empty creates a [Stream] that produces no values. This can be useful as a default or placeholder
// source in situations where a no-op source is needed.
//
// Example:
//
//	// Use Empty as a fallback flow
//	var flow Source
//	if someCondition {
//	    flow = NewFromItems(1, 2, 3)
//	} else {
//	    flow = Empty()
//	}
//
//	// The empty flow will produce no values
//	ctx := context.Background()
//	for item := range flow.Stream(ctx) {
//	    // This loop will not execute for Empty flow
//	}
func Empty() Stream {
	return func(func(any) bool) {
		// No-op
	}
}

// Source defines an interface for producing streams of values. A Source is the fundamental
// building block for creating data pipelines. It provides a single method Out that returns
// a [Stream] of values.
type Source interface {
	// Stream returns a [Stream] that produces values from this [Source]. The returned Stream
	// should continue producing values until either:
	//   - The source is exhausted
	//   - The provided context is cancelled
	//   - The consumer stops requesting values
	Stream(context.Context) Stream
}

// SourceFunc is a function type that implements the [Source] interface. It allows regular
// functions to be used as Sources by implementing the [Source.Stream] method.
type SourceFunc func(context.Context) Stream

// Stream implements the [Source] interface for [SourceFunc].
func (fn SourceFunc) Stream(ctx context.Context) Stream { return fn(ctx) }

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
//	for item := range flow.Stream(ctx) {
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
//	for item := range flow.Stream(ctx) {
//	    fmt.Println(item)
//	}
func NewFromSourceFunc(fn func(context.Context) Stream) Flow {
	return func() Source {
		return SourceFunc(fn)
	}
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
//	for item := range flow.Stream(ctx) {
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
//	for item := range flow.Stream(ctx) {
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
				for item := range src.Stream(ctx) {
					if !yield(item) {
						return
					}
				}
			}
		}
	})
}

// Stream implements [Source], returning the generated [Stream] from the [Flow] using
// the provided context.
func (fn Flow) Stream(ctx context.Context) Stream {
	return fn().Stream(ctx)
}

// Clone creates a new [Flow] that produces the same values as the original.
func (fn Flow) Clone() Flow {
	return NewFromSource(fn)
}
