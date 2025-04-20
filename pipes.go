package flow

import (
	"context"
	"fmt"
	"maps"

	"golang.org/x/time/rate"
)

// Transform is a function type that transforms one [Source] into another [Source].
// It represents a processing stage in a data pipeline that can modify, filter,
// or transform the data [Stream].
type Transform func(Source) Source

// Join combines multiple transformers into a single [Transform] by composing them in indexed order.
// Each transformers's output becomes the input to the next transformer in the sequence.
//
// Join panics if no transformers are provided.
func Join(pipes ...Transform) Transform {
	if len(pipes) == 0 {
		panic("no pipes to join")
	}
	return func(s Source) Source {
		var cur Source
		for idx, p := range pipes {
			if idx == 0 {
				cur = p(s)
				continue
			}
			cur = p(cur)
		}
		return cur
	}
}

// Transform applies a series of transformations to the [Flow] by joining the provided
// transformers and creating a new Flow with the combined [Transform].
//
// Example:
//
//	f := flow.NewFromItems(1, 2, 3, 4).Transform(
//	  Map(func(ctx context.Context, n int) (int, error) { return n * 2, nil }),
//	  Filter(func(ctx context.Context, n int) bool { return n > 5 }),
//	)
func (fn Flow) Transform(pipes ...Transform) Flow {
	joined := Join(pipes...)

	return func() Source {
		return joined(fn())
	}
}

// Passthrough creates a new [Transform] component that forwards items without modification.
// This can be useful for debugging or when you need to maintain the [Flow] structure without processing.
func Passthrough() Transform {
	return func(s Source) Source {
		return s
	}
}

// Map creates a new [Transform] component that transforms items using the provided function.
// Each input item is transformed from type T to type U using function fn.
func Map[T any, U any](fn func(context.Context, T) (U, error)) Transform {
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				for item := range s.Out(ctx) {
					mapped, err := fn(ctx, item.(T))
					if err != nil {
						SetError(ctx, err)
						continue
					}
					if !yield(mapped) {
						return
					}
				}
			}
		})
	}
}

// FlatMap creates a new [Transform] component that transforms items using the provided mapping function.
// Each input item is transformed into a slice of output items, which are then sent individually downstream.
func FlatMap[T any, U any](fn func(context.Context, T) ([]U, error)) Transform {
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				for item := range s.Out(ctx) {
					items, err := fn(ctx, item.(T))
					if err != nil {
						SetError(ctx, err)
						continue
					}
					for _, item := range items {
						if !yield(item) {
							return
						}
					}
				}
			}
		})
	}
}

// Flatten creates a new [Transform] component that receives a batch of items and flattens it,
// sending each item from the upstream batch individually downstream.
// It is the semantic equivalent of:
//
//	FlatMap(func(in []T) []T { return in })
//
// For example, to handle an upstream csv in indexed row order, call:
//
//	// flatten a 2-D array of strings
//	Flatten[[][]string]() // [][]string - Flatten -> []string
func Flatten[T []U, U any]() Transform {
	return FlatMap(func(_ context.Context, in T) ([]U, error) { return in, nil })
}

// Reduce creates a new [Transform] component that combines multiple items into one using
// the provided reduction function. Each input item is combined with the accumulator
// to produce a new accumulated value.
//
// Example:
//
//	// Join strings with separator
//	flow := NewFromItems("Hello", "World", "!").
//	    Transform(
//	        Reduce(func(ctx context.Context, acc, item string) (string, error) {
//	            if acc == "" {
//	                return item, nil
//	            }
//	            return acc + " " + item, nil
//	        }),
//	    )
//
//	result, err := Collect[string](ctx, flow)
//	// Result: ["Hello", "Hello World", "Hello World !"]
func Reduce[T any](fn func(ctx context.Context, acc T, item T) (T, error)) Transform {
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				var (
					acc any
					err error
				)
				for item := range s.Out(ctx) {
					if acc == nil {
						acc = item
					} else {
						acc, err = fn(ctx, acc.(T), item.(T))
					}
					if err != nil {
						SetError(ctx, err)
						return
					}
					if !yield(acc) {
						return
					}
				}
			}
		})
	}
}

// Keep creates a new [Transform] that limits the stream to at most n items from
// the input stream. Once n items have been processed, any remaining input items
// are ignored.
//
// Example:
//
//	// Keep first 3 items
//	f := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(Keep(3))
//
//	results, err := Collect[int](ctx, f)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(results) // [1, 2, 3]
func Keep(n int) Transform {
	if n < 0 {
		panic(fmt.Sprintf("invalid take count: %d", n))
	}
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				cur := 0
				for item := range s.Out(ctx) {
					if cur >= n || !yield(item) {
						return
					}
					cur++
				}
			}
		})
	}
}

// First creates a new [Transform] that takes only the first item from the stream
// and terminates processing. It is semantically equivalent to [Keep](1).
//
// Example:
//
//	// Get first item from stream
//	f := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(First())
//
//	result, err := Collect[int](ctx, f)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [1]
func First() Transform {
	return Keep(1)
}

// Skip creates a new [Transform] that skips the first n items from the input stream
// and forwards all remaining items. If n is less than or equal to 0, no items
// are dropped.
//
// Example:
//
//	// Skip first 2 items and process the rest
//	f := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(Skip(2))
//
//	result, err := Collect[int](ctx, f)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [3, 4, 5]
func Skip(n int) Transform {
	if n <= 0 {
		return Passthrough()
	}
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				cur := 0
				for item := range s.Out(ctx) {
					if cur < n {
						cur++
						continue
					}
					if !yield(item) {
						return
					}
				}
			}
		})
	}
}

// Last creates a new [Transform] that yields only the last item from the stream.
// If the stream is empty, no items are yielded.
//
// Example:
//
//	// Get the last item from a stream of numbers
//	flow := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(Last())
//
//	result, err := Collect[int](ctx, f)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [5]
func Last() Transform {
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				var last any
				for item := range s.Out(ctx) {
					last = item
				}
				if last != nil {
					yield(last)
				}
			}
		})
	}
}

// Filter creates a new [Transform] that selectively includes items from the [Stream]
// based on the provided predicate function. Only items that cause the predicate
// to return true are included in the output stream.
//
// Example:
//
//	// Keep only even numbers
//	flow := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(Filter(func(ctx context.Context, n int) bool {
//	        return n%2 == 0
//	    }))
//
//	result, err := Collect[int](ctx, flow)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [2, 4]
func Filter[T any](fn func(context.Context, T) bool) Transform {
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				for item := range s.Out(ctx) {
					if fn(ctx, item.(T)) && !yield(item.(T)) {
						return
					}
				}
			}
		})
	}
}

// KeepIf creates a new Transform that selectively includes items from the stream
// based on the provided predicate function. It is an alias for Filter.
//
// Example:
//
//	// Keep only positive numbers
//	flow := NewFromItems(-2, -1, 0, 1, 2).
//	    Transform(KeepIf(func(ctx context.Context, n int) bool {
//	        return n > 0
//	    }))
//
//	result, err := Collect[int](ctx, flow)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [1, 2]
func KeepIf[T any](fn func(context.Context, T) bool) Transform {
	return Filter(fn)
}

// SkipIf creates a new [Transform] that selectively excludes items from the [Stream]
// based on the provided predicate function. It is the inverse of [KeepIf].
//
// Example:
//
//	// Skip negative numbers
//	flow := NewFromItems(-2, -1, 0, 1, 2).
//	    Transform(SkipIf(func(ctx context.Context, n int) bool {
//	        return n < 0
//	    }))
//
//	result, err := Collect[int](ctx, flow)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [0, 1, 2]
func SkipIf[T any](fn func(context.Context, T) bool) Transform {
	return Filter(func(ctx context.Context, t T) bool { return !fn(ctx, t) })
}

type UniqueOptions[T any] struct {
	// KeyFunction is a function that generates a string key for a given element.
	// This key is used to determine uniqueness. If two elements generate the
	// same key, they are considered duplicates.
	KeyFunction func(T) string
}

// Unique creates a new [Transform] that filters out duplicate elements from the [Stream].
// It accepts optional configuration functions to customize the [Flow]'s behavior.
// By default, it uses the string representation of elements as the uniqueness key.
//
// Example:
//
//	// Remove duplicate numbers
//	flow := NewFromItems(1, 2, 2, 3, 3, 4).
//	    Transform(Unique[int]())
//
//	result, err := Collect[int](ctx, flow)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [1, 2, 3, 4]
func Unique[T any](opts ...func(*UniqueOptions[T])) Transform {
	options := UniqueOptions[T]{KeyFunction: func(t T) string { return fmt.Sprintf("%v", t) }}
	for _, opt := range opts {
		opt(&options)
	}

	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			cache := map[string]any{}
			keyFn := options.KeyFunction
			return func(yield func(any) bool) {
				for item := range s.Out(ctx) {
					cache[keyFn(item.(T))] = item
				}
				for value := range maps.Values(cache) {
					if !yield(value) {
						return
					}
				}
			}
		})
	}
}

// Chunk creates a new [Transform] that groups items into fixed-size chunks.
// The last chunk may contain fewer items if the stream length is not
// divisible by n. Each chunk is emitted as a slice of items.
//
// Example:
//
//	// Group numbers into chunks of 2
//	flow := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(Chunk[int](2))
//
//	result, err := Collect[[]int](ctx, flow)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [[1, 2], [3, 4], [5]]
func Chunk[T any](n int) Transform {
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				buffer := make([]T, 0, n)
				for item := range s.Out(ctx) {
					buffer = append(buffer, item.(T))
					if len(buffer) == n && !yield(buffer) {
						return
					} else if len(buffer) == n {
						buffer = make([]T, 0, n)
					}
				}
				if len(buffer) > 0 && !yield(buffer) {
					return
				}
			}
		})
	}
}

type SlidingWindowOptions struct {
	WindowSize int // Sliding window size
	StepSize   int // Number of items to slide forward for each window
}

// SlidingWindow creates a new [Transform] component that groups items using a sliding window.
// The window moves forward by StepSize items after each batch is emitted.
//
// Example:
//
//	// Create windows of size 3, sliding by 1
//	flow := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(SlidingWindow[int](func(opts *SlidingWindowOptions) {
//	        opts.WindowSize = 3
//	        opts.StepSize = 1
//	    }))
//
//	result, err := Collect[[]int](ctx, flow)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result) // [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
func SlidingWindow[T any](opts ...func(*SlidingWindowOptions)) Transform {
	options := SlidingWindowOptions{
		WindowSize: 2,
		StepSize:   1,
	}
	for _, opt := range opts {
		opt(&options)
	}

	if options.StepSize <= 0 {
		options.StepSize = 1
	}
	if options.WindowSize < options.StepSize {
		options.WindowSize = options.StepSize
	}

	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				buffer := make([]T, 0, options.WindowSize)
				for item := range s.Out(ctx) {
					buffer = append(buffer, item.(T))
					if len(buffer) == options.WindowSize {
						window := make([]T, options.WindowSize)
						copy(window, buffer)
						if !yield(window) {
							return
						}
						buffer = buffer[options.StepSize:]
					}
				}
				// emit remaining windows
				for len(buffer) >= options.WindowSize {
					window := make([]T, options.WindowSize)
					copy(window, buffer)
					if !yield(window) {
						return
					}
					buffer = buffer[options.StepSize:]
				}
			}
		})
	}
}

// Limit creates a new [Transform] that applies rate limiting to the stream using
// the token bucket algorithm. Items are processed according to the specified
// rate limit and burst size.
//
// Example:
//
//	// Process items at a rate of 10 per second with max burst of 20
//	flow := NewFromItems(1, 2, 3, 4, 5).
//	    Transform(Limit(rate.Limit(10), 20))
//
//	result, err := Collect[int](ctx, flow)
//	if err != nil {
//	    log.Fatal(err)
//	}
func Limit(limit rate.Limit, burst int) Transform {
	limiter := rate.NewLimiter(limit, burst)
	return func(s Source) Source {
		return SourceFunc(func(ctx context.Context) Stream {
			return func(yield func(any) bool) {
				for item := range s.Out(ctx) {
					if err := limiter.Wait(ctx); err != nil {
						SetError(ctx, err)
						return
					}
					if !yield(item) {
						return
					}
				}
			}
		})
	}
}
