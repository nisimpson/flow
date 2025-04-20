package flow

import (
	"context"
	"fmt"
	"maps"

	"golang.org/x/time/rate"
)

type Pipe func(Source) Source

func Join(pipes ...Pipe) Pipe {
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

func (fn Flow) Thru(pipes ...Pipe) Flow {
	joined := Join(pipes...)

	return func() Source {
		return joined(fn())
	}
}

// Passthrough creates a new [Pipe] component that forwards items without modification.
// This can be useful for debugging or when you need to maintain the pipeline structure without processing.
func Passthrough() Pipe {
	return func(s Source) Source {
		return s
	}
}

// Map creates a new [Pipe] component that transforms items using the provided function.
// Each input item is transformed from type T to type U using function fn.
func Map[T any, U any](fn func(context.Context, T) (U, error)) Pipe {
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

// FlatMap creates a new [Pipe] component that transforms items using the provided mapping function.
// Each input item is transformed into a slice of output items, which are then sent individually downstream.
func FlatMap[T any, U any](fn func(context.Context, T) ([]U, error)) Pipe {
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

// Flatten creates a new [Pipe] component that receives a batch of items and flattens it,
// sending each item from the upstream batch individually downstream.
// It is the semantic equivalent of:
//
//	FlatMap(func(in []T) []T { return in })
//
// For example, to handle an upstream csv in indexed row order, call:
//
//	// flatten a 2-D array of strings
//	Flatten[[][]string]() // [][]string - Flatten -> []string
func Flatten[T []U, U any]() Pipe {
	return FlatMap(func(_ context.Context, in T) ([]U, error) { return in, nil })
}

func Reduce[T any](fn func(ctx context.Context, acc T, item T) (T, error)) Pipe {
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

func Take(n int) Pipe {
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

func Drop(n int) Pipe {
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

func Filter[T any](fn func(context.Context, T) bool) Pipe {
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

type UniqueOptions[T any] struct {
	// KeyFunction is a function that generates a string key for a given element.
	// This key is used to determine uniqueness. If two elements generate the
	// same key, they are considered duplicates.
	KeyFunction func(T) string
}

// Unique creates a new [Pipe] that filters out duplicate elements from the stream.
// It accepts optional configuration functions to customize the pipe's behavior.
// By default, it uses the string representation of elements as the uniqueness key.
func Unique[T any](opts ...func(*UniqueOptions[T])) Pipe {
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

func Chunk[T any](n int) Pipe {
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

// SlidingWindow creates a new [Pipe] component that groups items using a sliding window.
// The window moves forward by StepSize items after each batch is emitted.
func SlidingWindow[T any](opts ...func(*SlidingWindowOptions)) Pipe {
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

func Limit(limit rate.Limit, burst int) Pipe {
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
