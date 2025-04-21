package flow

import (
	"context"
)

// Sinker represents a destination for a [Flow]'s output. It provides a way to consume
// and process values from a [Stream].
type Sinker interface {
	// Collect processes items from the provided [Stream] within the given context.
	// The implementation should respect context cancellation.
	Collect(ctx context.Context, stream Stream)
}

// Collect executes the [Flow] by sending its output to the provided [Sinker]. It handles error
// propagation and context cancellation while processing the stream.
//
// The method will stop processing when either:
//   - The flow completes successfully
//   - The context is cancelled
//   - An error occurs during processing
func (fn Flow) Collect(ctx context.Context, sink Sinker) error {
	var (
		src     = fn()
		fe      = FlowError{}
		flowctx = setFlowErrorContext(ctx, &fe)
		stream  = src.Stream(flowctx)
	)

	sink.Collect(flowctx, stream)
	if fe.hasErrors {
		return &fe
	}
	return ctx.Err()
}

// SinkerFunc is a function type that implements the [Sinker] interface, allowing
// simple functions to be used as Sinkers.
type SinkerFunc func(context.Context, Stream)

// Collect implements the [Sinker] interface for [SinkerFunc].
func (fn SinkerFunc) Collect(ctx context.Context, in Stream) { fn(ctx, in) }

// SliceSink collects items from a [Stream] into a slice. It provides a type-safe way
// to accumulate items of type T into a slice while respecting context cancellation.
type SliceSink[T any] struct {
	arr []T // The underlying slice that stores collected items
}

// NewSliceSink creates a new [SliceSink] with the provided initial slice.
// If nil is provided, a new empty slice will be created.
func NewSliceSink[T any](s []T) *SliceSink[T] { return &SliceSink[T]{arr: s} }

// Collect is a convenience function that collects all items from a [Source] into a slice.
func Collect[T any](ctx context.Context, src Source) ([]T, error) {
	var (
		sink = NewSliceSink(make([]T, 0))
		err  = NewFromSource(src).Collect(ctx, sink)
	)
	return sink.Items(), err
}

// Items returns the underlying slice containing collected items.
func (s SliceSink[T]) Items() []T { return s.arr }

// Collect implements the [Sinker] interface. It collects items from the stream into
// the underlying slice, respecting context cancellation.
func (s *SliceSink[T]) Collect(ctx context.Context, in Stream) {
	for item := range in {
		select {
		case <-ctx.Done():
			return
		default:
			s.arr = append(s.arr, item.(T))
		}
	}
}

// Discard creates a [Sinker] that consumes and discards all items from a [Stream].
// It respects context cancellation while efficiently draining the stream without
// allocating memory for the items.
//
// The function is useful for:
//   - Draining streams when values are not needed
//   - Testing flow processing without storing results
//   - Completing flow processing when only side effects matter
func Discard() Sinker {
	return SinkerFunc(func(ctx context.Context, s Stream) {
		for range s {
			select {
			case <-ctx.Done():
				return
			default:
				// do nothing
			}
		}
	})
}

// ChannelSink sends items from a [Stream] to a typed channel. It provides type-safe
// forwarding of items while respecting context cancellation.
type ChannelSink[T any] chan<- T

// NewChannelSink creates a new [ChannelSink] from the provided channel.
func NewChannelSink[T any](ch chan<- T) ChannelSink[T] { return ChannelSink[T](ch) }

// Collect implements the [Sinker] interface, sending items from the [Stream] to the
// underlying channel until there are no more items present or the provided
// context is cancelled.
func (c ChannelSink[T]) Collect(ctx context.Context, in Stream) {
	defer close(c)
	for item := range in {
		select {
		case <-ctx.Done():
			return
		case c <- item.(T):
		}
	}
}

// FanOutSink distributes items from a [Stream] into multiple flows based on a key function.
// It allows for dynamic partitioning of data streams based on item characteristics.
type FanOutSink[T any] struct {
	// Flows maps keys to flow transformation functions. Each key represents a partition
	// and its associated function defines how items in that partition should be processed.
	Flows map[string]func(Source) Flow

	// Key determines the partition key for each item. It receives the context
	// and the item, returning a string key that determines which flow will process the item.
	Key func(context.Context, T) string

	// sources contains the resulting Sources after partitioning, one for each unique key.
	sources map[string]Source
}

// Sources returns the list of [Source] partitions created during processing.
func (f FanOutSink[T]) Sources() []Source {
	items := make([]Source, 0, len(f.sources))
	for _, v := range f.sources {
		items = append(items, v)
	}
	return items
}

// Source returns the [Source] associated with the given key after partitioning.
// If no Source exists for the key, an empty stream source is returned.
func (f FanOutSink[T]) Source(key string) Source {
	src, ok := f.sources[key]
	if !ok {
		return Empty()
	}
	return src
}

// In implements the [Sinker] interface. It partitions incoming items based on the key function
// and creates a separate [Source] for each partition.
func (f *FanOutSink[T]) Collect(ctx context.Context, in Stream) {
	maps := map[string][]T{}
	for item := range in {
		if ctx.Err() != nil {
			return
		}
		value := item.(T)
		key := f.Key(ctx, value)
		maps[key] = append(maps[key], value)
	}

	f.sources = map[string]Source{}
	for k, v := range maps {
		f.sources[k] = NewFromItems(v...)
	}
}
