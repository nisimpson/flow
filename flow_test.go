package flow_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nisimpson/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

var (
	stringify = func(_ context.Context, i int) (string, error) { return strconv.Itoa(i), nil }
	double    = func(_ context.Context, i int) (int, error) { return i * 2, nil }
	triple    = func(_ context.Context, i int) (int, error) { return i * 3, nil }
	increment = func(_ context.Context, acc, i int) (int, error) { return acc + i, nil }
	throwsErr = func(context.Context, int) (string, error) { return "", assert.AnError }
	halves    = func(_ context.Context, i int) ([]int, error) { return []int{i / 2, i / 2}, nil }
	keepOdds  = func(_ context.Context, i int) bool { return i%2 != 0 }
)

// FibonacciIterator implements Iterable to generate a sequence of Fibonacci numbers.
type FibonacciIterator struct {
	prev    int
	current int
	max     int
	closed  bool
}

// NewFibonacciIterator creates a new FibonacciIterator that generates Fibonacci numbers
// up to but not including max.
func NewFibonacciIterator(max int) *FibonacciIterator {
	return &FibonacciIterator{
		prev:    0,
		current: 1,
		max:     max,
		closed:  false,
	}
}

// HasNext implements Iterable.HasNext by checking if there are more Fibonacci numbers
// to generate within the max limit.
func (fi *FibonacciIterator) HasNext(ctx context.Context) bool {
	if fi.closed {
		return false
	}
	return fi.current < fi.max
}

// Next implements Iterable.Next by returning the next Fibonacci number in the sequence.
func (fi *FibonacciIterator) Next(ctx context.Context) (any, error) {
	if fi.closed {
		return nil, fmt.Errorf("iterator is closed")
	}

	if fi.current >= fi.max {
		return nil, fmt.Errorf("no more Fibonacci numbers")
	}

	next := fi.prev + fi.current
	value := fi.current
	fi.prev = fi.current
	fi.current = next

	return value, nil
}

// Close implements Iterable.Close by marking the iterator as closed.
func (fi *FibonacciIterator) Close(ctx context.Context) {
	fi.closed = true
}

func TestFromClock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// blocks until timeout
	err := flow.NewFromTicker(time.Millisecond).Collect(ctx, flow.Discard())
	require.ErrorContains(t, err, "context deadline exceeded")
}

func TestFromRange(t *testing.T) {
	got, err := flow.Collect[int](context.TODO(), flow.NewFromRange(0, 5, 1))
	require.NoError(t, err)
	require.ElementsMatch(t, []int{0, 1, 2, 3, 4}, got)
}

func TestFromChannel(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)

	got, err := flow.Collect[int](context.TODO(), flow.NewFromChannel(ch))
	require.NoError(t, err)
	require.ElementsMatch(t, []int{1, 2, 3}, got)
}

func TestMerge(t *testing.T) {
	f1 := flow.NewFromItems(1, 2, 3)
	f2 := flow.NewFromItems(4, 5, 6)
	f3 := flow.Merge(f1, f2)

	got, err := flow.Collect[int](context.TODO(), f3)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{1, 2, 3, 4, 5, 6}, got)
}

func TestFlowClone(t *testing.T) {
	src1 := flow.NewFromItems(1, 2, 3)
	src2 := src1.Clone()

	src1 = src1.Transform(flow.Map(double))
	ctx := context.TODO()
	got1, err1 := flow.Collect[any](ctx, src1)
	require.NoError(t, err1)
	require.ElementsMatch(t, []any{2, 4, 6}, got1)

	src2 = src2.Transform(flow.Map(triple))
	got2, err2 := flow.Collect[any](ctx, src2)
	require.NoError(t, err2)
	require.ElementsMatch(t, []any{3, 6, 9}, got2)
}

func TestFlowCloneIterable(t *testing.T) {
	it := NewFibonacciIterator(10)
	src1 := flow.NewFromIterable(it)

	ctx := context.TODO()
	got1, err1 := flow.Collect[any](ctx, src1)
	require.NoError(t, err1)
	require.ElementsMatch(t, []any{1, 1, 2, 3, 5, 8}, got1)
	require.True(t, it.closed, true)

	// since the iterable is shared by both flows,
	// the second flow will not generate any values; the first
	// flow will have exhausted the iterable up to the max value.
	src2 := src1.Clone()
	got2, err2 := flow.Collect[any](ctx, src2)
	require.NoError(t, err2)
	require.ElementsMatch(t, []any{}, got2)
}

func TestTransforms(t *testing.T) {
	type testcase struct {
		name    string
		in      flow.Flow
		pipes   []flow.Transform
		want    []any
		wantErr bool
	}

	for _, tc := range []testcase{
		{
			name:    "empty",
			in:      flow.NewFromSource(flow.Empty()),
			want:    []any{},
			wantErr: false,
		},
		{
			name:    "passthrough",
			in:      flow.NewFromItems(1, 2, 3),
			pipes:   []flow.Transform{flow.Pass()},
			want:    []any{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "error",
			in:      flow.NewFromItems(1, 2, 3),
			pipes:   []flow.Transform{flow.Map(throwsErr)},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "map double and stringify",
			in:      flow.NewFromItems(1, 2, 3),
			pipes:   []flow.Transform{flow.Map(double), flow.Map(stringify)},
			want:    []any{"2", "4", "6"},
			wantErr: false,
		},
		{
			name:    "map double, take 2, and stringify",
			in:      flow.NewFromItems(1, 2, 3),
			pipes:   []flow.Transform{flow.Map(double), flow.Keep(2), flow.Map(stringify)},
			want:    []any{"2", "4"},
			wantErr: false,
		},
		{
			name:    "map double, drop 2, and stringify",
			in:      flow.NewFromItems(1, 2, 3, 4),
			pipes:   []flow.Transform{flow.Map(double), flow.Skip(2), flow.Map(stringify)},
			want:    []any{"6", "8"},
			wantErr: false,
		},
		{
			name:    "reduce and stringify",
			in:      flow.NewFromItems(1, 2, 3),
			pipes:   []flow.Transform{flow.Reduce(increment), flow.Map(stringify)},
			want:    []any{"1", "3", "6"},
			wantErr: false,
		},
		{
			name:    "unique",
			in:      flow.NewFromItems(1, 2, 2, 3),
			pipes:   []flow.Transform{flow.KeepDistinct[int]()},
			want:    []any{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "limit every 10 milliseconds",
			in:      flow.NewFromItems(1, 2, 3),
			pipes:   []flow.Transform{flow.Limit(rate.Every(10*time.Millisecond), 1)},
			want:    []any{1, 2, 3},
			wantErr: false,
		},
		{
			name:    "flat map",
			in:      flow.NewFromItems(2, 4, 6),
			pipes:   []flow.Transform{flow.FlatMap(halves)},
			want:    []any{1, 1, 2, 2, 3, 3},
			wantErr: false,
		},
		{
			name:    "flatten",
			in:      flow.NewFromItems([]int{1, 2, 3}, []int{4, 5, 6}),
			pipes:   []flow.Transform{flow.Flatten[[]int]()},
			want:    []any{1, 2, 3, 4, 5, 6},
			wantErr: false,
		},
		{
			name:    "filter",
			in:      flow.NewFromItems(1, 2, 3, 4),
			pipes:   []flow.Transform{flow.Filter(keepOdds)},
			want:    []any{1, 3},
			wantErr: false,
		},
		{
			name:  "chunk",
			in:    flow.NewFromItems(1, 2, 3, 4, 5),
			pipes: []flow.Transform{flow.Chunk[int](2)},
			want: []any{
				[]int{1, 2},
				[]int{3, 4},
				[]int{5},
			},
			wantErr: false,
		},
		{
			name: "sliding window",
			in:   flow.NewFromItems(1, 2, 3, 4),
			pipes: []flow.Transform{flow.SlidingWindow[int](func(swo *flow.SlidingWindowOptions) {
				swo.WindowSize = 2
				swo.StepSize = 1
			})},
			want: []any{
				[]int{1, 2},
				[]int{2, 3},
				[]int{3, 4},
			},
			wantErr: false,
		},
		{
			name: "sliding window (emit remaining window)",
			in:   flow.NewFromItems(1, 2, 3),
			pipes: []flow.Transform{flow.SlidingWindow[int](func(swo *flow.SlidingWindowOptions) {
				swo.WindowSize = 2
				swo.StepSize = 1
			})},
			want: []any{
				[]int{1, 2},
				[]int{2, 3},
			},
			wantErr: false,
		},
		{
			name: "double in parallel, keep if greater than equal to 8",
			in:   flow.NewFromItems(1, 2, 3, 4),
			pipes: []flow.Transform{
				flow.ParallelMap(2, double),
				flow.KeepIf(func(_ context.Context, i int) bool { return i >= 8 }),
			},
			want:    []any{8},
			wantErr: false,
		},
		{
			name: "skip if number is even",
			in:   flow.NewFromItems(1, 2, 3, 4),
			pipes: []flow.Transform{
				flow.OmitIf(func(_ context.Context, i int) bool { return i%2 == 0 }),
			},
			want:    []any{1, 3},
			wantErr: false,
		},
		{
			name: "last",
			in:   flow.NewFromItems(1, 2, 3, 4),
			pipes: []flow.Transform{
				flow.KeepLast(),
			},
			want:    []any{4},
			wantErr: false,
		},
		{
			name: "first",
			in:   flow.NewFromItems(1, 2, 3, 4),
			pipes: []flow.Transform{
				flow.KeepFirst(),
			},
			want:    []any{1},
			wantErr: false,
		},
		{
			name:    "empty fibonnaci",
			in:      flow.NewFromIterable(NewFibonacciIterator(1)),
			want:    []any{},
			wantErr: false,
		},
		{
			name:    "single fibonnaci",
			in:      flow.NewFromIterable(NewFibonacciIterator(2)),
			want:    []any{1, 1},
			wantErr: false,
		},
		{
			name:    "many fibonnaci",
			in:      flow.NewFromIterable(NewFibonacciIterator(10)),
			want:    []any{1, 1, 2, 3, 5, 8},
			wantErr: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var (
				src      = tc.in.Transform(tc.pipes...)
				got, err = flow.Collect[any](context.TODO(), src)
			)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestSinks(t *testing.T) {
	t.Run("discard", func(t *testing.T) {
		err := flow.NewFromItems(1, 2, 3).Collect(context.TODO(), flow.Discard())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = flow.NewFromItems(1, 2, 3).Collect(ctx, flow.Discard())
		require.Error(t, err)
	})

	t.Run("channel sink", func(t *testing.T) {
		ch := make(chan int)
		sink := flow.NewChannelSink(ch)
		go func() {
			for range ch {
			}
		}()

		// blocks until ch is consumed
		err := flow.NewFromItems(1, 2, 3).Collect(context.TODO(), sink)
		require.NoError(t, err)
	})

	t.Run("fan out sink", func(t *testing.T) {
		sink := flow.FanOutSink[int]{
			Flows: map[string]func(flow.Source) flow.Flow{
				"odd": func(s flow.Source) flow.Flow {
					return flow.NewFromSource(s).Transform(flow.Map(double))
				},
				"even": func(s flow.Source) flow.Flow {
					return flow.NewFromSource(s).Transform(flow.Map(triple))
				},
			},
			Key: func(ctx context.Context, i int) string {
				if i%2 == 0 {
					return "even"
				}
				return "odd"
			},
		}
		err := flow.NewFromItems(1, 2, 3, 4).Collect(context.TODO(), &sink)
		require.NoError(t, err)
		require.Len(t, sink.Sources(), 2)
	})
}
