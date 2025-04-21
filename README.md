# flow

[![Test](https://github.com/nisimpson/flow/actions/workflows/test.yml/badge.svg)](https://github.com/nisimpson/flow/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/nisimpson/flow?status.svg)](http://godoc.org/github.com/nisimpson/flow)
[![Release](https://img.shields.io/github/release/nisimpson/flow.svg)](https://github.com/nisimpson/flow/releases)

Flow is a Go library that provides primitives for building flow-based processing pipelines. It implements a pull-based streaming model that enables efficient data processing through composable components. The library allows you to create, transform, and consume data streams with built-in support for error handling, context cancellation, and type safety using Go generics.

## Key Features

- **Pull-based streaming model** for efficient data processing
- **Context-aware processing** with cancellation support
- **Built-in error handling and propagation**
- **Type-safe data processing** using Go generics
- **Composable pipeline components**
- **Support for both synchronous and asynchronous processing**
- **Rich set of source constructors**:
  - Create flows from slices, channels, iterables, time-based events, and numeric ranges
  - Merge multiple sources into a single flow
  - Clone streams for parallel processing
- **Comprehensive transformation operations**:
  - Map: Transform data items
  - Filter/KeepIf/OmitIf: Include/exclude data based on predicates
  - Reduce: Aggregate data
  - FlatMap/Flatten: Expand data streams
  - Chunk: Group items into fixed-size chunks
  - SlidingWindow: Create overlapping windows of data
  - KeepDistinct: Remove duplicates
  - Limit: Apply rate limiting
  - Keep/Omit/KeepFirst/KeepLast: Control which items are processed
- **Flexible data sinks**:
  - Collect results into slices
  - Send data to channels
  - Fan-out to distribute items to multiple flows
  - Discard items when only side effects matter

## Installation

```bash
go get github.com/nisimpson/flow
```

## Examples

### Basic Flow Creation

```go
package main

import (
    "context"
    "fmt"

    "github.com/nisimpson/flow"
)

func main() {
    // Create a flow from a slice of integers
    f := flow.NewFromItems(1, 2, 3, 4, 5)

    // Create a context
    ctx := context.Background()

    // Consume the flow and print each item
    for item := range f.Stream(ctx) {
        fmt.Println(item)
    }
}
```

### Transforming Data with Map, Filter, and Reduce

```go
package main

import (
    "context"
    "fmt"
    "strconv"

    "github.com/nisimpson/flow"
)

func main() {
    ctx := context.Background()

    // Create a flow and apply transformations
    f := flow.NewFromItems(1, 2, 3, 4, 5).Transform(
        // Double each number -> [2, 4, 6, 8, 10]
        flow.Map(func(ctx context.Context, n int) (int, error) {
            return n * 2, nil
        }),
        // Keep only numbers greater than 5 -> [6, 8, 10]
        flow.KeepIf(func(ctx context.Context, n int) bool {
            return n > 5
        }),
        // Reduce to a rolling sum of each value -> [6, 14, 24]
        flow.Reduce(func(ctx context.Context, acc, item int) (int, error) {
            return acc + item, nil
        }),
        // Keep the last value -> [24]
        flow.KeepLast(),
        // Convert to string -> ["24"]
        flow.Map(func(ctx context.Context, n int) (string, error) {
            return strconv.Itoa(n), nil
        }),
    )

    // Collect results into a slice
    results, err := flow.Collect[string](ctx, f)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    fmt.Println(results) // ["24"]
}
```

### Working with Channels

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/nisimpson/flow"
)

func main() {
    // Create a channel and send values to it
    ch := make(chan int)
    go func() {
        defer close(ch)
        for i := 1; i <= 5; i++ {
            ch <- i
            time.Sleep(100 * time.Millisecond)
        }
    }()

    // Create a flow from the channel
    f := flow.NewFromChannel(ch)

    // Create a context with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
    defer cancel()

    // Process the flow
    for item := range f.Stream(ctx) {
        fmt.Printf("Received: %d\n", item)
    }
}
```

### Chunking and Windowing

```go
package main

import (
    "context"
    "fmt"

    "github.com/nisimpson/flow"
)

func main() {
    ctx := context.Background()

    // Create a flow with numbers 1 through 10
    f := flow.NewFromRange(1, 11, 1)

    // Group into chunks of 3
    chunks, err := flow.Collect[[]int](ctx, f.Transform(flow.Chunk[int](3)))
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    fmt.Println("Chunks:", chunks) // [[1 2 3], [4 5 6], [7 8 9], [10]]

    // Create sliding windows of size 3, stepping by 1
    windows, err := flow.Collect[[]int](ctx, flow.NewFromRange(1, 8, 1).Transform(
        flow.SlidingWindow[int](func(opts *flow.SlidingWindowOptions) {
            opts.WindowSize = 3
            opts.StepSize = 1
        }),
    ))
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    fmt.Println("Windows:", windows) // [[1 2 3], [2 3 4], [3 4 5], [4 5 6], [5 6 7]]
}
```

### Error Handling

```go
package main

import (
    "context"
    "errors"
    "fmt"

    "github.com/nisimpson/flow"
)

func main() {
    ctx := context.Background()

    // Create a flow with a transformation that might fail
    f := flow.NewFromItems(1, 2, 0, 4, 5).Transform(
        flow.Map(func(ctx context.Context, n int) (int, error) {
            if n == 0 {
                return 0, errors.New("division by zero")
            }
            return 10 / n, nil
        }),
    )

    // Collect results and handle errors
    results, err := flow.Collect[int](ctx, f)
    if err != nil {
        fmt.Printf("Error occurred: %v\n", err)
    } else {
        fmt.Println("Results:", results)
    }
}
```

### Fan-Out and Fan-In

```go
package main

import (
    "context"
    "fmt"

    "github.com/nisimpson/flow"
)

func main() {
    ctx := context.Background()

    // Create a flow with numbers
    f := flow.NewFromItems(1, 2, 3, 4, 5, 6)

    // Create a fan-out sink to separate odd and even numbers
    sink := &flow.FanOutSink[int]{
        Key: func(ctx context.Context, n int) string {
            if n%2 == 0 {
                return "even"
            }
            return "odd"
        },
    }

    // Execute the flow with the fan-out sink
    err := f.Collect(ctx, sink)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    // Process each source separately
    for _, src := range sink.Sources() {
        items, _ := flow.Collect[int](ctx, src)
        fmt.Println("Source items:", items)
    }

    // Fan-in: Merge multiple flows
    f1 := flow.NewFromItems(1, 3, 5)
    f2 := flow.NewFromItems(2, 4, 6)
    merged := flow.Merge(f1, f2)

    mergedItems, _ := flow.Collect[int](ctx, merged)
    fmt.Println("Merged items:", mergedItems)
}
```

### Rate Limiting

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/nisimpson/flow"
    "golang.org/x/time/rate"
)

func main() {
    ctx := context.Background()

    // Create a flow with rate limiting (2 items per second)
    f := flow.NewFromItems(1, 2, 3, 4, 5).Transform(
        flow.Limit(rate.Every(time.Millisecond), 1),
    )

    start := time.Now()

    // Process the flow
    for item := range f.Stream(ctx) {
        fmt.Printf("Processed %d after %v\n", item, time.Since(start))
    }
}
```

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/nisimpson/flow"
)

// BatchDataIterator demonstrates fetching data in batches
type BatchDataIterator struct {
    currentBatch int
    batchSize   int
    maxBatches  int
    closed      bool
}

func (bi *BatchDataIterator) HasNext(ctx context.Context) bool {
    if bi.closed {
        return false
    }
    return bi.currentBatch < bi.maxBatches
}

func (bi *BatchDataIterator) Next(ctx context.Context) (any, error) {
    if bi.closed {
        return nil, fmt.Errorf("iterator is closed")
    }
    if bi.currentBatch >= bi.maxBatches {
        return nil, fmt.Errorf("no more batches")
    }

    // Simulate fetching a batch of data
    time.Sleep(100 * time.Millisecond)
    batch := fmt.Sprintf("Batch %d", bi.currentBatch)
    bi.currentBatch++
    return batch, nil
}

func (bi *BatchDataIterator) Close(ctx context.Context) {
    bi.closed = true
}

func main() {
    iterator := &BatchDataIterator{
        batchSize:   10,
        maxBatches:  5,
    }

    // Create a flow and process batches
    f := flow.NewFromIterable(iterator).
        Transform(
            flow.Map(func(ctx context.Context, batch string) (string, error) {
            return fmt.Sprintf("Processed %s", batch), nil
            }),
        )

    // Process results
    ctx := context.Background()
    for item := range f.Stream(ctx) {
        fmt.Println(item)
    }
}
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
