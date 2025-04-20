# flow

[![Test](https://github.com/nisimpson/flow/actions/workflows/test.yml/badge.svg)](https://github.com/nisimpson/flow/actions/workflows/test.yml)
[![GoDoc](https://godoc.org/github.com/nisimpson/flow?status.svg)](http://godoc.org/github.com/nisimpson/flow)
[![Release](https://img.shields.io/github/release/nisimpson/flow.svg)](https://github.com/nisimpson/flow/releases)

Flow is a Go library that enables the creation of composable data pipelines using Go channels.
It provides a set of primitives for building concurrent data processing pipelines with operations
like map, filter, reduce, fan-out, and fan-in.

## Key Features

- Type-safe pipeline operations using Go generics
- Concurrent processing with Go channels
- Composable pipeline components
- Built-in support for common operations:
  - Map: Transform data
  - Filter: Include/exclude data
  - Reduce: Aggregate data
  - Fan-out: Split data streams
  - Fan-in: Combine data streams

## Installation

```bash
go get github.com/nisimpson/flow
```

## Examples

### Basic Pipeline with Map Operation

```go
import (
    "github.com/nisimpson/flow/pipeline"
)

// Create a pipeline that doubles numbers
source := pipeline.FromSlice(1, 2, 3, 4)
double := func(in int) int { return in * 2 }
result := source.Thru(pipeline.Map(double))

// Consume the results
for num := range result.Out() {
    fmt.Println(num) // Prints: 2, 4, 6, 8
}
```

### Filtering Data

```go
// Create a pipeline that keeps only even numbers
source := pipeline.FromSlice(1, 2, 3, 4, 5, 6)
isEven := func(n int) bool { return n % 2 == 0 }
result := source.Thru(pipeline.Filter(isEven))

// Or using the more expressive KeepIf
result := source.Thru(pipeline.KeepIf(isEven))
```

### Complex Pipeline with Multiple Operations

```go
source := pipeline.
    FromSlice(1, 2, 3, 4, 5).
    Thru(
        pipeline.Map(func(n int) int { return n * 2 }),
        pipeline.Filter(func(n int) bool { return n > 5 }),
        pipeline.Map(func(n int) string { return fmt.Sprintf("Value: %d", n) }),
    )
```

### Fan-Out Example

```go
source := pipeline.FromSlice(1, 2, 3, 4)
keyFn := func(n int) string {
    if n % 2 == 0 {
        return "even"
    }
    return "odd"
}

generators := map[string]pipeline.DemuxPipelineFunction{
    "even": func(s flow.Source) pipeline.Flow {
        // process even numbers separately
        return pipeline.From(s)
    },
    "odd":  func(s flow.Source) pipeline.Flow {
        // process odd numbers separately
        return pipeline.From(s)
    },
}

fanout := pipeline.Demux(keyFn, generators)
source.To(fanout)

// return sources for further processing, such as fan-in
source = pipeline.Mux(fanout.Sources()...)
```

### Fan-In Example

```go
// Create two sources
source1 := pipeline.FromSlice(1, 2, 3)
source2 := pipeline.FromSlice(4, 5, 6)

// Combine the sources into a single pipeline
combined := pipeline.Mux(source1, source2)
```

### Reduce Example

```go
// Sum all numbers in a pipeline
sum := func(acc, item int) int { return acc + item }

// The final values will be [1, 3, 6, 10, 15]
pipeline.
    FromSlice(1, 2, 3, 4, 5).
    Thru(pipeline.Reduce(sum)).
    To(pipeline.ToSlice())
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
