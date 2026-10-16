# FlareDB Capability Matrix

Condensed from the Apache Beam capability matrix supplied with FlareDB. Use it to
check a pipeline against FlareDB before generating code. This is a snapshot: the
matrix may lag the current implementation, so verify anything critical against
the FlareDB docs before publishing a pipeline as the final answer.

## Supported

- `ParDo`
- `GroupByKey`, `CombinePerKey`, `CombineGlobally`, `Flatten`
- Bounded sources, including text-style sources run in the SDK harness
- Bounded Splittable DoFns (restriction pairing, splitting, sizing, truncation,
  processing, and residual handling)
- SDF-initiated checkpointing and residual-root processing
- Global, fixed, and sliding windows
- Allowed lateness
- Configurable, event-time, processing-time, count, and composite triggers
- Discarding and accumulating panes
- `BagState`, bag-backed `ValueState`, event-time timers, processing-time timers

## Not supported

- Unbounded sources
- SDF side inputs
- Runner-initiated dynamic splitting
- Session windows, custom windows, custom merging windows, custom timestamp
  combiners
- `SetState`, `MapState`, ordered-list state
- Metrics collection/reporting
- Bundle finalization
- Drain and checkpoint/savepoint APIs
- Explicit key-ordered delivery guarantees

## Guidance

1. Choose transforms and I/O that the matrix lists as supported.
2. Prefer bounded sources and bounded SDFs for input; do not add an
   `beam.Create` + read-file workaround because of an assumed SDF limitation.
3. If a required capability is unsupported, say so and adjust the plan instead
   of generating a pipeline that will fail.
