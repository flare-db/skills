---
name: beam-pipeline
description: End-to-end guide for coding agents to design, construct, test, and debug Apache Beam batch and streaming pipelines. Write Python by default and run local jobs on FlareDB through flaredb-skills; keep Java for explicitly Java work. Use when writing a new pipeline, choosing transforms or I/O, configuring windowing and triggers, or diagnosing pipeline errors.
---

# Apache Beam Pipeline Development

Apache Beam defines batch and streaming pipelines once and runs them on multiple
engines. In this workspace the default path is **Python on FlareDB**: this skill
covers *authoring* the pipeline, and
[`flaredb-skills`](../../../flaredb-skills/SKILL.md) covers *running* it. Read
the FlareDB workflow and its approval gate before submitting any job.

## 1. Language and runner defaults

- **Default SDK: Python.** No compile step, no JAR staging, and FlareDB ships the
  code to workers. Use Java only when the user asks for Java or you are porting
  an existing Java pipeline.
- **Default local runner: FlareDB.** Use it for bounded jobs that process more
  data than should be read into context and return a small result.
- **DirectRunner** is for fast unit tests and tiny checks.
- **Other runners are out of default scope.** Do not introduce them unless the
  user explicitly requests a specific runner.
- **FlareDB supports bounded sources and bounded Splittable DoFns.** Unbounded
  sources are not supported. Check
  [references/flaredb-features.md](references/flaredb-features.md) before using
  windowing, triggers, or state.

## 2. Agent workflow

1. **Clarify requirements** — batch vs streaming, Python by default, input/output
   systems, and target runner. For FlareDB jobs confirm the run plan against
   `flaredb-skills` (CLI, environment, approval gate).
2. **Define options** — argparse + `PipelineOptions` in Python. For FlareDB set
   `job_endpoint` and `job_name`. Never hard-code paths or endpoints.
3. **Design the topology** — sources → transforms → sinks, grouping, side
   inputs, dead-letter routing. See
   [references/transforms.md](references/transforms.md).
4. **Choose data model and coders** — prefer `typing.NamedTuple` in Python;
   Java `@DefaultSchema` and `TypeDescriptors` only for Java work. See
   [references/coders-and-schemas.md](references/coders-and-schemas.md).
5. **Choose I/O** — for FlareDB, prefer `ReadFromText`/`ReadAllFromText` for
   plain text input; use `beam.Create` + a file-reading `Map`/`FlatMap` only for
   custom parsing or an explicit input list. See
   [references/flaredb-runner.md](references/flaredb-runner.md) and
   [references/flaredb-features.md](references/flaredb-features.md). For
   portable connectors see [references/io-connectors.md](references/io-connectors.md).
6. **Check FlareDB capabilities** — bounded sources/SDFs are supported; use
   windowing, triggers, or state only where
   [references/flaredb-features.md](references/flaredb-features.md) lists them.
   For unbounded input, use the `beam-streaming` skill only if the user
   explicitly requests streaming.
7. **Write unit tests** — `TestPipeline` + `assert_that` under DirectRunner. See
   [../beam-debugging/references/testing-patterns.md](../beam-debugging/references/testing-patterns.md).
8. **Run** — DirectRunner for tests; FlareDB for the real bounded job after the
   `flaredb-skills` approval gate.

## 3. Python skeleton — FlareDB (default)

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from flaredb_runner.flare_runner import FlareRunner


pipeline_options = PipelineOptions(
    job_endpoint="127.0.0.1:8099",
    job_name="wordcount",
)

with beam.Pipeline(runner=FlareRunner(), options=pipeline_options) as p:
    (
        p
        | "InputPaths"  >> beam.Create(["/data/a.txt", "/data/b.txt"])
        | "ReadLines"   >> beam.io.ReadAllFromText()
        | "Tokenize"    >> beam.FlatMap(lambda line: line.lower().split())
        | "Count"       >> beam.combiners.Count.PerElement()
        | "Format"      >> beam.Map(lambda wc: f"{wc[0]}\t{wc[1]}")
    )
```

Do not submit this (or a trial) until the user approves the brief described in
`flaredb-skills`. See [references/flaredb-runner.md](references/flaredb-runner.md)
for input/output patterns and constraints.

## 4. Other runners

FlareDB is the default. If a specific non-FlareDB runner is explicitly
requested, keep the pipeline portable with argparse + `PipelineOptions` and
consult that runner's connector support in
[references/io-connectors.md](references/io-connectors.md). Do not otherwise
introduce cloud or cluster runners.

## 5. Java skeleton (only when Java is required)

The full Java WordCount lives in
[examples/WordCountBatch.java](examples/WordCountBatch.java). In short:

```java
public interface Options extends PipelineOptions {
  String getInputFile();
  void setInputFile(String value);
  String getOutputFile();
  void setOutputFile(String value);
}

Pipeline p = Pipeline.create(
    PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class));

p.apply("Read", TextIO.read().from(options.getInputFile()))
 .apply("Words", FlatMapElements.into(TypeDescriptors.strings())
     .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
 .apply("Count", Count.perElement())
 .apply("Write", TextIO.write().to(options.getOutputFile()));

p.run().waitUntilFinish();
```

Java-only details (TypeDescriptors, coders, schemas, Maven/Gradle commands) are
in [references/transforms.md](references/transforms.md),
[references/coders-and-schemas.md](references/coders-and-schemas.md), and
[references/io-connectors.md](references/io-connectors.md).

## 6. Transform selection

| Need | Python | Java | Key constraint |
|---|---|---|---|
| 1-to-1 mapping | `beam.Map(fn)` | `MapElements.into(...).via(fn)` | Java lambdas need `TypeDescriptors` |
| 1-to-N expansion | `beam.FlatMap(fn)` | `FlatMapElements.into(...).via(fn)` | Return iterable/generator |
| Filter | `beam.Filter(pred)` | `Filter.by(pred)` | Predicate returns boolean |
| Complex / stateful / multi-output | `beam.ParDo(MyDoFn())` | `ParDo.of(new MyDoFn())` | Supports lifecycle, side inputs, timers |
| Count | `beam.combiners.Count.PerElement()` | `Count.perElement()` | Inputs are elements |
| Group by key | `beam.GroupByKey()` | `GroupByKey.create()` | Needs `(k, v)` / `KV<K,V>` |
| Associative aggregation | `beam.CombinePerKey(fn)` | `Combine.perKey(fn)` | Prefer over GBK + map |
| Join by key | `beam.CoGroupByKey()` | `CoGroupByKey.create()` | Both inputs keyed |
| Merge same-type | `beam.Flatten()` | `Flatten.pCollections()` | Same element type |
| Split into N buckets | `beam.Partition(fn, N)` | `Partition.of(N, fn)` | Return index `[0, N)` |
| Enrich with lookup | `beam.Map(fn, lookup=AsDict(pc))` | `ParDo(...).withSideInputs(view)` | Side input fits in memory |
| Route bad records | `beam.ParDo + TaggedOutput` | `ParDo + TupleTag` outputs | Dead-letter pattern |

Full examples: [references/transforms.md](references/transforms.md).

## 7. I/O connector selection

| Source / sink | Python | Java | Bounded / unbounded |
|---|---|---|---|
| Text files | `beam.io.ReadFromText` / `WriteToText` | `TextIO` | Bounded |
| Avro | `beam.io.ReadFromAvro` / `WriteToAvro` | `AvroIO` | Bounded |
| Parquet | `beam.io.ReadFromParquet` / `WriteToParquet` | `ParquetIO` | Bounded |
| Kafka | `apache_beam.io.kafka.ReadFromKafka` / `apache_beam.io.kafka.WriteToKafka` | `KafkaIO` | Unbounded |
| Pub/Sub | `beam.io.ReadFromPubSub` / `WriteToPubSub` | `PubsubIO` | Unbounded |
| BigQuery | `beam.io.ReadFromBigQuery` / `WriteToBigQuery` | `BigQueryIO` | Both |
| JDBC | `apache_beam.io.jdbc.ReadFromJdbc` / `apache_beam.io.jdbc.WriteToJdbc` | `JdbcIO` | Bounded |

**FlareDB note:** bounded sources and bounded Splittable DoFns are supported, so
`ReadFromText`/`ReadAllFromText` work for text input. Prefer them; use
`beam.Create` + a file-reading `Map`/`FlatMap` only for custom parsing or an
explicit input list. Verify file sinks against the current FlareDB docs. See
[references/flaredb-runner.md](references/flaredb-runner.md) and
[references/flaredb-features.md](references/flaredb-features.md).

Full connector usage: [references/io-connectors.md](references/io-connectors.md).

## 8. FlareDB rules of thumb

- Reduce inside the pipeline; emit counts, top-N, or one short line per item,
  never raw content. Aim for a final result under about 50 lines.
- Make per-item functions defensive: catch per-item exceptions and emit error
  records rather than failing the job.
- Never modify or delete inputs.
- Never submit a FlareDB job (including a trial) before the `flaredb-skills`
  approval gate. That skill owns installation, `flare up`, submission, and logs.

## 9. Streaming rules of thumb

- Never `GroupByKey`/`Combine.perKey` on an unbounded `PCollection` in
  `GlobalWindows` without a non-default trigger.
- Decide late-data handling explicitly; default allowed lateness is 0.
- Guard idle source partitions (Kafka) so the watermark keeps advancing.
- Set `save_main_session=True` in Python when lambdas/DoFns capture module
  globals.
- Stateful DoFns require keyed input first.
- These rules apply to unbounded input. FlareDB does not support unbounded
  sources, but it does support fixed/sliding windows, triggers, and bag/value
  state per [references/flaredb-features.md](references/flaredb-features.md).
  Use `beam-streaming` only when the user explicitly requests streaming.

## 10. Testing checklist

- [ ] Test each DoFn/transform with `TestPipeline` + `Create` before a full run.
- [ ] Verify with `assert_that(pcoll, equal_to([...]))` (Python) or `PAssert`
  (Java).
- [ ] Use `TestStream` only for streaming/windowed logic on a streaming runner.
- [ ] Run DirectRunner first; then a FlareDB trial only after approval.
- [ ] Keep DoFn classes/functions picklable (Python) or serializable (Java).
- [ ] Verify connectors have correct credentials and accessible paths.

## 11. Quick debugging

| Error | Fix |
|---|---|
| `PicklingError` (Python) | Use a top-level `DoFn` class; initialize resources in `setup()`; set `save_main_session=True`. |
| `NotSerializableException` (Java) | Mark fields `transient`, initialize in `@Setup`, make inner DoFns static. |
| `Cannot infer coder` (Java) | Add `TypeDescriptors` or `@DefaultSchema`. |
| `GroupByKey` on unbounded without window | Add `Window.into(...)` first (non-FlareDB runners). |
| No output on FlareDB | Check `flare logs <JOB_ID>`; ensure the source is a supported bounded source/SDF and every transform yields compact records. |
| Connection refused to `127.0.0.1:8099` | Run `flare up`; see `flaredb-skills`. |
| `ModuleNotFoundError: flaredb_runner` | Activate the venv and `pip install flaredb-runner`. |

## 12. Load more detail

| Reference | Contents |
|---|---|
| [references/flaredb-runner.md](references/flaredb-runner.md) | FlareDB Python patterns, input/output, constraints, troubleshooting |
| [references/flaredb-features.md](references/flaredb-features.md) | FlareDB capability matrix: supported and unsupported Beam features |
| [references/transforms.md](references/transforms.md) | All transforms with Python + Java code, DoFn lifecycle, composites, anti-patterns |
| [references/io-connectors.md](references/io-connectors.md) | Text, Avro, Parquet, Kafka, Pub/Sub, BigQuery, JDBC read/write examples |
| [references/coders-and-schemas.md](references/coders-and-schemas.md) | Coders, type hints, NamedTuple, Beam Schemas, coder errors |
| [references/windowing.md](references/windowing.md) | Windows, triggers, accumulation modes, allowed lateness |
| [../beam-debugging/references/testing-patterns.md](../beam-debugging/references/testing-patterns.md) | TestPipeline, `assert_that`, TestStream, integration tests |
