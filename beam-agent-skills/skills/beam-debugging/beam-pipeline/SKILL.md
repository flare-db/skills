---
name: beam-pipeline
description: End-to-end guide for coding agents to design, construct, test, and debug Apache Beam batch and streaming data processing pipelines in Java and Python. Use when writing new Beam pipelines, implementing transforms or I/O connectors, configuring windowing and triggers, running tests with TestPipeline, or diagnosing pipeline runtime errors.
---

# Apache Beam Pipeline Development

Apache Beam is a unified programming model for defining and executing both batch and streaming data-parallel processing pipelines. Pipelines are defined once and can run on multiple execution engines (runners) such as Google Cloud Dataflow, Apache Flink, FlareDB and Apache Spark.

---

## 1. Agent Workflow

Follow this 8-step workflow when building or modifying a Beam pipeline:

1. **Clarify requirements** — Determine execution model (batch=bounded vs streaming=unbounded), target SDK (Java or Python), input/output systems (Kafka, BigQuery, text files, Pub/Sub, JDBC), and target runner (DirectRunner for local execution, DataflowRunner, FlinkRunner, FlareDB Runner, SparkRunner).
2. **Define PipelineOptions** — Expose all runtime parameters (input/output paths, runner flags, project IDs) through a typed `PipelineOptions` interface (Java) or argparse with `PipelineOptions` (Python). Never hard-code paths.
3. **Design the pipeline topology** — Sketch the DAG: sources → transformations → sinks. Decide on grouping (keyed vs unkeyed), side inputs, dead-letter routing. For transform patterns, load [references/transforms.md](references/transforms.md).
4. **Choose data model and coders** — Prefer Beam Schemas (`@DefaultSchema` in Java, `NamedTuple` in Python) over manual coders. Provide explicit `TypeDescriptor`s in Java lambdas to avoid type-erasure errors. For details, load [references/coders-and-schemas.md](references/coders-and-schemas.md).
5. **Configure windowing** (streaming/aggregations only) — Any `GroupByKey` or `Combine.perKey` on an unbounded source requires non-global windowing (or a non-default trigger). Choose triggers, accumulation mode, and allowed lateness to match your late-data requirements. For patterns, load [references/windowing.md](references/windowing.md).
6. **Choose IO connectors** — Use built-in connectors. Configure watermarks for streaming sources. For connector details, load [references/io-connectors.md](references/io-connectors.md).
7. **Write unit tests** — Test transforms with `TestPipeline` + `Create.of` + `PAssert`/`assert_that`. For streaming, use `TestStream`. Load [references/testing-patterns.md](../beam-debugging/references/testing-patterns.md) from the beam-debugging skill.
8. **Verify runner compatibility** — Run locally with `DirectRunner` first. Then verify options against the target runner.

---

## 2. Pipeline Skeleton — Java

```java
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Arrays;

public class WordCountPipeline {

  // 1. Define options interface — expose all runtime parameters
  public interface WordCountOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Required
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the output file (prefix)")
    @Required
    String getOutputFile();
    void setOutputFile(String value);
  }

  public static void main(String[] args) {
    // 2. Parse options from command line
    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

    // 3. Create pipeline with options (runner selected by --runner flag)
    Pipeline pipeline = Pipeline.create(options);

    // 4. Build the pipeline topology
    pipeline
        // Read lines from text file(s) — supports wildcards: gs://bucket/*.txt
        .apply("ReadLines", TextIO.read().from(options.getInputFile()))
        // Split each line into words — TypeDescriptors.strings() prevents coder errors
        .apply("ExtractWords",
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        // Drop empty strings
        .apply("FilterEmpty", Filter.by((String word) -> !word.isEmpty()))
        // Count occurrences of each unique word
        .apply("CountPerWord", Count.perElement())
        // Format for output
        .apply("FormatOutput",
            MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> wc) -> wc.getKey() + ": " + wc.getValue()))
        // Write results to text file(s)
        .apply("WriteResults", TextIO.write().to(options.getOutputFile()));

    // 5. Run (blocks until completion)
    pipeline.run().waitUntilFinish();
  }
}
```

**Run:**
```bash
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCountPipeline \
  -Dexec.args="--inputFile=gs://my-bucket/input/*.txt \
               --outputFile=gs://my-bucket/output/counts \
               --runner=DataflowRunner \
               --project=my-gcp-project \
               --region=us-central1 \
               --gcpTempLocation=gs://my-bucket/tmp"
```

---

## 3. Pipeline Skeleton — Python

```python
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input', required=True,
        help='Input file pattern (e.g., gs://bucket/input/*.txt)')
    parser.add_argument(
        '--output', required=True,
        help='Output file prefix (e.g., gs://bucket/output/counts)')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    # save_main_session=True makes global imports available on workers
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'ReadLines'    >> beam.io.ReadFromText(known_args.input)
            | 'ExtractWords' >> beam.FlatMap(
                lambda line: [w for w in line.split() if w.isalpha()])
            | 'CountWords'   >> beam.combiners.Count.PerElement()
            | 'Format'       >> beam.Map(
                lambda word_count: f'{word_count[0]}: {word_count[1]}')
            | 'WriteResults' >> beam.io.WriteToText(known_args.output)
        )


if __name__ == '__main__':
    run()
```

**Run:**
```bash
python wordcount.py \
  --input gs://my-bucket/input/*.txt \
  --output gs://my-bucket/output/counts \
  --runner DataflowRunner \
  --project my-gcp-project \
  --region us-central1 \
  --temp_location gs://my-bucket/tmp
```

---

## 4. Transform Selection

| Need | Java | Python | Key Constraint |
|---|---|---|---|
| 1-to-1 element mapping | `MapElements.into(TypeDescr).via(fn)` | `beam.Map(fn)` | Must provide `TypeDescriptors` for Java lambdas |
| 1-to-N expansion | `FlatMapElements.into(TypeDescr).via(fn)` | `beam.FlatMap(fn)` | Return iterable/generator |
| Filter elements | `Filter.by(predicate)` | `beam.Filter(predicate)` | Predicate returns boolean |
| Complex / stateful / multi-output | `ParDo.of(new MyDoFn())` | `beam.ParDo(MyDoFn())` | Supports lifecycle hooks, side inputs, timers |
| Count occurrences | `Count.perElement()` | `beam.combiners.Count.PerElement()` | Inputs must be elements (not KVs) |
| Group values by key | `GroupByKey.create()` | `beam.GroupByKey()` | Requires `KV<K,V>` input; unbounded needs windowing |
| Associative aggregation | `Combine.perKey(fn)` / `Combine.globally(fn)` | `beam.CombinePerKey(fn)` | Combiner lifting reduces shuffle; prefer over GBK+map |
| Join two datasets on key | `CoGroupByKey.create()` | `beam.CoGroupByKey()` | Both must be `KV<K, V>` |
| Merge same-type collections | `Flatten.pCollections()` | `beam.Flatten()` | No data transformation; preserves windowing |
| Split into N buckets | `Partition.of(N, partFn)` | `beam.Partition(fn, N)` | Partition fn returns integer `[0, N)` |
| Enrich with lookup | `ParDo.of(dofn).withSideInputs(view)` | `beam.ParDo(dofn, view=AsSingleton(pcoll))` | Side input must fit in worker memory |
| Route bad records | `ParDo + TupleTag multiple outputs` | `beam.ParDo + TaggedOutput` | Use dead-letter queue pattern |

*For full code examples of each transform → load [references/transforms.md](references/transforms.md)*

---

## 5. IO Connector Selection

| Source / Sink | Java Class | Python Class | Bounded/Unbounded |
|---|---|---|---|
| Text files (line-by-line) | `TextIO` | `beam.io.ReadFromText` / `WriteToText` | Bounded |
| Avro files | `AvroIO` | `beam.io.ReadFromAvro` / `WriteToAvro` | Bounded |
| Parquet files | `ParquetIO` | `beam.io.ReadFromParquet` / `WriteToParquet` | Bounded |
| Apache Kafka | `KafkaIO` | `beam.io.ReadFromKafka` / `WriteToKafka` | Unbounded |
| Google Pub/Sub | `PubsubIO` | `beam.io.ReadFromPubSub` / `WriteToPubSub` | Unbounded |
| Google BigQuery | `BigQueryIO` | `beam.io.ReadFromBigQuery` / `WriteToBigQuery` | Both |
| JDBC / Relational DB | `JdbcIO` | `beam.io.ReadFromJdbc` / `WriteToJdbc` | Bounded |

*Full connector usage with code → load [references/io-connectors.md](references/io-connectors.md)*

---

## 6. Streaming Rules of Thumb

- **Never call `GroupByKey` / `Combine.perKey` on an unbounded `PCollection` in `GlobalWindows`** without configuring a non-default trigger — it will either throw at pipeline construction time or never fire.
- **Decide how to handle late data explicitly.** The default allowed lateness is `0`, so late arrivals are silently dropped. Set a nonzero `withAllowedLateness` (with a `withLateFirings` trigger) only when late data matters for your use case; otherwise leave the default.
- **Guard against idle source partitions** (Kafka and similar) — an idle partition freezes the watermark for all keys on that worker, preventing downstream windows from firing. Use `withCreateTime(Duration)` (Java) or `timestamp_policy=ReadFromKafka.log_append_time` (Python) so idle partitions auto-advance the watermark.
- **Set `save_main_session=True`** (Python) when using global imports inside lambdas or `DoFn.process()` — without it, workers cannot deserialize functions that capture module-level names.
- **Stateful `DoFn`s require keyed input** — apply `beam.Map(lambda x: (key_fn(x), x))` before using state or timers.

---

## 7. Testing Checklist

- [ ] Test each `DoFn` or composite transform independently with `TestPipeline` and `Create.of` — do not test the whole pipeline as a unit.
- [ ] Verify outputs with `PAssert.that(pcoll).containsInAnyOrder(...)` (Java) or `assert_that(pcoll, equal_to([...]))` (Python).
- [ ] For streaming/windowed logic use `TestStream` to control timestamps and watermark advancement deterministically.
- [ ] Run with `DirectRunner` before any distributed runner — it validates serialization and coder inference.
- [ ] Check that `DoFn` classes and all fields are serializable (Java) or picklable (Python).
- [ ] Verify that IO connectors have the correct credentials and accessible URIs.

---

## 8. Quick Debugging

| Error | Fix |
|---|---|
| `NotSerializableException` (Java) | Mark non-serializable fields `transient`; initialize them in `@Setup`. Make inner `DoFn` classes `static`. |
| `PicklingError` (Python) | Avoid closures capturing module globals; don't use `lambda` inside `ParDo` for complex logic — use a `DoFn` class. |
| `Cannot infer coder` (Java) | Add `TypeDescriptors` to `MapElements`/`FlatMapElements`, or annotate POJO with `@DefaultSchema(JavaFieldSchema.class)`. |
| `GroupByKey on unbounded without window` | Add `Window.into(FixedWindows.of(...))` before `GroupByKey`. |
| Pipeline produces no output | Check watermark config — idle source partitions freeze watermarks. Use `withCreateTime`/`withLogAppendTime` (Java) or `timestamp_policy` (Python). |

---

## 9. Load More Detail

| Reference File | Contents |
|---|---|
| [references/transforms.md](references/transforms.md) | All transform types with full Java + Python code, DoFn lifecycle, composites, anti-patterns |
| [references/windowing.md](references/windowing.md) | Window types, triggers, accumulation modes, allowed lateness, full examples |
| [references/coders-and-schemas.md](references/coders-and-schemas.md) | Coder types, TypeDescriptors, Beam Schemas, custom coders, error diagnosis |
| [references/io-connectors.md](references/io-connectors.md) | TextIO, AvroIO, ParquetIO, KafkaIO, PubsubIO, BigQueryIO, JdbcIO — full read/write examples |
