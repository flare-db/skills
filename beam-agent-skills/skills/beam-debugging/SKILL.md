---
name: beam-debugging
description: Guide for diagnosing and fixing Apache Beam pipeline failures, including construction-time errors, worker runtime exceptions, logic bugs, performance issues, and testing strategies. Use when a pipeline throws an error, produces wrong output, hangs, OOMs, or runs unexpectedly slowly.
---

# Apache Beam Pipeline Debugging Guide

Use this skill when a Beam pipeline fails to build, fails at runtime, produces incorrect results, hangs without output, or has performance issues.

---

## 1. When to Use This Skill

- **Construction-time failure**: Pipeline fails before it runs (e.g., `IllegalStateException`, `Cannot infer coder`).
- **Worker runtime failure**: Pipeline starts but workers throw exceptions (`NotSerializableException`, `OutOfMemoryError`, `NullPointerException`).
- **Logic errors**: Pipeline runs to completion but output is wrong, missing, or malformed.
- **No output / Hangs**: Pipeline runs indefinitely without emitting results.
- **Performance**: Pipeline is unexpectedly slow, straggling workers, high GC pressure.

---

## 2. Diagnostic Decision Tree

Work through these checks in order:

```
1. Does the pipeline fail during graph construction (before .run())?
   YES → Check Section 4: Construction-Time Errors
   NO  ↓

2. Does the job fail during runner submission / planning?
   YES → Check serialization (Section 5.1) and runner-specific options
   NO  ↓

3. Does the pipeline start running but workers throw exceptions?
   YES → Check Section 5: Worker Runtime Errors
   NO  ↓

4. Does the pipeline complete but produce wrong output?
   YES → Check Section 6: Logic Errors
   NO  ↓

5. Does the pipeline run indefinitely without emitting results?
   YES → Streaming: check watermark config (Section 4 of beam-streaming skill)
         Batch: check for infinite loops in DoFns or very large input
   NO  ↓

6. Does the pipeline run slowly or OOM?
   YES → Check Section 7: Performance Issues
```

---

## 3. Local Testing with DirectRunner

**Always run with DirectRunner first.** The DirectRunner:
- Serializes and deserializes `DoFn` classes to catch non-serializable fields early.
- Processes elements sequentially to make stack traces clear.
- Validates coder inference — prevents "works locally, fails on cluster" surprises.
- Does not require cloud credentials or external infra.

**Java — DirectRunner:**
```bash
mvn compile exec:java \
  -Dexec.mainClass=com.example.MyPipeline \
  -Dexec.args="--runner=DirectRunner \
               --inputFile=/tmp/test-input.txt \
               --outputFile=/tmp/test-output"
```

**Python — DirectRunner:**
```bash
python my_pipeline.py \
  --runner=DirectRunner \
  --input=/tmp/test-input.txt \
  --output=/tmp/test-output
```

**Enable more verbose logging:**
```bash
# Java: add to -Dexec.args
--defaultWorkerLogLevel=DEBUG

# Python:
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 4. Construction-Time Errors

### 4.1 IllegalStateException: GroupByKey on Unbounded

**Symptom:**
```
java.lang.IllegalStateException: GroupByKey cannot be applied to an unbounded PCollection
in the GlobalWindow without a trigger. Use a Window first.
```

**Root Cause:** Applying `GroupByKey` or `Combine.perKey` on an unbounded source (Kafka, Pub/Sub) with the default `GlobalWindows` and default trigger (which never fires on streaming data).

**Fix:**
```java
// WRONG:
kafkaStream
    .apply("PairWithOne", MapElements.into(...).via(s -> KV.of(s, 1L)))
    .apply("GroupByKey", GroupByKey.create());  // ERROR

// CORRECT: Apply windowing first
kafkaStream
    .apply("PairWithOne", MapElements.into(...).via(s -> KV.of(s, 1L)))
    .apply("Window", Window.into(FixedWindows.of(Duration.standardMinutes(5))))
    .apply("GroupByKey", GroupByKey.create());
```

### 4.2 Cannot Infer Coder

**Symptom:**
```
java.lang.IllegalStateException: Unable to return a default Coder for
MapElements.MapFn. Correct one of the following root causes:
  - No Coder has been manually specified; you may do so using .setCoder().
  - Inferring a Coder from the CoderRegistry failed.
```

**Root Cause:** Java type erasure prevents Beam from inferring the output type from a lambda at runtime.

**Fix (Option 1 — TypeDescriptors):**
```java
// WRONG:
words.apply(MapElements.via((String w) -> w.length()));  // type erased

// CORRECT:
words.apply(MapElements
    .into(TypeDescriptors.integers())
    .via((String w) -> w.length()));
```

**Fix (Option 2 — SimpleFunction):**
```java
// Use anonymous class instead of lambda (Beam can inspect its generic signature):
words.apply(MapElements.via(new SimpleFunction<String, Integer>() {
    @Override
    public Integer apply(String input) { return input.length(); }
}));
```

**Fix (Option 3 — @DefaultSchema for POJOs):**
```java
@DefaultSchema(JavaFieldSchema.class)
public class MyRecord { public String name; public int count; }

// Beam now infers the coder from the Schema annotation
PCollection<MyRecord> records = ...;
```

### 4.3 Pipeline Validation Failures

**Symptom:**
```
org.apache.beam.sdk.options.PipelineOptionsValidator: Missing required value for [--outputFile]
```

**Fix:** Ensure all `@Required` options are passed as command-line arguments, or provide defaults in the options interface:
```java
@Description("Output file path")
@Default.String("gs://my-bucket/output")
String getOutputFile();
```

---

## 5. Worker Runtime Errors

### 5.1 Serialization Errors (Java: NotSerializableException)

**Symptom:**
```
java.io.NotSerializableException: com.example.MyHttpClient
  at java.io.ObjectOutputStream.writeObject(ObjectOutputStream.java:349)
  ...
  at org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray(...)
```

**Root Cause:** Beam serializes the entire `DoFn` instance (including all fields) to send it to workers. Any non-serializable field causes this error.

**Common causes:**
- Logger: `private Logger LOG = LoggerFactory.getLogger(...)` — loggers are not serializable.
- HTTP clients: `OkHttpClient`, `HttpClient`, etc.
- Database connections: `Connection`, `DataSource`, etc.
- Thread pools: `ExecutorService`, `ScheduledExecutorService`.
- Non-static inner class capturing outer `this` — the outer class must also be serializable.

**Fix Pattern 1 — Mark field transient, initialize in `@Setup`:**
```java
public class MyDoFn extends DoFn<String, String> {
    // WRONG: initialized at construction, not serializable
    // private HttpClient client = HttpClient.newHttpClient();

    // CORRECT: transient = excluded from serialization
    private transient HttpClient client;
    private transient Logger log;

    @Setup
    public void setup() {
        this.client = HttpClient.newHttpClient();  // re-created on each worker
        this.log = LoggerFactory.getLogger(MyDoFn.class);
    }

    @ProcessElement
    public void process(@Element String s, OutputReceiver<String> out) {
        // use client and log safely
    }

    @Teardown
    public void teardown() {
        client = null;
    }
}
```

**Fix Pattern 2 — Static inner class:**
```java
// WRONG: non-static inner class captures outer 'this'
public class MyTransform extends PTransform<...> {
    private String prefix = "x_";

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void process(@Element String s, OutputReceiver<String> out) {
                out.output(prefix + s);  // captures outer 'this.prefix' — ERROR
            }
        }));
    }
}

// CORRECT: static inner class with explicitly passed fields
public class MyTransform extends PTransform<...> {
    private final String prefix;
    public MyTransform(String prefix) { this.prefix = prefix; }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new PrefixFn(prefix)));
    }

    private static class PrefixFn extends DoFn<String, String> {
        private final String prefix;
        PrefixFn(String p) { this.prefix = p; }

        @ProcessElement
        public void process(@Element String s, OutputReceiver<String> out) {
            out.output(prefix + s);
        }
    }
}
```

---

### 5.2 Serialization Errors (Python: PicklingError)

**Symptom:**
```
PicklingError: Can't pickle <function <lambda> at 0x7f...>: attribute lookup failed
AttributeError: Can't pickle local object 'my_function.<locals>.<lambda>'
```

**Root Cause:** Python serializes `DoFn` and functions using `pickle`. Lambdas, closures that capture unpicklable objects, and threading primitives cannot be pickled.

**Fix — Use a `DoFn` class instead of lambda:**
```python
# WRONG: lambda captures module-level state, may fail to pickle
result = data | beam.Map(lambda x: my_module.expensive_function(x))

# CORRECT: DoFn class with setup/teardown
class ProcessDoFn(beam.DoFn):
    def setup(self):
        self.client = create_client()  # created on worker, not captured from closure

    def process(self, element):
        yield self.client.call(element)

    def teardown(self):
        self.client.close()

result = data | beam.ParDo(ProcessDoFn())
```

**Fix — Set `save_main_session=True`:**
```python
from apache_beam.options.pipeline_options import SetupOptions

pipeline_options = PipelineOptions(pipeline_args)
# Saves the current global namespace so workers can access module-level names
pipeline_options.view_as(SetupOptions).save_main_session = True
```

---

### 5.3 OutOfMemoryError / Hot Key Skew

**Symptom:** One worker runs out of memory; other workers are idle. The failing worker handles a disproportionate number of elements for one key.

**Root Cause:** A single key dominates the data (e.g., `country="US"` has 90% of all records). All elements for that key are routed to the same worker.

**Fix 1 — Key Salting:**
```java
// Java: add a random suffix to distribute hot key across N partitions
int NUM_SALT = 10;

PCollection<KV<String, Long>> salted = input
    .apply("SaltKeys", MapElements.into(
        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
        .via(kv -> KV.of(kv.getKey() + "_" + ThreadLocalRandom.current().nextInt(NUM_SALT), kv.getValue())));

PCollection<KV<String, Long>> partialSums = salted
    .apply("PartialSum", Sum.longsPerKey());

// Remove salt and re-aggregate
PCollection<KV<String, Long>> finalSums = partialSums
    .apply("RemoveSalt", MapElements.into(
        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
        .via(kv -> KV.of(kv.getKey().replaceAll("_\\d+$", ""), kv.getValue())))
    .apply("FinalSum", Sum.longsPerKey());
```

```python
# Python: key salting
import random

NUM_SALT = 10

salted = (
    input_kvs
    | "SaltKeys" >> beam.Map(lambda kv: (f"{kv[0]}_{random.randint(0, NUM_SALT-1)}", kv[1]))
    | "PartialSum" >> beam.CombinePerKey(sum)
    | "RemoveSalt" >> beam.Map(lambda kv: (kv[0].rsplit("_", 1)[0], kv[1]))
    | "FinalSum" >> beam.CombinePerKey(sum)
)
```

**Fix 2 — Use `Combine.perKey` instead of `GroupByKey`:**
```java
// WRONG: GroupByKey materializes all values for a key in memory at once
PCollection<KV<String, Iterable<Long>>> grouped = kvLongs.apply(GroupByKey.create());
PCollection<KV<String, Long>> sums = grouped.apply(
    MapElements.into(...).via(kv -> KV.of(kv.getKey(), Streams.stream(kv.getValue()).sum())));

// CORRECT: Combine uses combiner lifting — partial aggregation before shuffle
PCollection<KV<String, Long>> sums = kvLongs.apply(Sum.longsPerKey());
```

---

## 6. Logic Errors

### 6.1 Wrong Window Assignment

**Symptom:** Elements are grouped in unexpected windows; session windows don't work as expected.

**Cause:** Timestamps were not assigned before windowing, or session windows applied to non-KV data.

**Fix:**
```java
// WRONG: Window before timestamp assignment → uses default timestamp (min value)
events
    .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5))))
    .apply(WithTimestamps.of(e -> new Instant(e.getTimestamp())));

// CORRECT: Assign timestamps FIRST, then window
events
    .apply("AssignTs", WithTimestamps.of(e -> new Instant(e.getTimestamp())))
    .apply("Window", Window.into(FixedWindows.of(Duration.standardMinutes(5))));
```

### 6.2 Accumulation Mode Confusion

**Symptom:** Consumer receives duplicate or missing data from windowed streaming results.

**Fix:** Verify accumulation mode matches consumer expectations:
- **ACCUMULATING**: Each pane is a complete result; consumer should overwrite by key.
- **DISCARDING**: Each pane is a delta; consumer should add/append.

### 6.3 Side Input Key Not Matching

**Symptom:** Side input lookups always return null/empty; enrichment doesn't work.

**Fix:**
- Ensure the side input `PCollection` is computed from the same pipeline.
- Verify the key type and encoding matches between the main collection and the side input.
- For windowed pipelines, ensure the side input window covers the main collection's window (use `View.asSingleton` for a global side input).

---

## 7. Performance Issues

### 7.1 Fusion Breaking

Beam's runner may fuse consecutive transforms into a single execution stage. When a single fused stage is slow (e.g., includes expensive I/O and heavy computation), break fusion to allow parallelism:

```java
// Java: insert a Reshuffle to break fusion
events
    .apply("HeavyIO", MyExpensiveIORead.from("..."))
    .apply("BreakFusion", Reshuffle.viaRandomKey())  // forces materialization
    .apply("HeavyCompute", ParDo.of(new MyExpensiveDoFn()));
```

```python
# Python:
events | "BreakFusion" >> beam.Reshuffle() | "HeavyCompute" >> beam.ParDo(MyDoFn())
```

### 7.2 Logging and Metrics

**Java — Beam Metrics:**
```java
public class MyDoFn extends DoFn<String, String> {
    private final Counter processed = Metrics.counter(MyDoFn.class, "processed-elements");
    private final Distribution latency = Metrics.distribution(MyDoFn.class, "latency-ms");
    private final Gauge lastSeen = Metrics.gauge(MyDoFn.class, "last-seen-timestamp");

    @ProcessElement
    public void process(@Element String s, OutputReceiver<String> out) {
        long start = System.currentTimeMillis();
        try {
            String result = process(s);
            out.output(result);
            processed.inc();
            lastSeen.set(System.currentTimeMillis());
        } finally {
            latency.update(System.currentTimeMillis() - start);
        }
    }
}

// Read metrics after pipeline completes:
PipelineResult result = pipeline.run();
result.waitUntilFinish();
MetricResults metrics = result.metrics();
MetricQueryResults queryResults = metrics.queryMetrics(
    MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.inNamespace(MyDoFn.class))
        .build());
```

**Python — Beam Metrics:**
```python
import apache_beam as beam
from apache_beam import metrics

class MyDoFn(beam.DoFn):
    def __init__(self):
        self.processed = metrics.Metrics.counter(self.__class__, 'processed-elements')
        self.latency = metrics.Metrics.distribution(self.__class__, 'latency-ms')

    def process(self, element):
        import time
        start = time.time()
        try:
            result = self._process(element)
            self.processed.inc()
            yield result
        finally:
            self.latency.update(int((time.time() - start) * 1000))
```

---

## 8. Load More Detail

| Reference File | Contents |
|---|---|
| [references/common-errors.md](references/common-errors.md) | Complete error lookup table, deep-dive diagnosis for serialization, coder, OOM, and watermark issues |
| [references/testing-patterns.md](references/testing-patterns.md) | TestPipeline, PAssert, TestStream, integration testing, running tests locally |
