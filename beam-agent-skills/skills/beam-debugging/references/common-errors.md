# Beam Pipeline: Common Errors Reference

Comprehensive lookup table and deep-dive diagnosis for every common Apache Beam pipeline error.

> **Python first.** New pipelines default to Python. For FlareDB runner/CLI
> errors (`flare logs`, connection refused, install issues), read
> `flaredb-skills`; this reference covers Beam-level failures.

---

## 1. Quick Lookup Table

| Error / Symptom | Category | Root Cause | One-Line Fix |
|---|---|---|---|
| `java.io.NotSerializableException: MyClass` | Serialization | `DoFn` field is not `java.io.Serializable` | Mark field `transient`, initialize in `@Setup` |
| `PicklingError: Can't pickle <lambda>` (Python) | Serialization | Lambda or closure captures unpicklable object | Replace with a `DoFn` class; set `save_main_session=True` |
| `Cannot infer coder for type T` (Java) | Coder inference | Java type erasure: lambda in `MapElements` loses generic type | Add `TypeDescriptors.xxx()` to `MapElements.into(...)` |
| `IllegalStateException: GroupByKey applied to unbounded PCollection` | Construction | `GroupByKey` on streaming source without windowing | Add `Window.into(FixedWindows.of(...))` before `GroupByKey` |
| `java.lang.OutOfMemoryError: GC overhead / Java heap space` | Performance | Hot-key skew — one key receives disproportionate data | Key salting + `Combine.perKey` instead of `GroupByKey` |
| Pipeline hangs — no output produced (streaming) | Watermark | Watermark never advances; idle source partition | Add idle timeout to source; configure watermark strategy |
| Records silently dropped — no error but data missing | Late data | Late elements dropped (default allowed lateness = 0) | Add `.withAllowedLateness(Duration)` to window config |
| Elements assigned to wrong window | Logic | Timestamps assigned after windowing, or not assigned at all | Assign timestamps (WithTimestamps / outputWithTimestamp) BEFORE Window.into |
| `CoderException: Failed to encode element` | Coder | Coder cannot handle a specific value (e.g., null in non-nullable coder) | Wrap with `NullableCoder`; filter nulls before encode |
| `ClassCastException` during coder decode | Coder | Wrong coder registered; type mismatch in `KvCoder<K,V>` | Verify coder registry entries; check `KvCoder` key/value types |
| Watermark never advances past a specific timestamp | Watermark | One source partition is idle or producing far-future timestamps | Add `.withIdleStateUpdateRate` / `withIdleTimeout` to source |
| `BigQueryIOException: quotaExceeded` | IO | Too many streaming insert requests per second | Use file-load write method; reduce parallelism; add retry logic |
| `org.apache.kafka.common.errors.OffsetOutOfRangeException` | IO | Requested Kafka offset no longer exists (retention expired) | Change `auto.offset.reset` to `earliest` or `latest` |
| `FileNotFoundException` for temp location | Config | Temp/staging GCS or S3 bucket path doesn't exist or no permission | Create bucket; verify service account has `roles/storage.admin` |
| `UnsupportedOperationException: operation not supported by runner` | Runner compat | Feature not implemented by the target runner (e.g., state on a non-FlareDB runner) | Check the target runner's capabilities; switch to a compatible runner or a bounded FlareDB job |
| `java.lang.NullPointerException in processElement` | Logic | Element or a field is null and not guarded | Add null checks; use `@Nullable` / `Optional` |
| `io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED` | Infra | Worker exceeded gRPC message size limit or quota | Reduce batch sizes; enable chunking in gRPC calls |

---

## 2. Deep Dive: Java Serialization Errors

### How Beam Serializes DoFns

When you call `pipeline.run()`, Beam serializes the full `DoFn` object graph using Java object serialization (`ObjectOutputStream`) and sends the byte array to each worker. The worker deserializes it and creates a fresh `DoFn` instance.

**Everything reachable from a `DoFn` field must be `Serializable`**, or marked `transient`.

### Non-Serializable Objects and Fixes

#### Pattern 1: HTTP clients, DB connections, thread pools

```java
// WRONG — HttpClient is not serializable
public class FetchDoFn extends DoFn<String, String> {
    private HttpClient client = HttpClient.newHttpClient(); // serialized with DoFn — FAILS

    @ProcessElement
    public void process(@Element String url, OutputReceiver<String> out) {
        // use client...
    }
}

// CORRECT — transient + @Setup
public class FetchDoFn extends DoFn<String, String> {
    private transient HttpClient client;      // excluded from serialization
    private transient Connection dbConn;      // excluded from serialization
    private transient ExecutorService pool;   // excluded from serialization

    @Setup
    public void setup() {
        // Re-created on each worker, after deserialization
        this.client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();
        this.dbConn = DriverManager.getConnection("jdbc:postgresql://host/db", "user", "pass");
        this.pool = Executors.newFixedThreadPool(4);
    }

    @ProcessElement
    public void process(@Element String url, OutputReceiver<String> out) {
        // safely use client, dbConn, pool
    }

    @Teardown
    public void teardown() throws Exception {
        if (client != null) client = null;
        if (dbConn != null) dbConn.close();
        if (pool != null) pool.shutdown();
    }
}
```

#### Pattern 2: Loggers

```java
// WRONG — SLF4J Logger is not serializable in most implementations
public class MyDoFn extends DoFn<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(MyDoFn.class);
    // This WORKS in some JVM configs but is fragile and runner-dependent.
    // Safest approach: make it transient.

    private transient Logger log;  // CORRECT: transient

    @Setup
    public void setup() {
        this.log = LoggerFactory.getLogger(MyDoFn.class);
    }
}
```

#### Pattern 3: Non-static inner classes

```java
// WRONG — inner class implicitly holds reference to outer class instance
public class MyPipeline {
    private String config = "prod";

    public PCollection<String> buildTransform(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void process(@Element String s, OutputReceiver<String> out) {
                out.output(config + ":" + s); // captures MyPipeline.this — NOT SERIALIZABLE
            }
        }));
    }
}

// CORRECT — static nested class with explicit parameter
public class MyPipeline {
    private String config = "prod";

    public PCollection<String> buildTransform(PCollection<String> input) {
        return input.apply(ParDo.of(new ConfigPrefixFn(config)));
    }

    private static class ConfigPrefixFn extends DoFn<String, String> {
        private final String config;
        ConfigPrefixFn(String config) { this.config = config; }

        @ProcessElement
        public void process(@Element String s, OutputReceiver<String> out) {
            out.output(config + ":" + s); // safe — config is a serializable String field
        }
    }
}
```

#### Pattern 4: Factory pattern for complex non-serializable resources

```java
// Serialize a factory (serializable config object) instead of the resource itself
@FunctionalInterface
public interface ClientFactory extends Serializable {
    MyClient create();
}

public class MyDoFn extends DoFn<String, String> {
    private final ClientFactory factory;   // factory is serializable
    private transient MyClient client;

    public MyDoFn(ClientFactory factory) { this.factory = factory; }

    @Setup
    public void setup() {
        this.client = factory.create();    // client created on worker
    }
}

// Usage:
ParDo.of(new MyDoFn(() -> new MyClient("https://api.example.com")));
```

---

## 3. Deep Dive: Python Serialization Errors

### How Python Pickling Works for DoFns

Beam uses Python's `pickle` module (or `cloudpickle`) to serialize `DoFn` instances and lambda functions before sending them to workers. Python's pickling is more flexible than Java serialization but still has limitations.

### What Cannot Be Pickled

| Unpicklable Object | Why | Fix |
|---|---|---|
| Lambda with module-level import | Captures closure referencing module state | Move to a `DoFn` class method |
| `threading.Lock`, `threading.Event` | OS primitives, not portable across processes | Initialize in `setup()` instead |
| Open file handles (`open(...)`) | File descriptors are process-local | Open in `setup()`, close in `teardown()` |
| Database connections | Network state, not portable | Create in `setup()` |
| Generator objects | Can't serialize generator state | Materialize to list first |
| Lambdas defined inside another function | Pickle can't resolve the function's qualified name | Use a top-level function or class |

### Fix Patterns

```python
# WRONG: lambda capturing module-level heavy object
import heavy_module

result = data | beam.Map(lambda x: heavy_module.process(x))
# heavy_module.process may itself reference unpicklable objects

# CORRECT: DoFn class with lazy initialization
class ProcessDoFn(beam.DoFn):
    def setup(self):
        import heavy_module  # imported lazily on each worker
        self.processor = heavy_module.create_processor()

    def process(self, element):
        yield self.processor.run(element)

    def teardown(self):
        self.processor.close()
```

```python
# WRONG: capturing open file handle
f = open("/tmp/config.txt")

result = data | beam.Map(lambda x: f.read() + x)  # f is not picklable

# CORRECT: read config at construction time into a plain string
config_text = open("/tmp/config.txt").read()  # read once; string IS picklable

class ApplyConfigDoFn(beam.DoFn):
    def __init__(self, config):
        self.config = config  # plain string — picklable

    def process(self, element):
        yield self.config + element

result = data | beam.ParDo(ApplyConfigDoFn(config_text))
```

```python
# WRONG: lambda defined inside another function (pickle loses the qualified name)
def build_pipeline(multiplier):
    return data | beam.Map(lambda x: x * multiplier)  # closure — may fail to pickle

# CORRECT: top-level function with parameter
class MultiplyDoFn(beam.DoFn):
    def __init__(self, factor):
        self.factor = factor

    def process(self, element):
        yield element * self.factor

def build_pipeline(multiplier):
    return data | beam.ParDo(MultiplyDoFn(multiplier))
```

**Always set `save_main_session=True`** when using module-level functions inside lambdas or top-level DoFns:

```python
from apache_beam.options.pipeline_options import SetupOptions
options.view_as(SetupOptions).save_main_session = True
```

This pickles the entire main module's global namespace and ships it to workers, allowing workers to resolve module-level names.

---

## 4. Deep Dive: Java Coder Inference

### Why Java Generics Cause Type Erasure

Java compiles generic types by erasing them at the bytecode level. At runtime, `List<String>` and `List<Integer>` are both just `List`. Beam relies on runtime type information to choose a coder — so when it encounters a `MapElements` with a lambda, it cannot determine the output type.

### Three Patterns to Fix

#### Pattern 1: TypeDescriptors in MapElements / FlatMapElements

```java
// All TypeDescriptors options:
TypeDescriptors.strings()          // String
TypeDescriptors.integers()         // Integer
TypeDescriptors.longs()            // Long
TypeDescriptors.doubles()          // Double
TypeDescriptors.booleans()         // Boolean
TypeDescriptors.voids()            // Void
TypeDescriptors.kvs(k, v)          // KV<K, V>
TypeDescriptors.iterables(elem)    // Iterable<T>
TypeDescriptors.lists(elem)        // List<T>
TypeDescriptor.of(MyClass.class)   // Custom class

// Usage in MapElements:
PCollection<Integer> lengths = words.apply(
    MapElements.into(TypeDescriptors.integers())
        .via((String w) -> w.length()));

// Usage in FlatMapElements:
PCollection<String> tokens = lines.apply(
    FlatMapElements.into(TypeDescriptors.strings())
        .via((String line) -> Arrays.asList(line.split("\\s+"))));

// KV output:
PCollection<KV<String, Integer>> pairs = words.apply(
    MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
        .via((String w) -> KV.of(w, w.length())));
```

#### Pattern 2: @DefaultSchema on POJO Classes

```java
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

// Beam uses the Schema to infer a RowCoder automatically
@DefaultSchema(JavaFieldSchema.class)
public class LogEntry {
    public String level;
    public String message;
    public long timestampMillis;

    public LogEntry() {}  // required no-arg constructor
    public LogEntry(String level, String message, long ts) {
        this.level = level;
        this.message = message;
        this.timestampMillis = ts;
    }
}

// Now Beam can infer the coder automatically for PCollection<LogEntry>
PCollection<LogEntry> entries = pipeline.apply(/* source */);
```

#### Pattern 3: Manual Coder Registration

```java
Pipeline pipeline = Pipeline.create(options);

// Register a specific coder for a class globally in this pipeline
pipeline.getCoderRegistry().registerCoderForClass(
    GeoPoint.class, AvroCoder.of(GeoPoint.class));

// Or using @DefaultCoder annotation directly on the class:
@org.apache.beam.sdk.coders.DefaultCoder(AvroCoder.class)
public class GeoPoint {
    public double lat, lon;
    public GeoPoint() {}
}
```

---

## 5. Deep Dive: OutOfMemory / Hot Key Skew

### What Hot Keys Are

In a keyed operation (`GroupByKey`, `Combine.perKey`, stateful `DoFn`), all elements for a given key are routed to the **same worker**. If a key like `"US"` or `"page_home"` receives 80% of all traffic, that worker runs out of heap space while others sit idle.

### Detecting Hot Keys

- **Runner UI**: Look for "bundle size" or "element count" distribution across workers — one worker will have far more elements.
- **Metrics**: Add a `Counter` per key in your DoFn; query after the run to see which keys are disproportionately large.
- **Logs**: OOM on one worker while others succeed is the classic hot key signature.

### Fix 1: Key Salting

Distribute a hot key across multiple workers by adding a random suffix, aggregating partially, then removing the salt and re-aggregating:

```java
// Java: full key salting pattern for Sum
static final int N_SALT = 20;

PCollection<KV<String, Long>> salted = input
    .apply("SaltKeys", MapElements.into(
        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
        .via(kv -> KV.of(
            kv.getKey() + "_" + ThreadLocalRandom.current().nextInt(N_SALT),
            kv.getValue())));

PCollection<KV<String, Long>> partialSums = salted.apply("PartialSum", Sum.longsPerKey());

PCollection<KV<String, Long>> finalSums = partialSums
    .apply("RemoveSalt", MapElements.into(
        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
        .via(kv -> KV.of(
            kv.getKey().replaceAll("_\\d+$", ""),
            kv.getValue())))
    .apply("FinalSum", Sum.longsPerKey());
```

```python
# Python: key salting
import random

N_SALT = 20

salted = (
    input_kvs
    | "SaltKeys"    >> beam.Map(lambda kv: (f"{kv[0]}_{random.randint(0, N_SALT-1)}", kv[1]))
    | "PartialSum"  >> beam.CombinePerKey(sum)
    | "RemoveSalt"  >> beam.Map(lambda kv: (kv[0].rsplit("_", 1)[0], kv[1]))
    | "FinalSum"    >> beam.CombinePerKey(sum)
)
```

### Fix 2: Replace GroupByKey with Combine.perKey

`Combine.perKey` performs combiner lifting — partial aggregation happens locally on each mapper before the shuffle, dramatically reducing the amount of data sent to the reducer worker.

```java
// WRONG: GroupByKey + manual sum — no combiner lifting
PCollection<KV<String, Long>> sums = kvLongs
    .apply(GroupByKey.create())
    .apply(MapElements.into(...)
        .via(kv -> KV.of(kv.getKey(), StreamSupport.stream(
            kv.getValue().spliterator(), false).mapToLong(Long::longValue).sum())));

// CORRECT: Combine.perKey applies combiner lifting automatically
PCollection<KV<String, Long>> sums = kvLongs.apply(Sum.longsPerKey());
// Or with custom CombineFn:
PCollection<KV<String, Double>> avgs = kvLongs.apply(Combine.perKey(new AverageFn()));
```

### Fix 3: GroupIntoBatches for External Service Calls

If you must group elements (e.g., to batch external API calls), use `GroupIntoBatches` instead of `GroupByKey` to limit memory usage per worker:

```java
// Group elements into batches of at most 100 per key
PCollection<KV<String, Iterable<String>>> batches = keyedStrings
    .apply("BatchElements",
        GroupIntoBatches.<String, String>ofSize(100)
            .withMaxBufferingDuration(Duration.standardSeconds(5)));

batches.apply("CallAPI", ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
    @ProcessElement
    public void process(@Element KV<String, Iterable<String>> batch, OutputReceiver<String> out) {
        List<String> items = StreamSupport.stream(batch.getValue().spliterator(), false)
            .collect(Collectors.toList());
        // Make one API call for the whole batch
        String response = callBulkAPI(items);
        out.output(response);
    }
}));
```

---

## 6. Deep Dive: Watermark Stalls

### Causes of Watermark Stalls

| Cause | Mechanism | Detection |
|---|---|---|
| Idle Kafka partition | No messages → no new watermark from that partition → global watermark freezes | Check source-stage metrics; watermark constant for > 1 minute |
| Clock skew between producers | One producer sends messages with future timestamps → watermark can't advance past minimum seen | Inspect Kafka message timestamps; check for far-future values |
| Custom source not advancing watermark | `getWatermark()` returns `BoundedWindow.TIMESTAMP_MIN_VALUE` constantly | Check custom source's `getWatermark()` implementation |
| `GlobalWindows` with default trigger | Default trigger fires on `TIMESTAMP_MAX_VALUE` — never fires for streaming | Use `Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()...)` |

### Fix 1: Kafka Idle Timeout (Java)

```java
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withWatermarkFn(record -> new Instant(record.getTimestamp()))
    // If a partition receives no messages for 2 minutes, advance watermark anyway
    .withIdleStateUpdateRate(Duration.standardMinutes(2));
```

### Fix 2: Bounded Watermark Drift

Cap how far behind the watermark can lag relative to the current time:

```java
// Custom TimestampPolicyFactory that caps lag at 5 minutes
KafkaIO.<Long, String>read()
    .withTimestampPolicyFactory((topicPartition, previousWatermark) ->
        TimestampPolicyFactory.withEndOfSourceWatermark(
            topicPartition,
            record -> new Instant(record.getTimestamp()),
            Duration.standardMinutes(5))); // max lag
```

### Fix 3: Custom UnboundedSource Watermark

For custom unbounded sources, ensure `getWatermark()` returns a reasonable advancing timestamp:

```java
public class MyUnboundedReader extends UnboundedSource.UnboundedReader<MyRecord> {
    private Instant currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

    @Override
    public boolean advance() throws IOException {
        MyRecord record = fetchNextRecord();
        if (record != null) {
            currentRecord = record;
            // Advance watermark to slightly behind the current record's timestamp
            currentWatermark = new Instant(record.getTimestampMillis())
                .minus(Duration.standardSeconds(30)); // 30s lag for out-of-order tolerance
            return true;
        }
        return false;
    }

    @Override
    public Instant getWatermark() {
        // Must advance monotonically; never go backwards
        return currentWatermark;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new MyCheckpointMark(currentRecord.getOffset());
    }
}
```

---

## 7. Deep Dive: Wrong Window Assignment

### Common Mistake: Windowing Before Timestamp Assignment

```java
// WRONG: Window.into runs first; elements use default timestamp (ingestion time or TIMESTAMP_MIN_VALUE)
rawEvents
    .apply("Window", Window.into(FixedWindows.of(Duration.standardMinutes(5))))
    .apply("AssignTs", WithTimestamps.of(event -> new Instant(event.getEventTime())));
// Elements are already windowed with wrong timestamps; reassignment has no effect on window assignment.

// CORRECT: always assign timestamps FIRST
rawEvents
    .apply("AssignTs",
        WithTimestamps.of((MyEvent event) -> new Instant(event.getEventTimeMillis()))
            .withAllowedTimestampSkew(Duration.standardMinutes(10))) // allow elements up to 10min old
    .apply("Window", Window.into(FixedWindows.of(Duration.standardMinutes(5))));
```

**Python:**
```python
# WRONG: window before timestamp assignment
events = (
    raw_events
    | beam.WindowInto(window.FixedWindows(5 * 60))  # uses default timestamp!
    | beam.Map(lambda e: beam.window.TimestampedValue(e, e["ts"] / 1000.0))  # too late
)

# CORRECT: assign timestamp first
import apache_beam as beam

def add_timestamp(element):
    return beam.window.TimestampedValue(element, element["ts"] / 1000.0)

events = (
    raw_events
    | "AssignTs" >> beam.Map(add_timestamp)   # FIRST
    | "Window"   >> beam.WindowInto(window.FixedWindows(5 * 60))  # THEN window
)
```
