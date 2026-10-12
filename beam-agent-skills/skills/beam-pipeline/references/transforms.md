
# Apache Beam Transforms Reference

Deep reference for all built-in and user-defined transforms in Apache Beam, with complete Java and Python code examples.

---

## 1. Core Built-in Transforms

### MapElements / beam.Map — 1-to-1 Mapping

Transforms each element into exactly one output element.

**Java:**
```java
// Map String to its length (use TypeDescriptors for lambda to avoid coder errors)
PCollection<Integer> lengths = words
    .apply("GetLength", MapElements
        .into(TypeDescriptors.integers())
        .via((String word) -> word.length()));

// Map KV to formatted string
PCollection<String> formatted = kvPairs
    .apply("Format", MapElements
        .into(TypeDescriptors.strings())
        .via((KV<String, Long> kv) -> kv.getKey() + "=" + kv.getValue()));
```

**Python:**
```python
# Map each element to uppercase
upper = words | "ToUpper" >> beam.Map(lambda word: word.upper())

# Map using a named function
def normalize(word):
    return word.strip().lower()

normalized = words | "Normalize" >> beam.Map(normalize)
```

---

### FlatMapElements / beam.FlatMap — 1-to-N Mapping

Transforms each element into zero or more output elements.

**Java:**
```java
// Tokenize lines into words
PCollection<String> words = lines
    .apply("Tokenize", FlatMapElements
        .into(TypeDescriptors.strings())
        .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))));

// Explode a CSV field into individual tags
PCollection<String> tags = records
    .apply("ExplodeTags", FlatMapElements
        .into(TypeDescriptors.strings())
        .via((String record) -> {
            String[] parts = record.split(",");
            return parts.length > 2 ? Arrays.asList(parts[2].split(";")) : Collections.emptyList();
        }));
```

**Python:**
```python
# Tokenize lines into words
words = lines | "Tokenize" >> beam.FlatMap(lambda line: line.split())

# Generator-based flat map
def split_csv_tags(record):
    parts = record.split(",")
    if len(parts) > 2:
        yield from parts[2].split(";")

tags = records | "ExplodeTags" >> beam.FlatMap(split_csv_tags)
```

---

### Filter — Retain Matching Elements

**Java:**
```java
// Keep only non-empty strings
PCollection<String> nonEmpty = strings
    .apply("FilterEmpty", Filter.by((String s) -> !s.isEmpty()));

// Keep words longer than 5 characters
PCollection<String> longWords = words
    .apply("FilterShort", Filter.by((String w) -> w.length() > 5));
```

**Python:**
```python
# Keep only non-empty strings
non_empty = strings | "FilterEmpty" >> beam.Filter(lambda s: s.strip())

# Keep words longer than 5 characters
long_words = words | "FilterShort" >> beam.Filter(lambda w: len(w) > 5)
```

---

### ParDo — General Element-Wise Processing

Use `ParDo` when you need: element access with context, multiple outputs, side inputs, lifecycle hooks (`@Setup`/`@Teardown`), or stateful/timely processing.

#### DoFn Lifecycle (Java)

```java
public class EnrichmentDoFn extends DoFn<String, EnrichedRecord> {

    // Initialized once per worker thread — use for non-serializable resources
    private transient HttpClient httpClient;
    private transient Cache<String, String> localCache;

    // Called once per worker instance (before any bundles)
    @Setup
    public void setup() {
        this.httpClient = HttpClient.newBuilder().build();
        this.localCache = CacheBuilder.newBuilder().maximumSize(10_000).build();
    }

    // Called at the start of each bundle (before processElement runs for the bundle)
    @StartBundle
    public void startBundle(StartBundleContext ctx) {
        // Reset per-bundle counters, open transactions, etc.
    }

    // Called once per element — the core processing logic
    @ProcessElement
    public void processElement(
            @Element String record,
            OutputReceiver<EnrichedRecord> out,
            ProcessContext ctx) {
        try {
            String enriched = localCache.getIfPresent(record);
            if (enriched == null) {
                enriched = httpClient.send(buildRequest(record), HttpResponse.BodyHandlers.ofString()).body();
                localCache.put(record, enriched);
            }
            out.output(new EnrichedRecord(record, enriched));
        } catch (Exception e) {
            // Silently skip or use MultiOutputReceiver to route to dead-letter
        }
    }

    // Called after all elements in a bundle are processed
    @FinishBundle
    public void finishBundle(FinishBundleContext ctx) {
        // Flush buffers, commit transactions
    }

    // Called once per worker (after all bundles on this worker are done)
    @Teardown
    public void teardown() {
        httpClient = null;
    }
}

// Usage:
PCollection<EnrichedRecord> enriched = records.apply("Enrich", ParDo.of(new EnrichmentDoFn()));
```

#### DoFn Lifecycle (Python)

```python
import apache_beam as beam
import requests

class EnrichmentDoFn(beam.DoFn):
    def setup(self):
        """Called once per worker process before any bundle processing."""
        self.session = requests.Session()

    def start_bundle(self):
        """Called before each bundle."""
        self.batch = []

    def process(self, record):
        """Called once per element. Yield output elements."""
        try:
            response = self.session.get(f"https://api.example.com/enrich?id={record}")
            yield {"input": record, "enriched": response.json()}
        except Exception:
            pass  # or yield TaggedOutput to dead-letter

    def finish_bundle(self):
        """Called after each bundle. Use for flushing buffers."""
        # Process remaining batch items
        pass

    def teardown(self):
        """Called once per worker after all bundles are done."""
        self.session.close()
```

---

### Count.perElement / Count.PerElement

```java
// Java: count occurrences of each unique element
PCollection<KV<String, Long>> counts = words.apply(Count.perElement());

// Java: count all elements globally
PCollection<Long> total = words.apply(Count.globally());
```

```python
# Python: count occurrences of each unique element
counts = words | beam.combiners.Count.PerElement()

# Python: count all elements globally
total = words | beam.combiners.Count.Globally()
```

---

### GroupByKey

Groups a `KV<K, V>` collection so all values for a given key are collected together.

> **Warning**: On unbounded (streaming) data, `GroupByKey` requires non-global windowing or an explicit trigger. The default `GlobalWindows` trigger never fires on unbounded data.

**Java:**
```java
// Input: PCollection<KV<String, Integer>> (e.g., ("alice", 10), ("bob", 5), ("alice", 3))
// Output: PCollection<KV<String, Iterable<Integer>>>
PCollection<KV<String, Iterable<Integer>>> grouped = kvPairs.apply(GroupByKey.create());

// Consume grouped result
PCollection<String> summary = grouped.apply("Summarize",
    MapElements.into(TypeDescriptors.strings())
        .via((KV<String, Iterable<Integer>> kv) -> {
            long sum = StreamSupport.stream(kv.getValue().spliterator(), false)
                .mapToLong(Integer::longValue).sum();
            return kv.getKey() + ":" + sum;
        }));
```

**Python:**
```python
# Input: PCollection of (key, value) tuples
# Output: PCollection of (key, [value1, value2, ...])
grouped = kv_pairs | beam.GroupByKey()

# Consume:
summary = grouped | beam.MapTuple(
    lambda key, values: f"{key}: {sum(values)}")
```

---

### Combine.perKey / CombinePerKey — Efficient Aggregation

Prefer `Combine.perKey` over `GroupByKey` when performing aggregations. Beam performs **combiner lifting** — partial aggregation happens before the shuffle, reducing network transfer.

**Java — Built-in Combiners:**
```java
// Sum of values per key
PCollection<KV<String, Long>> sums = kvLongs.apply(Sum.longsPerKey());

// Max value per key
PCollection<KV<String, Integer>> maxPerKey = kvInts.apply(Max.integersPerKey());

// Count per key
PCollection<KV<String, Long>> countPerKey = kvPairs.apply(Count.perKey());
```

**Java — Custom CombineFn (Average):**
```java
public class AverageFn extends Combine.CombineFn<Long, AverageFn.Accum, Double> {

    public static class Accum implements Serializable {
        long sum = 0L;
        long count = 0L;
    }

    @Override
    public Accum createAccumulator() { return new Accum(); }

    @Override
    public Accum addInput(Accum accum, Long input) {
        accum.sum += input;
        accum.count++;
        return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
        Accum merged = createAccumulator();
        for (Accum a : accums) {
            merged.sum += a.sum;
            merged.count += a.count;
        }
        return merged;
    }

    @Override
    public Double extractOutput(Accum accum) {
        return accum.count == 0 ? 0.0 : (double) accum.sum / accum.count;
    }
}

// Usage:
PCollection<KV<String, Double>> avgPerKey = kvLongs.apply(Combine.perKey(new AverageFn()));
```

**Python — Custom CombineFn (Average):**
```python
class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return (0, 0)  # (sum, count)

    def add_input(self, accumulator, input_val):
        s, c = accumulator
        return (s + input_val, c + 1)

    def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return (sum(sums), sum(counts))

    def extract_output(self, accumulator):
        s, c = accumulator
        return s / c if c > 0 else 0.0

# Usage:
avg_per_key = kv_longs | beam.CombinePerKey(AverageFn())
avg_globally = numbers | beam.CombineGlobally(AverageFn())
```

---

### CoGroupByKey — Join Two Datasets

Joins two `KV<K, V>` collections on matching key `K`.

**Java:**
```java
PCollection<KV<String, String>> emails = ...; // (userId, email)
PCollection<KV<String, Integer>> ages = ...;   // (userId, age)

final TupleTag<String> emailTag = new TupleTag<>();
final TupleTag<Integer> ageTag = new TupleTag<>();

PCollection<KV<String, CoGbkResult>> coGrouped = KeyedPCollectionTuple
    .of(emailTag, emails)
    .and(ageTag, ages)
    .apply(CoGroupByKey.create());

PCollection<String> joined = coGrouped.apply("JoinResults",
    ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
      @ProcessElement
      public void processElement(@Element KV<String, CoGbkResult> element, OutputReceiver<String> out) {
          String userId = element.getKey();
          CoGbkResult result = element.getValue();
          Iterable<String> userEmails = result.getAll(emailTag);
          Iterable<Integer> userAges = result.getAll(ageTag);
          for (String email : userEmails) {
              for (Integer age : userAges) {
                  out.output(userId + "," + email + "," + age);
              }
          }
      }
    }));
```

**Python:**
```python
emails = p | "ReadEmails" >> beam.Create([("user1", "a@b.com"), ("user2", "c@d.com")])
ages   = p | "ReadAges"   >> beam.Create([("user1", 30), ("user2", 25)])

joined = {"emails": emails, "ages": ages} | beam.CoGroupByKey()

def format_join(element):
    user_id, data = element
    for email in data["emails"]:
        for age in data["ages"]:
            yield f"{user_id},{email},{age}"

output = joined | beam.FlatMap(format_join)
```

---

### Flatten — Union Collections

Merges multiple `PCollection<T>` of the same type.

**Java:**
```java
PCollection<String> a = ...;
PCollection<String> b = ...;
PCollection<String> merged = PCollectionList.of(a).and(b).apply(Flatten.pCollections());
```

**Python:**
```python
a = p | "CreateA" >> beam.Create(["a1", "a2"])
b = p | "CreateB" >> beam.Create(["b1", "b2"])
merged = (a, b) | beam.Flatten()
```

---

### Partition — Split into N Buckets

Splits a `PCollection` into a fixed number of sub-collections based on a function.

**Java:**
```java
// Split numbers into 3 buckets: [0,10), [10,100), [100+)
PCollectionList<Integer> partitioned = numbers.apply(
    Partition.of(3, (Integer n, int numPartitions) -> {
        if (n < 10) return 0;
        if (n < 100) return 1;
        return 2;
    }));

PCollection<Integer> small = partitioned.get(0);
PCollection<Integer> medium = partitioned.get(1);
PCollection<Integer> large = partitioned.get(2);
```

**Python:**
```python
def bucket_fn(element, num_partitions):
    if element < 10:
        return 0
    elif element < 100:
        return 1
    else:
        return 2

partitioned = numbers | beam.Partition(bucket_fn, 3)
small  = partitioned[0]
medium = partitioned[1]
large  = partitioned[2]
```

---

## 2. Side Inputs

Side inputs allow a `DoFn` to access a secondary (usually small) dataset alongside each element.

**Java — Side Input as Map (Lookup Table):**
```java
// Build a PCollectionView (materialized side input)
PCollectionView<Map<String, String>> countryView = countryCodes
    .apply("ViewAsMap", View.asMap());

// Use in ParDo
PCollection<EnrichedRecord> enriched = records.apply("Enrich",
    ParDo.of(new DoFn<String, EnrichedRecord>() {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
            Map<String, String> countries = ctx.sideInput(countryView);
            String code = parseCountryCode(ctx.element());
            String name = countries.getOrDefault(code, "Unknown");
            ctx.output(new EnrichedRecord(ctx.element(), name));
        }
    }).withSideInputs(countryView));
```

**Java — Side Input as Singleton:**
```java
PCollectionView<Double> thresholdView = thresholds
    .apply("ViewAsSingleton", View.asSingleton());

PCollection<String> filtered = records.apply("FilterByThreshold",
    ParDo.of(new DoFn<Record, String>() {
        @ProcessElement
        public void processElement(ProcessContext ctx) {
            double threshold = ctx.sideInput(thresholdView);
            if (ctx.element().value() > threshold) {
                ctx.output(ctx.element().id());
            }
        }
    }).withSideInputs(thresholdView));
```

**Python — Side Input:**
```python
# Small lookup PCollection
country_dict = p | "Countries" >> beam.Create([("US", "United States"), ("UK", "United Kingdom")])

def enrich(record, lookup):
    code = record.split(",")[2] if len(record.split(",")) > 2 else ""
    country = lookup.get(code, "Unknown")
    return f"{record},{country}"

enriched = records | beam.Map(enrich, lookup=beam.pvalue.AsDict(country_dict))

# As singleton:
threshold = p | "Threshold" >> beam.Create([100.0])
filtered = records | beam.Filter(
    lambda r, t: float(r.split(",")[1]) > t,
    t=beam.pvalue.AsSingleton(threshold))
```

---

## 3. Multiple Outputs / Dead-Letter Queue

Route elements to different outputs based on processing outcome.

**Java:**
```java
final TupleTag<ParsedRecord> SUCCESS_TAG  = new TupleTag<ParsedRecord>() {};
final TupleTag<String>        FAILURE_TAG = new TupleTag<String>() {};

PCollectionTuple results = rawLines.apply("ParseWithDLQ",
    ParDo.of(new DoFn<String, ParsedRecord>() {
        @ProcessElement
        public void processElement(@Element String line, MultiOutputReceiver out) {
            try {
                ParsedRecord parsed = parseRecord(line);
                out.get(SUCCESS_TAG).output(parsed);
            } catch (Exception e) {
                out.get(FAILURE_TAG).output(line + " | ERROR: " + e.getMessage());
            }
        }
    }).withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));

results.get(SUCCESS_TAG).apply("ProcessValid", ...);
results.get(FAILURE_TAG).apply("WriteDLQ", TextIO.write().to("gs://bucket/dlq/errors"));
```

**Python:**
```python
class ParseWithDLQ(beam.DoFn):
    ERROR_TAG = 'errors'

    def process(self, line):
        try:
            yield parse_record(line)
        except Exception as e:
            yield beam.pvalue.TaggedOutput(self.ERROR_TAG, f"{line} | {e}")

results = raw_lines | beam.ParDo(ParseWithDLQ()).with_outputs(
    ParseWithDLQ.ERROR_TAG, main='parsed')

results.parsed | "ProcessValid" >> ...
results.errors | "WriteDLQ"    >> beam.io.WriteToText("gs://bucket/dlq/errors")
```

---

## 4. Composite Transforms

Package a sub-pipeline into a reusable, named transform.

**Java:**
```java
public class NormalizeAndCount extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> input) {
        return input
            .apply("Tokenize", FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.toLowerCase().split("[^a-z]+"))))
            .apply("FilterEmpty", Filter.by((String w) -> w.length() >= 2))
            .apply("CountPerWord", Count.perElement());
    }
}

// Usage — appears as a single node in the execution graph:
PCollection<KV<String, Long>> wordCounts = lines.apply(new NormalizeAndCount());
```

**Python:**
```python
class NormalizeAndCount(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "Tokenize"    >> beam.FlatMap(lambda line: [w for w in line.lower().split() if len(w) >= 2])
            | "CountPerWord" >> beam.combiners.Count.PerElement()
        )

# Usage:
word_counts = lines | NormalizeAndCount()
```

---

## 5. Common Anti-Patterns

### 5.1 Using Regular Collections Instead of PCollections
```java
// WRONG — collecting to a list defeats parallelism and breaks distribution
List<String> all = new ArrayList<>();
rawData.apply(ParDo.of(new DoFn<String, Void>() {
    @ProcessElement
    public void processElement(@Element String s) {
        all.add(s); // NOT THREAD SAFE; breaks on distributed runners
    }
}));

// CORRECT — use pipeline transforms to process data in PCollections
PCollection<String> filtered = rawData.apply(Filter.by(s -> s.length() > 0));
```

### 5.2 Mutable State Across Elements in `@ProcessElement`
```java
// WRONG — static or instance mutable state retained across elements is not safe
public class BrokenDoFn extends DoFn<String, String> {
    private List<String> buffer = new ArrayList<>(); // retained across elements in same bundle

    @ProcessElement
    public void process(@Element String s, OutputReceiver<String> out) {
        buffer.add(s);            // This may work locally but fails unpredictably
        if (buffer.size() >= 10) {
            out.output(String.join(",", buffer));
            buffer.clear();
        }
    }
}

// CORRECT — use @StartBundle / @FinishBundle to scope state to bundles
public class BatchingDoFn extends DoFn<String, String> {
    private List<String> buffer;

    @StartBundle
    public void startBundle() { buffer = new ArrayList<>(); }

    @ProcessElement
    public void process(@Element String s) { buffer.add(s); }

    @FinishBundle
    public void finishBundle(FinishBundleContext ctx) {
        if (!buffer.isEmpty()) {
            ctx.output(String.join(",", buffer), Instant.now(), GlobalWindow.INSTANCE);
        }
    }
}
```

### 5.3 Non-Static Inner DoFn Classes (Java)
```java
// WRONG — captures outer class 'this', which is often not serializable
public class MyTransform extends PTransform<...> {
    private String prefix = "prefix_";

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void process(@Element String s, OutputReceiver<String> out) {
                out.output(prefix + s); // captures outer 'this.prefix' — serialization error
            }
        }));
    }
}

// CORRECT — static inner class with explicit field copy
public class MyTransform extends PTransform<...> {
    private final String prefix;

    public MyTransform(String prefix) { this.prefix = prefix; }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input.apply(ParDo.of(new PrefixFn(prefix)));
    }

    private static class PrefixFn extends DoFn<String, String> {
        private final String prefix;
        PrefixFn(String prefix) { this.prefix = prefix; }

        @ProcessElement
        public void process(@Element String s, OutputReceiver<String> out) {
            out.output(prefix + s);
        }
    }
}
```
