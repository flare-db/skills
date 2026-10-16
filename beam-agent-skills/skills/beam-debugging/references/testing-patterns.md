# Beam Pipeline: Testing Patterns Reference

Complete guide for unit testing, streaming testing, and integration testing Apache Beam pipelines in Java and Python.

> **Python first.** Test new Python transforms with `TestPipeline` + `assert_that`
> under DirectRunner. A FlareDB trial is execution, not a unit test — it belongs to
> `flaredb-skills` and requires its approval gate.

---

## 1. Unit Testing Strategy

The recommended Beam testing workflow:

1. **Use `TestPipeline`** — a special pipeline that runs with the `DirectRunner` and captures errors during testing.
2. **Create test input** with `Create.of(...)` — inject in-memory data without needing real I/O.
3. **Apply the transform** under test.
4. **Assert outputs** using `PAssert` (Java) or `assert_that` (Python).
5. **Run the pipeline** — `pipeline.run().waitUntilFinish()` or the `with TestPipeline as p:` block.

---

## 2. Java Unit Test Patterns

### 2.1 Basic Transform Test

```java
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.Serializable;
import java.util.Arrays;

public class MyTransformTest implements Serializable {

    // TestPipeline must be a @Rule — it handles pipeline lifecycle and validation
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testFilterShortWords() {
        // 1. Create test input in-memory
        PCollection<String> input = pipeline.apply(
            Create.of("hello", "hi", "world", "a", "beam"));

        // 2. Apply the transform under test
        PCollection<String> output = input
            .apply("Filter", Filter.by((String w) -> w.length() > 2));

        // 3. Assert the expected output (order does not matter)
        PAssert.that(output).containsInAnyOrder("hello", "world", "beam");

        // 4. Run — this executes the pipeline and validates assertions
        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testWordCountTransform() {
        PCollection<String> words = pipeline.apply(
            Create.of("apple", "banana", "apple", "cherry", "banana", "apple"));

        PCollection<String> formatted = words
            .apply("Count", org.apache.beam.sdk.transforms.Count.perElement())
            .apply("Format",
                MapElements.into(TypeDescriptors.strings())
                    .via(kv -> kv.getKey() + ":" + kv.getValue()));

        PAssert.that(formatted).containsInAnyOrder(
            "apple:3", "banana:2", "cherry:1");

        pipeline.run().waitUntilFinish();
    }
}
```

### 2.2 Testing a Composite Transform

```java
// Composite transform to test
public class NormalizeWords extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input
            .apply("Lowercase", MapElements.into(TypeDescriptors.strings())
                .via(String::toLowerCase))
            .apply("Trim", MapElements.into(TypeDescriptors.strings())
                .via(String::trim))
            .apply("FilterEmpty", Filter.by((String s) -> !s.isEmpty()));
    }
}

// Test:
public class NormalizeWordsTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testNormalization() {
        PCollection<String> input = pipeline.apply(
            Create.of("  Hello  ", "WORLD", "", "  ", "Beam"));

        PCollection<String> output = input.apply(new NormalizeWords());

        PAssert.that(output).containsInAnyOrder("hello", "world", "beam");
        pipeline.run().waitUntilFinish();
    }
}
```

### 2.3 Testing DoFn with Multiple Outputs (TupleTag)

```java
public class ParseDoFnTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    // The DoFn under test
    static final TupleTag<Integer> SUCCESS_TAG = new TupleTag<Integer>() {};
    static final TupleTag<String> ERROR_TAG    = new TupleTag<String>() {};

    static class ParseIntDoFn extends DoFn<String, Integer> {
        @ProcessElement
        public void process(@Element String s, MultiOutputReceiver out) {
            try {
                out.get(SUCCESS_TAG).output(Integer.parseInt(s.trim()));
            } catch (NumberFormatException e) {
                out.get(ERROR_TAG).output("INVALID:" + s);
            }
        }
    }

    @Test
    @Category(NeedsRunner.class)
    public void testParseWithDLQ() {
        PCollection<String> input = pipeline.apply(
            Create.of("42", "abc", "100", "xyz", "7"));

        PCollectionTuple results = input.apply(
            ParDo.of(new ParseIntDoFn())
                .withOutputTags(SUCCESS_TAG, TupleTagList.of(ERROR_TAG)));

        PAssert.that(results.get(SUCCESS_TAG)).containsInAnyOrder(42, 100, 7);
        PAssert.that(results.get(ERROR_TAG)).containsInAnyOrder("INVALID:abc", "INVALID:xyz");

        pipeline.run().waitUntilFinish();
    }
}
```

### 2.4 PAssert API Reference

```java
// Check exact contents (order independent)
PAssert.that(pcoll).containsInAnyOrder("a", "b", "c");

// Check empty collection
PAssert.that(pcoll).empty();

// Check single element
PAssert.that(pcoll).containsInAnyOrder(42L);

// Custom predicate check
PAssert.that(pcoll).satisfies(elements -> {
    List<String> list = StreamSupport.stream(elements.spliterator(), false)
        .collect(Collectors.toList());
    assertEquals(3, list.size());
    assertTrue(list.stream().allMatch(s -> s.startsWith("prefix_")));
    return null;  // must return null
});

// Check a PCollection<Long> has a single value == 5
PAssert.thatSingleton(longPcoll).isEqualTo(5L);

// Assert that windowed data matches per-window
PAssert.that(windowed)
    .inWindow(new IntervalWindow(new Instant(0), new Instant(5 * 60 * 1000)))
    .containsInAnyOrder("a", "b");
```

---

## 3. Python Unit Test Patterns

### 3.1 Basic Transform Test

```python
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_empty


class FilterShortWordsTest(unittest.TestCase):

    def test_filter_short_words(self):
        input_data = ["hello", "hi", "world", "a", "beam"]
        expected  = ["hello", "world", "beam"]

        with TestPipeline() as p:
            output = (
                p
                | "Create"  >> beam.Create(input_data)
                | "Filter"  >> beam.Filter(lambda w: len(w) > 2)
            )
            assert_that(output, equal_to(expected))

    def test_word_count(self):
        with TestPipeline() as p:
            words = p | beam.Create(["apple", "banana", "apple", "cherry", "banana", "apple"])
            counts = (
                words
                | beam.combiners.Count.PerElement()
                | beam.Map(lambda wc: f"{wc[0]}:{wc[1]}")
            )
            assert_that(counts, equal_to(["apple:3", "banana:2", "cherry:1"]))

    def test_empty_output(self):
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(["a", "b", "c"])
                | beam.Filter(lambda w: len(w) > 10)  # nothing passes
            )
            assert_that(output, is_empty())


if __name__ == "__main__":
    unittest.main()
```

### 3.2 Testing DoFn with Multiple Outputs

```python
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import unittest

class ParseIntDoFn(beam.DoFn):
    OUTPUT_TAG_ERRORS = 'errors'

    def process(self, element):
        try:
            yield int(element.strip())
        except (ValueError, AttributeError) as e:
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_ERRORS, f"INVALID:{element}")


class ParseDoFnTest(unittest.TestCase):

    def test_parse_with_dlq(self):
        with TestPipeline() as p:
            results = (
                p
                | beam.Create(["42", "abc", "100", "xyz", "7"])
                | beam.ParDo(ParseIntDoFn()).with_outputs(
                    ParseIntDoFn.OUTPUT_TAG_ERRORS, main='parsed')
            )
            assert_that(results.parsed, equal_to([42, 100, 7]))
            assert_that(results.errors, equal_to(["INVALID:abc", "INVALID:xyz"]))
```

### 3.3 assert_that Matchers

```python
from apache_beam.testing.util import (
    assert_that,
    equal_to,
    is_empty,
    matches_all,
)

# Exact match (order independent):
assert_that(pcoll, equal_to([1, 2, 3]))

# Empty collection:
assert_that(pcoll, is_empty())

# Custom predicate:
def all_positive(elements):
    for e in elements:
        if e <= 0:
            raise AssertionError(f"Found non-positive element: {e}")

assert_that(pcoll, all_positive)

# Per-window assertions (for windowed output):
from apache_beam.transforms.window import IntervalWindow
from apache_beam.testing.util import assert_that, equal_to

assert_that(
    windowed_pcoll,
    equal_to(["a", "b"]),
    label="window0",
    reify_windows=True
)
```

---

## 4. Streaming Tests with TestStream

`TestStream` lets you inject elements at specific event-time timestamps and advance the watermark deterministically — no wall-clock sleeps needed.

### 4.1 Java TestStream

```java
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowedAggregationTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testFixedWindowAggregation() {
        Instant baseTime = new Instant(0);

        // Build a controlled stream of events
        TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
            // Add elements in window [0, 5min)
            .addElements(
                TimestampedValue.of("apple", baseTime),
                TimestampedValue.of("banana", baseTime.plus(Duration.standardMinutes(1))),
                TimestampedValue.of("apple", baseTime.plus(Duration.standardMinutes(2))))
            // Advance watermark past end of first window → triggers ON_TIME pane
            .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(6)))
            // Add a late element for the first window (within allowed lateness)
            .addElements(
                TimestampedValue.of("apple", baseTime.plus(Duration.standardMinutes(3))))
            // Advance watermark to infinity → all windows close
            .advanceWatermarkToInfinity();

        PCollection<String> output = pipeline
            .apply(stream)
            .apply("Window",
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                    .triggering(AfterWatermark.pastEndOfWindow()
                        .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.standardMinutes(5)))
            .apply("CountPerElement", Count.perElement())
            .apply("Format",
                MapElements.into(TypeDescriptors.strings())
                    .via(kv -> kv.getKey() + ":" + kv.getValue()));

        // After ON_TIME pane fires: apple=2, banana=1
        // After late pane fires: apple=3, banana=1 (accumulating mode)
        PAssert.that(output)
            .inFinalPane(new IntervalWindow(baseTime, baseTime.plus(Duration.standardMinutes(5))))
            .containsInAnyOrder("apple:3", "banana:1");

        pipeline.run().waitUntilFinish();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testEarlyFiringEmitsSpeculativeResult() {
        Instant baseTime = new Instant(0);

        TestStream<String> stream = TestStream.create(StringUtf8Coder.of())
            .addElements(TimestampedValue.of("x", baseTime))
            // Advance processing time (not event time) to trigger early firing
            .advanceProcessingTime(Duration.standardSeconds(35))
            .addElements(TimestampedValue.of("y", baseTime))
            .advanceWatermarkToInfinity();

        PCollection<Long> counts = pipeline
            .apply(stream)
            .apply("Window",
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                    .triggering(AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(AfterProcessingTime.pastFirstElement()
                            .plusDelayOf(Duration.standardSeconds(30))))
                    .discardingFiredPanes())
            .apply(Count.globally().withoutDefaults());

        // Early pane should fire after 30s, containing just "x"
        PAssert.that(counts).containsInAnyOrder(1L, 1L); // early(1) + on-time(1) in discarding mode

        pipeline.run().waitUntilFinish();
    }
}
```

### 4.2 Python TestStream

```python
import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms import window
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AccumulationMode
from apache_beam.transforms.window import TimestampedValue


class StreamingWindowTest(unittest.TestCase):

    def test_fixed_window_aggregation(self):
        # Build a controlled stream of events with explicit timestamps
        test_stream = (
            TestStream()
            # Elements at event-time t=0 and t=60s (both in [0, 300s) window)
            .add_elements([
                TimestampedValue("apple", 0),
                TimestampedValue("banana", 60),
                TimestampedValue("apple", 120),
            ])
            # Advance watermark past window end [0, 300s) → ON_TIME pane fires
            .advance_watermark_to(310)
            # Late element for the same window
            .add_elements([TimestampedValue("apple", 90)])
            # Close all windows
            .advance_watermark_to_infinity()
        )

        with TestPipeline() as p:
            output = (
                p
                | test_stream
                | "Window" >> beam.WindowInto(
                    window.FixedWindows(300),  # 5-minute windows
                    trigger=AfterWatermark(late=AfterCount(1)),
                    accumulation_mode=AccumulationMode.ACCUMULATING,
                    allowed_lateness=300
                )
                | "Count" >> beam.combiners.Count.PerElement()
                | "Format" >> beam.Map(lambda wc: f"{wc[0]}:{wc[1]}")
            )
            # After all panes: apple appears 3 times (2 on-time + 1 late, accumulating)
            assert_that(output, equal_to(["apple:3", "banana:1"]))

    def test_session_window(self):
        """Test that session windows correctly group events by inactivity gap."""
        test_stream = (
            TestStream()
            # Session 1 for user "alice": events at t=0 and t=60s (within 120s gap)
            .add_elements([
                TimestampedValue(("alice", "click"), 0),
                TimestampedValue(("alice", "scroll"), 60),
            ])
            # Gap > 120s — session should close for alice
            .add_elements([
                TimestampedValue(("alice", "purchase"), 300),  # new session
            ])
            .advance_watermark_to_infinity()
        )

        with TestPipeline() as p:
            sessions = (
                p
                | test_stream
                | "Window" >> beam.WindowInto(window.Sessions(120))  # 2-min gap
                | "Group"  >> beam.GroupByKey()
                | "CountEvents" >> beam.Map(lambda kv: (kv[0], len(list(kv[1]))))
            )
            # Session 1: alice with 2 events; Session 2: alice with 1 event
            assert_that(sessions, equal_to([("alice", 2), ("alice", 1)]))


if __name__ == "__main__":
    unittest.main()
```

---

## 5. Testing Windowed Output with `inWindow` / `inFinalPane`

### Java — Per-Window PAssert

```java
// Define expected window boundaries
IntervalWindow window1 = new IntervalWindow(
    new Instant(0),
    new Instant(5 * 60 * 1000));  // [0, 5min)

IntervalWindow window2 = new IntervalWindow(
    new Instant(5 * 60 * 1000),
    new Instant(10 * 60 * 1000)); // [5min, 10min)

// Assert per window
PAssert.that(output)
    .inWindow(window1)
    .containsInAnyOrder("a", "b", "c");

PAssert.that(output)
    .inWindow(window2)
    .containsInAnyOrder("d", "e");

// Assert in the FINAL pane only (after all late firings)
PAssert.that(output)
    .inFinalPane(window1)
    .containsInAnyOrder("a", "b", "c", "late_element");

// Assert in ON_TIME pane only
PAssert.that(output)
    .inOnTimePane(window1)
    .containsInAnyOrder("a", "b", "c");
```

---

## 6. Integration Tests

Integration tests run the pipeline against real sources/sinks in a test environment.

### 6.1 Java Integration Test Pattern

```java
/**
 * Integration test that runs against DirectRunner with real temp files.
 * Annotate with @Category(ValidatesRunner.class) to run against multiple runners.
 */
@RunWith(JUnit4.class)
@Category(ValidatesRunner.class)
public class WordCountIntegrationTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testEndToEnd() throws Exception {
        // Write test input to a temp file
        File inputFile = tempFolder.newFile("input.txt");
        Files.writeString(inputFile.toPath(),
            "hello world\nhello beam\nworld beam beam\n");

        File outputDir = tempFolder.newFolder("output");
        String outputPrefix = outputDir.getPath() + "/result";

        // Run the actual WordCount pipeline
        WordCountOptions options = PipelineOptionsFactory.as(WordCountOptions.class);
        options.setInputFile(inputFile.getPath());
        options.setOutputFile(outputPrefix);

        Pipeline p = Pipeline.create(options);
        WordCountPipeline.buildPipeline(p, options).run().waitUntilFinish();

        // Read all output shards and verify
        List<String> lines = Files.list(outputDir.toPath())
            .filter(path -> path.getFileName().toString().startsWith("result"))
            .flatMap(path -> {
                try { return Files.lines(path); }
                catch (IOException e) { throw new RuntimeException(e); }
            })
            .sorted()
            .collect(Collectors.toList());

        assertThat(lines).containsExactlyInAnyOrder(
            "beam: 3", "hello: 2", "world: 2");
    }
}
```

### 6.2 Python Integration Test Pattern

```python
import os
import tempfile
import unittest
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class WordCountIntegrationTest(unittest.TestCase):

    def test_end_to_end(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input.txt")
            output_prefix = os.path.join(tmpdir, "output/wordcount")

            # Write test input
            with open(input_path, "w") as f:
                f.write("hello world\nhello beam\nworld beam beam\n")

            # Run the pipeline
            options = PipelineOptions([
                "--runner=DirectRunner",
                f"--temp_location={tmpdir}/tmp"
            ])

            with beam.Pipeline(options=options) as p:
                (
                    p
                    | beam.io.ReadFromText(input_path)
                    | beam.FlatMap(lambda line: line.split())
                    | beam.combiners.Count.PerElement()
                    | beam.Map(lambda wc: f"{wc[0]}: {wc[1]}")
                    | beam.io.WriteToText(output_prefix)
                )

            # Read all output shards
            output_dir = os.path.dirname(output_prefix)
            all_lines = []
            for fname in os.listdir(output_dir):
                if fname.startswith("wordcount"):
                    with open(os.path.join(output_dir, fname)) as f:
                        all_lines.extend(f.read().splitlines())

            self.assertCountEqual(all_lines, ["beam: 3", "hello: 2", "world: 2"])


if __name__ == "__main__":
    unittest.main()
```

---

## 7. Running Tests Locally

### Java

```bash
# Run all unit tests for a specific module
./gradlew :sdks:java:core:test

# Run a specific test class
./gradlew :examples:java:test --tests "org.apache.beam.examples.MyTransformTest"

# Run all tests matching a pattern
./gradlew :examples:java:test --tests "org.apache.beam.examples.*Test"

# Run with verbose output
./gradlew :examples:java:test --tests "org.apache.beam.examples.MyTransformTest" --info

# Run integration tests (those annotated with @Category(ValidatesRunner.class))
./gradlew :examples:java:integrationTest -DtargetRunner=direct
```

### Python

```bash
# Install Beam and test dependencies (in a virtual environment)
pip install apache-beam[gcp] pytest

# Run a specific test file
pytest tests/my_transform_test.py -v

# Run all test files matching a pattern
pytest tests/ -k "test_" -v

# Run with coverage
pip install pytest-cov
pytest tests/ --cov=my_pipeline --cov-report=term-missing

# Run a specific test method
pytest tests/my_transform_test.py::MyTransformTest::test_filter_short_words -v
```

---

## 8. Common Testing Mistakes

| Mistake | Consequence | Fix |
|---|---|---|
| Not calling `pipeline.run()` in Java | Assertions never executed; test always passes | Always call `pipeline.run().waitUntilFinish()` after `PAssert.that()` |
| Calling `TestPipeline.create()` without `@Rule` (Java) | Pipeline not validated; test may pass incorrectly | Use `@Rule public final transient TestPipeline pipeline = TestPipeline.create();` |
| Using real I/O (TextIO, BigQueryIO) in unit tests | Tests are slow, flaky, and require credentials | Use `Create.of(...)` for input; capture output with `PAssert`; mock I/O |
| Testing with `GlobalWindows` but expecting windowed assertions | All elements collapse into one global window; per-window assertions fail | Use `FixedWindows` and inject elements with `TestStream` for streaming tests |
| `assert_that` not inside `with TestPipeline() as p:` block (Python) | Assertions may not be registered; test always passes | Keep all `assert_that` calls inside the `with` block |
| Testing only happy path (no error/null inputs) | Real pipelines fail on malformed data not tested | Include invalid inputs; test DLQ / error outputs too |
