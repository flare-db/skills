
# Apache Beam Windowing Reference

Complete reference for windowing strategies, triggers, accumulation modes, and allowed lateness in Apache Beam.

---

## 1. Why Windowing Exists

Apache Beam processes elements in `PCollection`s. For **bounded** (batch) data the entire collection is finite and can be processed as one group. For **unbounded** (streaming) data, grouping operations like `GroupByKey` and `Combine.perKey` cannot wait for all data before emitting results — the data never ends.

**Windowing** solves this by dividing an unbounded stream into finite chunks called _windows_ based on element timestamps. Aggregations operate within each window separately.

### Two Notions of Time

| Time | Definition | Who controls it |
|---|---|---|
| **Event time** | Timestamp embedded in the data (e.g., when the event occurred on the device) | Application / data producer |
| **Processing time** | Wall-clock time on the Beam worker when the element is processed | The runner/worker |

Beam's data model is primarily **event-time** based. Elements carry event timestamps, and windowing is defined in event time by default.

### Watermarks

The **watermark** is the runner's estimate of "how far along in event time we are." When the watermark advances past timestamp `T`, the runner considers all elements with `event_time <= T` to have been received. Windows ending at or before `T` can then fire their results.

- Late data arrives after the watermark has passed the element's window boundary.
- By default, late data is **silently dropped** (zero allowed lateness).
- Use `.withAllowedLateness(Duration)` to accept and process late arrivals.

---

## 2. Window Types

### 2.1 Fixed (Tumbling) Windows

Non-overlapping, equal-duration windows. Every element belongs to exactly one window.

```
Event time: ----[0--5min)----[5--10min)----[10--15min)---->
```

**Java:**
```java
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

PCollection<KV<String, Long>> windowed = input
    .apply("Window5Min", Window.into(FixedWindows.of(Duration.standardMinutes(5))));
```

**Python:**
```python
import apache_beam as beam
from apache_beam.transforms import window

windowed = input | "Window5Min" >> beam.WindowInto(window.FixedWindows(5 * 60))
```

---

### 2.2 Sliding (Hopping) Windows

Overlapping windows — every window has the same duration, and a new window starts every `period` interval. An element can belong to multiple windows.

```
Event time:
Window 1: [0-10min)
Window 2:   [5-15min)
Window 3:     [10-20min)
```

Use case: "rolling 10-minute average, updated every 1 minute."

**Java:**
```java
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;

PCollection<KV<String, Long>> windowed = input
    .apply("Sliding10Every1",
        Window.into(SlidingWindows.of(Duration.standardMinutes(10))
            .every(Duration.standardMinutes(1))));
```

**Python:**
```python
windowed = input | beam.WindowInto(
    window.SlidingWindows(size=10 * 60, period=60))
```

---

### 2.3 Session Windows

Variable-length, per-key windows defined by activity gaps. A session ends when no data is seen for a key during the _gap duration_.

Use case: "group all page views for a user into a session; a session ends after 30 minutes of inactivity."

**Java:**
```java
import org.apache.beam.sdk.transforms.windowing.Sessions;

// Input must be KV<K, V> for session windows to be meaningful (they're per-key)
PCollection<KV<String, Long>> sessions = kvInput
    .apply("Sessions30Min", Window.into(Sessions.withGapDuration(Duration.standardMinutes(30))));
```

**Python:**
```python
sessions = kv_input | beam.WindowInto(window.Sessions(30 * 60))
```

---

### 2.4 Global Windows

The default window — all elements belong to one infinite window. Useful for batch processing or when combined with a custom trigger for streaming.

> [!WARNING]
> **The default trigger on `GlobalWindows` for streaming data waits for the watermark to reach positive infinity — which never happens on unbounded data.** Always pair `GlobalWindows` with an explicit trigger if using it for streaming aggregations.

**Java:**
```java
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;

PCollection<String> global = input.apply(
    Window.<String>into(new GlobalWindows())
        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(100)))
        .discardingFiredPanes());
```

**Python:**
```python
from apache_beam.transforms.trigger import Repeatedly, AfterCount, AccumulationMode

global_window = input | beam.WindowInto(
    window.GlobalWindows(),
    trigger=Repeatedly(AfterCount(100)),
    accumulation_mode=AccumulationMode.DISCARDING)
```

---

## 3. Triggers

Triggers control _when_ results for a window are emitted. A window can emit multiple times (called _panes_). The default trigger fires exactly once: when the watermark passes the window end.

### 3.1 AfterWatermark — Event-Time Trigger

Fires when the watermark passes the end of the window (the ON_TIME pane). Supports optional early and late firings.

**Java:**
```java
import org.apache.beam.sdk.transforms.windowing.*;

Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(
        AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(
                AfterProcessingTime.pastFirstElement()
                    .plusDelayOf(Duration.standardSeconds(30)))
            .withLateFirings(AfterPane.elementCountAtLeast(1)))
    .accumulatingFiredPanes()
    .withAllowedLateness(Duration.standardHours(1));
```

**Python:**
```python
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode
)

beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterWatermark(
        early=AfterProcessingTime(30),
        late=AfterCount(1)
    ),
    accumulation_mode=AccumulationMode.ACCUMULATING,
    allowed_lateness=3600
)
```

---

### 3.2 AfterProcessingTime — Wall-Clock Trigger

Fires after a delay measured in processing (wall-clock) time.

**Java:**
```java
// Fire 1 minute after the first element in the pane arrives (processing time)
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterProcessingTime.pastFirstElement().plusDelayOf(Duration.standardMinutes(1)))
    .discardingFiredPanes();
```

**Python:**
```python
from apache_beam.transforms.trigger import AfterProcessingTime

beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterProcessingTime(60),
    accumulation_mode=AccumulationMode.DISCARDING
)
```

---

### 3.3 AfterCount — Element-Count Trigger

Fires when at least N elements have arrived in the window pane.

**Java:**
```java
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterPane.elementCountAtLeast(50))
    .discardingFiredPanes();
```

**Python:**
```python
from apache_beam.transforms.trigger import AfterCount

beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterCount(50),
    accumulation_mode=AccumulationMode.DISCARDING
)
```

---

### 3.4 Composite Triggers

**Repeatedly — Repeat a Trigger Forever:**
```java
// Java: fire every 30 seconds indefinitely
.triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElement().plusDelayOf(Duration.standardSeconds(30))))
```

```python
from apache_beam.transforms.trigger import Repeatedly

trigger=Repeatedly(AfterProcessingTime(30))
```

**AfterFirst — Whichever Fires First:**
```java
// Java: fire at watermark OR after 100 elements, whichever comes first
.triggering(AfterFirst.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(100)))
```

```python
from apache_beam.transforms.trigger import AfterAny  # equivalent to AfterFirst

trigger=AfterAny(AfterWatermark(), AfterCount(100))
```

**OrFinally — Primary trigger, stop on secondary:**
```java
// Java: fire every 30s processing time, but stop as soon as watermark fires
.triggering(
    Repeatedly.forever(AfterProcessingTime.pastFirstElement().plusDelayOf(Duration.standardSeconds(30)))
        .orFinally(AfterWatermark.pastEndOfWindow()))
```

---

## 4. Accumulation Modes

When a window fires multiple panes (e.g., early speculative + on-time + late), accumulation mode controls what each pane contains.

### ACCUMULATING
Each new pane includes **all elements seen so far** in the window. Downstream consumers receive the full running total; they must be able to handle repeated/updated values.

| Pane | Elements Arrived | Elements in Pane Output |
|---|---|---|
| Early (at 30s) | [a, b] | [a, b] |
| On-Time | [a, b, c] | [a, b, c] |
| Late | [a, b, c, d] | [a, b, c, d] |

Use when the consumer can overwrite results (e.g., updating a BigQuery table by key).

### DISCARDING
Each pane contains **only elements that arrived since the last firing**. Downstream consumers receive deltas.

| Pane | Elements Arrived | Elements in Pane Output |
|---|---|---|
| Early (at 30s) | [a, b] | [a, b] |
| On-Time | [c] (since early) | [c] |
| Late | [d] | [d] |

Use when the consumer accumulates values itself (e.g., appending to a log, or incrementing a counter).

---

## 5. Allowed Lateness

**Java:**
```java
PCollection<KV<String, Long>> windowed = input
    .apply(Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardMinutes(5)))
        .triggering(AfterWatermark.pastEndOfWindow()
            .withLateFirings(AfterPane.elementCountAtLeast(1)))
        .accumulatingFiredPanes()
        // Accept late data up to 2 hours past window end
        .withAllowedLateness(Duration.standardHours(2), Window.ClosingBehavior.FIRE_ALWAYS));
```

**Python:**
```python
windowed = input | beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterWatermark(late=AfterCount(1)),
    accumulation_mode=AccumulationMode.ACCUMULATING,
    allowed_lateness=2 * 3600  # seconds
)
```

> [!WARNING]
> Data arriving **after** the `allowedLateness` period elapses from the window's end is dropped regardless of trigger configuration. There is no way to recover it after the window closes permanently.

---

## 6. Production Pattern: Full Trigger Setup

This is the standard trigger pattern for production streaming pipelines:

**Java:**
```java
PCollection<KV<String, Long>> metricsPerMinute = events
    .apply("AssignWindows",
        Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardMinutes(1)))
            .triggering(
                AfterWatermark.pastEndOfWindow()
                    // Speculative: emit every 10s of processing time while waiting
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElement()
                            .plusDelayOf(Duration.standardSeconds(10)))
                    // Late: re-emit on every new late element, up to allowed lateness
                    .withLateFirings(AfterPane.elementCountAtLeast(1)))
            // Each pane includes ALL data seen so far (consumers overwrite by key)
            .accumulatingFiredPanes()
            // Accept late data for up to 30 minutes after window ends
            .withAllowedLateness(Duration.standardMinutes(30), Window.ClosingBehavior.FIRE_ALWAYS))
    .apply("CountPerKey", Count.perKey());
```

**Python:**
```python
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode
)

metrics_per_minute = (
    events
    | "AssignWindows" >> beam.WindowInto(
        window.FixedWindows(60),
        trigger=AfterWatermark(
            early=AfterProcessingTime(10),
            late=AfterCount(1)
        ),
        accumulation_mode=AccumulationMode.ACCUMULATING,
        allowed_lateness=1800
    )
    | "CountPerKey" >> beam.combiners.Count.PerKey()
)
```

---

## 7. Assigning Timestamps to Elements

By default, elements from bounded sources get a timestamp of `BoundedWindow.TIMESTAMP_MIN_VALUE`. For streaming, sources assign timestamps automatically. You can override timestamps manually:

**Java — using `WithTimestamps`:**
```java
// Parse timestamp from a CSV field and assign as event time
PCollection<Event> withTimestamps = rawEvents.apply(
    "AssignTimestamps",
    WithTimestamps.of((Event e) -> new Instant(e.getEventTimeMillis()))
        // Bound how much earlier than current watermark an event can be assigned
        .withAllowedTimestampSkew(Duration.standardMinutes(5)));
```

**Java — using `outputWithTimestamp` in `DoFn`:**
```java
public class ParseEventDoFn extends DoFn<String, Event> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<Event> out) {
        Event event = parseLine(line);
        // Explicitly set the event timestamp
        out.outputWithTimestamp(event, new Instant(event.getEventTimeMillis()));
    }
}
```

**Python — using `TimestampedValue`:**
```python
def parse_with_timestamp(line):
    parts = line.split(",")
    event_time = int(parts[0])  # epoch millis
    return beam.window.TimestampedValue({"data": parts[1]}, event_time / 1000.0)

events_with_ts = raw_lines | beam.Map(parse_with_timestamp)
```

---

## 8. Critical Warnings

> [!WARNING]
> **GlobalWindows + default trigger on streaming = never fires.** The default trigger waits for the watermark to reach `+Infinity`. On an unbounded source this never happens. Always specify a custom trigger when using `GlobalWindows` with streaming data.

> [!WARNING]
> **Late data is silently dropped without `withAllowedLateness`.** The default allowed lateness is zero. Elements arriving after the watermark passes the window end are discarded with no error. Always check pipeline metrics for `droppedDueToLateness` to detect this.

> [!WARNING]
> **Session windows are per-key.** If your data is not in `KV` format, session windows group all elements into one session per window per parallel bundle — which is usually not what you want. Always key your data before applying session windows.

> [!WARNING]
> **Timestamps must be assigned BEFORE windowing.** If you apply `Window.into()` before assigning event timestamps, elements will be windowed based on the default timestamp (ingestion time or min value), not your event's actual time.
