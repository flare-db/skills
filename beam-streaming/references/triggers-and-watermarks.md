
# Beam Streaming: Triggers and Watermarks

Complete reference for triggers, watermarks, accumulation modes, and late-data handling.

---

## 1. The Beam Execution Model for Streaming Data

In the Beam model, each element in a `PCollection` carries an **event timestamp** — a point in time in the event timeline (not the worker's wall clock). Windowing divides the event timeline into named ranges; elements are assigned to windows based on their event timestamp.

### Key Concepts

- **Watermark**: The runner's lower-bound estimate for "the minimum event timestamp not yet observed." When the watermark advances past time `T`, it means the runner believes all elements with event_time ≤ T have been received.
- **Pane**: Each emission from a window is a _pane_. A window can fire multiple panes if configured with early/late firings.
- **Trigger**: Controls when panes are emitted — when the watermark passes the window end, after a processing-time delay, after N elements, or combinations of these.

### Timeline Example

```
Event time axis:
  |--window A--|--window B--|--window C--|-->
  t=0         t=5         t=10        t=15  (minutes)

Watermark advances from left to right.
When watermark reaches t=5:
  → Window A's ON_TIME pane fires.
  → Elements arriving with event_time in [0,5) but processing_time > watermark(5) are "late."
```

---

## 2. Trigger Types

### 2.1 AfterWatermark — Event-Time Trigger

The primary trigger for event-time processing. Fires when the watermark passes the end of the window boundary.

**Java:**
```java
import org.apache.beam.sdk.transforms.windowing.*;
import org.joda.time.Duration;

// Basic: fire only ON_TIME (once, when watermark passes window end)
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterWatermark.pastEndOfWindow())
    .discardingFiredPanes();

// With early speculative and late firings
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(
        AfterWatermark.pastEndOfWindow()
            // Early: fire every 30s of processing time while waiting for watermark
            .withEarlyFirings(
                AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardSeconds(30)))
            // Late: fire on every late element (up to allowed lateness)
            .withLateFirings(AfterPane.elementCountAtLeast(1)))
    .accumulatingFiredPanes()
    .withAllowedLateness(Duration.standardHours(2));
```

**Python:**
```python
from apache_beam.transforms import window
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode
)
import apache_beam as beam

# Basic ON_TIME only
beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterWatermark(),
    accumulation_mode=AccumulationMode.DISCARDING
)

# With early and late firings
beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterWatermark(
        early=AfterProcessingTime(30),
        late=AfterCount(1)
    ),
    accumulation_mode=AccumulationMode.ACCUMULATING,
    allowed_lateness=2 * 3600
)
```

---

### 2.2 AfterProcessingTime — Wall-Clock Trigger

Fires after a delay relative to when the first element in the window pane arrived (processing time).

**Java:**
```java
// Fire 1 minute after the first element in this pane arrives
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterProcessingTime.pastFirstElementInPane()
        .plusDelayOf(Duration.standardMinutes(1)))
    .discardingFiredPanes();

// Fire 30 seconds after any element arrives (absolute processing time)
Window.into(new GlobalWindows())
    .triggering(Repeatedly.forever(
        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30))))
    .discardingFiredPanes();
```

**Python:**
```python
from apache_beam.transforms.trigger import AfterProcessingTime, Repeatedly

# Fire 60s after first element
beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterProcessingTime(60),
    accumulation_mode=AccumulationMode.DISCARDING
)

# Periodic: fire every 30s indefinitely
beam.WindowInto(
    window.GlobalWindows(),
    trigger=Repeatedly(AfterProcessingTime(30)),
    accumulation_mode=AccumulationMode.DISCARDING
)
```

---

### 2.3 AfterCount (AfterPane) — Element-Count Trigger

Fires when a specified number of elements have been buffered in the current pane.

**Java:**
```java
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterPane.elementCountAtLeast(100))
    .discardingFiredPanes();
```

**Python:**
```python
from apache_beam.transforms.trigger import AfterCount

beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterCount(100),
    accumulation_mode=AccumulationMode.DISCARDING
)
```

---

### 2.4 Composite Triggers

**Repeatedly — Repeat any trigger indefinitely:**
```java
// Java: fire every 30s of processing time, forever
Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30)))
```
```python
from apache_beam.transforms.trigger import Repeatedly
Repeatedly(AfterProcessingTime(30))
```

**AfterFirst / AfterAny — whichever trigger fires first:**
```java
// Java: fire when watermark fires OR 100 elements arrive, whichever comes first
AfterFirst.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(100))
```
```python
from apache_beam.transforms.trigger import AfterAny
AfterAny(AfterWatermark(), AfterCount(100))
```

**AfterAll — all listed triggers must have fired:**
```java
// Java: fire after BOTH watermark AND 100+ elements
AfterAll.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(100))
```

**OrFinally — primary trigger fires repeatedly, stops on secondary:**
```java
// Java: fire every 10s, stop when watermark fires
Repeatedly.forever(
    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(10)))
    .orFinally(AfterWatermark.pastEndOfWindow())
```

---

## 3. Full Production Trigger Examples

### Standard Streaming Aggregation

Full Java example for a production streaming pipeline:

```java
PCollection<KV<String, Long>> metricsPerMinute = eventStream
    .apply("AssignWindows",
        Window.<KV<String, Long>>into(FixedWindows.of(Duration.standardMinutes(1)))
            .triggering(
                AfterWatermark.pastEndOfWindow()
                    // Speculative: emit partial results every 10s while waiting
                    .withEarlyFirings(
                        AfterProcessingTime.pastFirstElementInPane()
                            .plusDelayOf(Duration.standardSeconds(10)))
                    // Late: re-emit when any late element arrives
                    .withLateFirings(AfterPane.elementCountAtLeast(1)))
            // ACCUMULATING: each pane includes all data so far (consumer can overwrite by key)
            .accumulatingFiredPanes()
            // Accept late data for up to 30 minutes after window end
            .withAllowedLateness(Duration.standardMinutes(30),
                Window.ClosingBehavior.FIRE_ALWAYS))
    .apply("CountPerKey", Count.perKey());
```

Full Python equivalent:

```python
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode
)

metrics_per_minute = (
    event_stream
    | "AssignWindows" >> beam.WindowInto(
        window.FixedWindows(60),
        trigger=AfterWatermark(
            early=AfterProcessingTime(10),
            late=AfterCount(1)
        ),
        accumulation_mode=AccumulationMode.ACCUMULATING,
        allowed_lateness=1800  # 30 minutes
    )
    | "CountPerKey" >> beam.combiners.Count.PerKey()
)
```

### Count-Based Micro-Batch Pattern

```java
// Java: emit every 500 elements or when watermark fires (whichever comes first)
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterFirst.of(
        AfterWatermark.pastEndOfWindow(),
        AfterPane.elementCountAtLeast(500)))
    .discardingFiredPanes()
    .withAllowedLateness(Duration.ZERO);
```

```python
# Python equivalent
beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterAny(AfterWatermark(), AfterCount(500)),
    accumulation_mode=AccumulationMode.DISCARDING,
    allowed_lateness=0
)
```

---

## 4. Accumulation Modes Explained

### ACCUMULATING

Each new pane includes **all** elements seen in the window so far. Use when:
- The downstream consumer **overwrites** by key (e.g., BigQuery UPSERT, updating a cache)
- You want each pane to be a self-sufficient complete result

| Event | Elements in This Pane |
|---|---|
| Early pane at t+10s | `["a", "b"]` → count=2 |
| ON_TIME pane at watermark | `["a", "b", "c"]` → count=3 |
| Late pane (element "d" arrives) | `["a", "b", "c", "d"]` → count=4 |

**Risk**: If a consumer appends each pane output, rows are duplicated.

### DISCARDING

Each pane contains **only** the elements that arrived since the last pane fired. Use when:
- The downstream consumer **appends or accumulates** values itself (e.g., log file, increment counter)
- You want each pane to represent a delta

| Event | Elements in This Pane |
|---|---|
| Early pane at t+10s | `["a", "b"]` → count=2 (delta) |
| ON_TIME pane | `["c"]` → count=1 (only new) |
| Late pane | `["d"]` → count=1 (only new) |

**Risk**: If a consumer tries to "use the latest pane", it loses context.

---

## 5. Allowed Lateness Deep Dive

```
Window: [0, 5min) ends at t=5min
Watermark passes t=5min → ON_TIME pane fires
Watermark passes t=5min + allowedLateness → window CLOSES PERMANENTLY
Any element with event_time in [0, 5min) arriving after the window closes → DROPPED
```

### Fire Behaviors

- `ClosingBehavior.FIRE_ALWAYS` (Java): The window emits a final pane when it closes (even if empty).
- `ClosingBehavior.FIRE_IF_NON_EMPTY` (Java): Only fires a closing pane if new elements arrived since the last pane.

**Java:**
```java
Window.into(FixedWindows.of(Duration.standardMinutes(5)))
    .triggering(AfterWatermark.pastEndOfWindow()
        .withLateFirings(AfterPane.elementCountAtLeast(1)))
    .accumulatingFiredPanes()
    .withAllowedLateness(Duration.standardHours(1), Window.ClosingBehavior.FIRE_ALWAYS);
```

**Python:**
```python
beam.WindowInto(
    window.FixedWindows(5 * 60),
    trigger=AfterWatermark(late=AfterCount(1)),
    accumulation_mode=AccumulationMode.ACCUMULATING,
    allowed_lateness=3600  # 1 hour in seconds
)
```

### Interaction with Accumulation Mode

- `ACCUMULATING + late firings`: Late elements cause the window to re-emit an updated full result. Consumer must handle duplicate row updates.
- `DISCARDING + late firings`: Late elements cause a new delta pane containing only the late arrivals.

---

## 6. Watermark Configuration by Source

### KafkaIO — Full Java Examples

```java
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.joda.time.Duration;
import org.joda.time.Instant;

// Strategy 1: Processing-time watermark — simplest, no event-time semantics.
// Watermark never stalls on idle partitions.
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withProcessingTime();

// Strategy 2: Log-append-time watermark — uses Kafka broker's log-append timestamp.
// Requires Kafka topic to be configured with 'LogAppendTime' timestamp type.
// Recommended for most event-time streaming use cases.
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withLogAppendTime();

// Strategy 3: Create-time watermark with bounded lag.
// Uses producer-set message timestamp; maxDelay allows for out-of-order tolerance.
// When a partition is idle, the watermark advances to 'now - maxDelay'.
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withCreateTime(Duration.standardMinutes(2));  // max lag = 2 minutes

// Strategy 4: Custom timestamp policy for full control.
// TimestampPolicyFactory gives per-record timestamp + partition-level idle handling.
// When a partition goes idle, the policy can return 'now - maxDelay' to advance the watermark.
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withTimestampPolicyFactory(TimestampPolicyFactory.withCreateTime(Duration.standardMinutes(2)));
```

### PubsubIO — Java

```java
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;

// Default: uses Pub/Sub message publish time as event timestamp
PubsubIO.readStrings()
    .fromSubscription("projects/my-project/subscriptions/my-sub");

// Custom event time from message attribute (attribute value = epoch millis as string)
PubsubIO.readStrings()
    .fromSubscription("projects/my-project/subscriptions/my-sub")
    .withTimestampAttribute("event_time_millis");
```

---

## 7. Debugging Watermark Issues

### Detecting Stalled Watermarks

1. Check your runner's metrics/UI for the current watermark timestamp.
2. If the watermark hasn't advanced in several minutes, a source partition is likely idle or stalled.
3. Check pipeline metrics: `watermark_hold` and `element_count` for source stages.

### Common Causes and Fixes

| Cause | Symptom | Fix |
|---|---|---|
| Idle Kafka partition (no messages arriving) | Watermark freezes at the last-seen timestamp of the idle partition | Use `withCreateTime(maxDelay)` — on idle partitions it auto-advances watermark to `now - maxDelay` |
| Clock skew between producers | Watermark cannot advance because some elements have far-future timestamps | Use `withCreateTime(Duration)` with a bounded lag to cap how far ahead timestamps can be |
| Custom source not advancing watermark | `UnboundedReader.getWatermark()` returns `BoundedWindow.TIMESTAMP_MIN_VALUE` | Implement proper watermark tracking in custom `UnboundedReader.getWatermark()` |
| AllowedLateness too short | Elements processed before watermark catches up are dropped | Increase `withAllowedLateness(Duration)` to match source delay characteristics |
| GlobalWindows with default trigger | No output ever produced | Add explicit trigger (e.g., `Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(30))))`) |
