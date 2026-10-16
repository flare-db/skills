# Beam Streaming: State and Timers

Complete reference for stateful processing and timely computation in Apache Beam, with full Java and Python examples.

---

## 1. Why Stateful Processing?

Standard Beam transforms are **stateless** — each element is processed independently. Stateful `DoFn`s allow you to maintain **per-key, per-window mutable memory** across multiple elements.

### When to Use Each State Type

| State Type | When to Use |
|---|---|
| `ValueState<T>` | Store a single value per key and window, such as a counter or the latest observed value. |
| `BagState<T>` | Accumulate multiple values when you need to process them together later. |
| `SetState<T>` | Track unique values and check membership. |
| `MapState<K, V>` | Maintain a collection of key-value entries within a key's state. |
| `CombiningState<InputT, AccumT, OutputT>` | Maintain an aggregate, such as a sum or count, without explicitly storing every input value. |

### When to Use Timers

| Timer Type | When to Use |
|---|---|
| Event-time timer | Schedule a callback based on the event-time watermark reaching a specified timestamp. |
| Processing-time timer | Schedule a callback based on processing time, such as triggering work after a period of elapsed time. |
---

## 2. State Types

### 2.1 ValueState — Single Value Per Key/Window

**Java:**
```java
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.coders.VarLongCoder;

public class LastSeenDoFn extends DoFn<KV<String, Long>, String> {

    @StateId("lastValue")
    private final StateSpec<ValueState<Long>> lastValueSpec =
        StateSpecs.value(VarLongCoder.of());

    @ProcessElement
    public void process(
            @Element KV<String, Long> element,
            @StateId("lastValue") ValueState<Long> lastValue,
            OutputReceiver<String> out) {

        Long prev = lastValue.read();   // read current state (null if first time)
        long current = element.getValue();
        lastValue.write(current);       // update state

        if (prev != null && current < prev) {
            out.output("Decrease detected for key=" + element.getKey()
                + " from=" + prev + " to=" + current);
        }
    }
}
```

**Python:**
```python
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
import apache_beam as beam

class LastSeenDoFn(beam.DoFn):
    LAST_VALUE = ReadModifyWriteStateSpec('last_value', beam.coders.VarIntCoder())

    def process(self, element, last_value=beam.DoFn.StateParam(LAST_VALUE)):
        key, current = element
        prev = last_value.read()
        last_value.write(current)

        if prev is not None and current < prev:
            yield f"Decrease detected for key={key}: {prev} -> {current}"
```

---

### 2.2 BagState — Append-Only Multiset

Best for accumulating a list of elements. Appends are efficient; iteration order is unspecified.

**Java:**
```java
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpecs;

public class BufferDoFn extends DoFn<KV<String, String>, List<String>> {

    @StateId("buffer")
    private final StateSpec<BagState<String>> bufferSpec =
        StateSpecs.bag(StringUtf8Coder.of());

    @ProcessElement
    public void process(
            @Element KV<String, String> element,
            @StateId("buffer") BagState<String> buffer,
            OutputReceiver<List<String>> out) {

        buffer.add(element.getValue());

        // Read all accumulated values
        Iterable<String> all = buffer.read();
        List<String> asList = StreamSupport
            .stream(all.spliterator(), false)
            .collect(Collectors.toList());

        if (asList.size() >= 10) {
            out.output(asList);
            buffer.clear();  // CRITICAL: always clear when done
        }
    }
}
```

**Python:**
```python
from apache_beam.transforms.userstate import BagStateSpec
import apache_beam as beam

class BufferDoFn(beam.DoFn):
    BUFFER = BagStateSpec('buffer', beam.coders.StrUtf8Coder())

    def process(self, element, buffer=beam.DoFn.StateParam(BUFFER)):
        key, value = element
        buffer.add(value)

        all_values = list(buffer.read())
        if len(all_values) >= 10:
            yield all_values
            buffer.clear()  # CRITICAL
```

---

### 2.3 SetState — Track Unique Elements

Stores a set of unique elements per key and window. Use `contains()` to check membership or `addIfAbsent()` to add an element only if it is not already present. This can be used to implement deduplication logic.

**Java:**
```java
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpecs;

public class DeduplicateDoFn extends DoFn<KV<String, String>, String> {

    @StateId("seenIds")
    private final StateSpec<SetState<String>> seenIdsSpec =
        StateSpecs.set(StringUtf8Coder.of());

    @ProcessElement
    public void process(
            @Element KV<String, String> element,
            @StateId("seenIds") SetState<String> seenIds,
            OutputReceiver<String> out) {

        String eventId = element.getKey();
        if (!seenIds.contains(eventId).read()) {
            seenIds.add(eventId);
            out.output(element.getValue());  // emit only first occurrence
        }
    }
}
```

---

### 2.4 MapState — Key-Value Map

For maintaining a per-key lookup table in state.

**Java:**
```java
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.StateSpecs;

public class AggregateBySubkeyDoFn extends DoFn<KV<String, KV<String, Long>>, String> {

    @StateId("perSubkey")
    private final StateSpec<MapState<String, Long>> mapSpec =
        StateSpecs.map(StringUtf8Coder.of(), VarLongCoder.of());

    @ProcessElement
    public void process(
            @Element KV<String, KV<String, Long>> element,
            @StateId("perSubkey") MapState<String, Long> map,
            OutputReceiver<String> out) {

        String subkey = element.getValue().getKey();
        long value = element.getValue().getValue();

        Long existing = map.get(subkey).read();
        map.put(subkey, (existing == null ? 0L : existing) + value);
    }
}
```

---

### 2.5 CombiningState — Auto-Aggregating State

Maintains a running aggregate (sum, min, max, etc.) with combiner semantics — more efficient than `ValueState<Long>` with manual addition.

**Java:**
```java
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.Sum;

public class RunningCountDoFn extends DoFn<KV<String, Long>, KV<String, Long>> {

    @StateId("count")
    private final StateSpec<CombiningState<Long, long[], Long>> countSpec =
        StateSpecs.combining(Sum.ofLongs());

    @ProcessElement
    public void process(
            @Element KV<String, Long> element,
            @StateId("count") CombiningState<Long, long[], Long> count,
            OutputReceiver<KV<String, Long>> out) {

        count.add(element.getValue());
        out.output(KV.of(element.getKey(), count.read()));
    }
}
```

**Python:**
```python
from apache_beam.transforms.userstate import CombiningValueStateSpec
import apache_beam as beam

class RunningCountDoFn(beam.DoFn):
    COUNT = CombiningValueStateSpec('count', combine_fn=sum)

    def process(self, element, count=beam.DoFn.StateParam(COUNT)):
        key, value = element
        count.add(value)
        yield (key, count.read())
```

---

## 3. Timer Types

### EVENT_TIME Timer — Fires Based on Watermark

The timer fires when the **watermark** passes the time you set. Useful for session timeouts defined in event time.

### PROCESSING_TIME Timer — Fires Based on Wall Clock

The timer fires when the **worker's wall clock** reaches the set time. Useful for max-buffering-time guarantees or real-time rate limits.

### Timer Usage

**Java:**
```java
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class TimerExampleDoFn extends DoFn<KV<String, Long>, String> {

    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
            @Element KV<String, Long> element,
            @Timestamp Instant timestamp,
            @TimerId("expiry") Timer timer) {
        timer.set(timestamp.plus(Duration.standardMinutes(1)));
    }

    @OnTimer("expiry")
    public void onExpiry(@Timestamp Instant fireTime, OutputReceiver<String> out) {
        out.output("timer fired at " + fireTime);
    }
}
```

**Python:**
```python
import apache_beam as beam
from apache_beam.transforms.userstate import TimerSpec, on_timer
from apache_beam.transforms.timeutil import TimeDomain

class TimerExampleDoFn(beam.DoFn):
    EXPIRY = TimerSpec('expiry', TimeDomain.WATERMARK)

    def process(self, element,
                timestamp=beam.DoFn.TimestampParam,
                timer=beam.DoFn.TimerParam(EXPIRY)):
        timer.set(timestamp + 60)

    @on_timer(EXPIRY)
    def on_expiry(self, fire_time=beam.DoFn.TimestampParam):
        yield fire_time
```

---

## 4. Common Stateful Processing Mistakes

| Mistake | Consequence | Fix |
|---|---|---|
| Retaining state after it is no longer needed | Unnecessary state growth and storage costs | Clear state when its contents are no longer needed. Account for window expiration and the intended state lifetime. |
| Using `PROCESSING_TIME` when event-time semantics are required | Timer firing depends on processing time rather than watermark progress | Use an `EVENT_TIME` timer when the operation should be driven by the event-time watermark. |
| Not keying input before applying a stateful `DoFn` | Invalid stateful processing or an error during pipeline validation or execution | Produce the required keyed input (`KV<K, V>` in Java or `(key, value)` tuples in Python) before applying the stateful `DoFn`. |
| Using `ValueState` to repeatedly accumulate a collection | Repeatedly reading, modifying, and writing a growing collection can be expensive | Use `BagState` when individual items must be retained, or `CombiningState` when only an aggregate is needed. |
| Assuming state will eventually be cleared because a timer was set | State may remain longer than expected if the intended cleanup conditions are not met | Design cleanup around timer semantics, window lifecycle, and the required retention policy. Do not assume a timer guarantees cleanup at an exact time. |
| Using a stateful `DoFn` with a hot key | Concentrated work and state can limit parallelism and increase latency | Evaluate the key distribution and state requirements. Consider key partitioning or salting only when the computation can be correctly recombined. |
