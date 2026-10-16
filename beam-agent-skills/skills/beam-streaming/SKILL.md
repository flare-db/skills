---
name: beam-streaming
description: Guide for building Apache Beam streaming pipelines with event-time semantics, windowing, watermarks, and stateful processing on a non-FlareDB runner. Use when working with unbounded sources (Kafka, Pub/Sub), configuring windows and triggers, handling late data, or implementing stateful and timely DoFns. Do not use for FlareDB jobs, which do not support unbounded sources.
---

# Apache Beam Streaming Pipeline Guide

Streaming pipelines in Apache Beam process **unbounded** (continuous, never-ending) data from sources like Apache Kafka, Google Cloud Pub/Sub, or custom socket-based feeds. Unlike batch pipelines, streaming pipelines require explicit decisions about *when* to emit results via windows and triggers.

> **FlareDB does not support unbounded sources.** It does support bounded
> sources, bounded Splittable DoFns, fixed/sliding windows, and bag/value state
> per the capability matrix. If the target is FlareDB, use `beam-pipeline`; use
> this skill only for unbounded input when the user explicitly requests
> streaming.

---

## 1. When to Use Streaming vs Batch

| Use Streaming (Unbounded) | Use Batch (Bounded) |
|---|---|
| Data arrives continuously from Kafka, Pub/Sub, sockets | Data is a finite file, database snapshot, or export |
| Results must be emitted continuously without waiting for all data | Total latency is acceptable to wait for all data |
| Real-time dashboards, alerting, fraud detection | ETL jobs, historical reports, daily rollups |
| Low-latency response (< minutes) | High-throughput, cost-optimized processing |

---

## 2. Streaming Pipeline Skeleton — Java (only when Java is required)

Python is the default SDK. The Java skeleton below is kept for Java-only runners
and porting; the Python skeleton follows in Section 3.

```java
package org.apache.beam.examples.streaming;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class KafkaToBigQueryStreaming {

    public interface Options extends PipelineOptions, StreamingOptions {
        @Description("Kafka bootstrap servers")
        String getBootstrapServers();
        void setBootstrapServers(String v);

        @Description("Kafka input topic")
        String getInputTopic();
        void setInputTopic(String v);

        @Description("BigQuery output table (project:dataset.table)")
        String getOutputTable();
        void setOutputTable(String v);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);  // Enable streaming mode

        Pipeline pipeline = Pipeline.create(options);

        pipeline
            // 1. Read from Kafka (unbounded source)
            .apply("ReadFromKafka",
                KafkaIO.<Long, String>read()
                    .withBootstrapServers(options.getBootstrapServers())
                    .withTopic(options.getInputTopic())
                    .withKeyDeserializer(LongDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
                    .withProcessingTime()          // use processing time as watermark
                    .withConsumerConfigUpdates(java.util.Collections.singletonMap(
                        "auto.offset.reset", "latest")))

            // 2. Extract just the string values
            .apply("ExtractValues",
                MapElements.into(TypeDescriptors.strings())
                    .via(record -> record.getKV().getValue()))

            // 3. Apply 1-minute fixed windows
            .apply("Window1Min",
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterProcessingTime.pastFirstElement()
                                    .plusDelayOf(Duration.standardSeconds(10)))
                            .withLateFirings(AfterPane.elementCountAtLeast(1)))
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.standardMinutes(5)))

            // 4. Count words per window
            .apply("Tokenize",
                org.apache.beam.sdk.transforms.FlatMapElements.into(TypeDescriptors.strings())
                    .via((String line) -> java.util.Arrays.asList(line.split("\\s+"))))
            .apply("CountPerWord", Count.perElement())

            // 5. Format as BigQuery TableRow
            .apply("ToTableRow",
                MapElements.into(org.apache.beam.sdk.values.TypeDescriptor.of(TableRow.class))
                    .via((KV<String, Long> wc) -> new TableRow()
                        .set("word", wc.getKey())
                        .set("count", wc.getValue())))

            // 6. Write to BigQuery (streaming inserts)
            .apply("WriteToBigQuery",
                BigQueryIO.writeTableRows()
                    .to(options.getOutputTable())
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        pipeline.run();  // Non-blocking for streaming
    }
}
```

---

## 3. Streaming Pipeline Skeleton — Python (default, Pub/Sub → BigQuery)

```python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0

"""Streaming pipeline: Pub/Sub → word count → BigQuery."""

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions, SetupOptions, StandardOptions
)
from apache_beam.transforms import window
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode
)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True,
                        help="Pub/Sub subscription: projects/PROJECT/subscriptions/SUB")
    parser.add_argument("--output_table", required=True,
                        help="BigQuery table: project:dataset.table")
    known_args, pipeline_args = parser.parse_known_args(argv)

    opts = PipelineOptions(pipeline_args)
    opts.view_as(SetupOptions).save_main_session = True
    opts.view_as(StandardOptions).streaming = True  # Enable streaming mode

    with beam.Pipeline(options=opts) as p:
        (
            p
            # 1. Read from Pub/Sub (unbounded source)
            | "ReadPubSub"    >> beam.io.ReadFromPubSub(subscription=known_args.subscription)

            # 2. Decode bytes to string
            | "Decode"        >> beam.Map(lambda b: b.decode("utf-8"))

            # 3. Tokenize
            | "Tokenize"      >> beam.FlatMap(lambda line: line.split())

            # 4. Apply 1-minute fixed windows with triggers
            | "Window1Min"    >> beam.WindowInto(
                window.FixedWindows(60),
                trigger=AfterWatermark(
                    early=AfterProcessingTime(10),
                    late=AfterCount(1)
                ),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=300  # 5 minutes
            )

            # 5. Count per word
            | "CountWords"    >> beam.combiners.Count.PerElement()

            # 6. Format for BigQuery
            | "FormatBQ"      >> beam.Map(
                lambda wc: {"word": wc[0], "count": wc[1]})

            # 7. Write to BigQuery
            | "WriteBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema={"fields": [
                    {"name": "word",  "type": "STRING", "mode": "REQUIRED"},
                    {"name": "count", "type": "INTEGER", "mode": "REQUIRED"}
                ]},
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )


if __name__ == "__main__":
    run()
```

---

## 4. Event-Time vs Processing-Time Decision Guide

| Scenario | Use Event-Time | Use Processing-Time | Reasoning |
|---|---|---|---|
| IoT sensor readings | ✅ | | Data arrives out of order; sensor timestamp reflects reality |
| Click-stream analytics | ✅ | | User actions have meaningful timestamps in the data |
| Fraud detection by transaction time | ✅ | | Transaction time is the semantically correct grouping boundary |
| Simple rate limiting / throttling | | ✅ | Wall-clock is the correct reference for request rates |
| Log ingestion with no meaningful timestamp | | ✅ | No event timestamp available; ingestion time is best proxy |
| Alerting "within 5 minutes of now" | | ✅ | Human latency expectations are in wall-clock time |
| Joining two streams by event time | ✅ | | Cross-stream joins require a common time reference |

---

## 5. Watermark Fundamentals

The **watermark** is the runner's estimate of the minimum event-time not yet seen. Conceptually:

```
Event timeline: ----[old events]----[watermark]----[new events arriving]---->
                                        ^
                              "We think all events before
                               this timestamp have arrived"
```

### How Watermarks Advance

1. Sources emit watermarks alongside data elements.
2. The runner tracks the minimum watermark across all parallel source partitions.
3. When the watermark passes `T`, windows ending at `T` can fire their ON_TIME pane.
4. Elements arriving with `event_time < watermark` are classified as **late**.

### Late Data Flow

```
Window: [0, 5min)  — ends at T=5min
Watermark advances past T=5min → window fires ON_TIME pane
Late element with event_time=3min arrives at processing_time=7min:
  - If allowedLateness >= 2min: accepted, triggers LATE pane
  - If allowedLateness < 2min:  dropped (droppedDueToLateness metric increments)
```

### Source vs Output Watermarks

- **Source watermark**: Set by the source (KafkaIO, PubsubIO, etc.) based on message timestamps.
- **Output watermark**: Runner computes the minimum event-time of all unprocessed elements; it can never exceed the source watermark.
- **Watermark skew**: If one source partition is idle, the global watermark freezes at its last-seen timestamp.

---

## 6. Source Watermark Configuration

### KafkaIO Watermark Strategies (Java)

```java
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.joda.time.Duration;
import org.joda.time.Instant;

// Strategy 1: Processing time watermark (wall clock)
// - Simple; no event-time semantics
// - Watermark never stalls on idle partitions
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withProcessingTime();

// Strategy 2: Log-append-time watermark (event time from the Kafka broker)
// - Kafka records carry a server-side log-append timestamp
// - withLogAppendTime() derives event time and watermark from that timestamp
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withLogAppendTime();

// Strategy 3: Create-time watermark with bounded lag
// - Uses the producer-set record timestamp
// - withCreateTime(maxDelay) auto-advances the watermark to 'now - maxDelay'
//   on idle partitions, preventing watermark stalls
KafkaIO.<Long, String>read()
    .withBootstrapServers("broker:9092")
    .withTopic("events")
    .withKeyDeserializer(LongDeserializer.class)
    .withValueDeserializer(StringDeserializer.class)
    .withCreateTime(Duration.standardMinutes(2));  // max lag = 2 minutes
```

### PubsubIO (Java) — Event-Time from Attribute

```java
// PubsubIO auto-assigns watermark from Pub/Sub server receive time by default.
// To use a custom event-time attribute:
PubsubIO.readStrings()
    .fromSubscription("projects/my-project/subscriptions/my-sub")
    // attribute value must be epoch milliseconds as a string
    .withTimestampAttribute("event_time_millis");
```

### Python KafkaIO Watermark

```python
from apache_beam.io.kafka import ReadFromKafka

# Reading from Kafka — Python uses cross-language expansion.
# Default timestamp_policy is ProcessingTime. For event-time semantics,
# set timestamp_policy to ReadFromKafka.log_append_time or
# ReadFromKafka.create_time_policy.
records = pipeline | ReadFromKafka(
    consumer_config={"bootstrap.servers": "broker:9092"},
    topics=["events"],
    timestamp_policy=ReadFromKafka.log_append_time
)
```

---

## 7. Streaming State Management Rules

1. **Stateful `DoFn`s require keyed input** — input must be `KV<K, V>` (Java) or `(key, value)` tuples (Python). Apply a key-extraction step before using state.
2. **Always call `.clear()` in timer callbacks** — state accumulated inside a window that is never cleared will leak memory indefinitely on workers. Clear state when the timer fires and the session/window is done.
3. **Avoid hot keys** — if a single key receives millions of events per second, all its state lives on one worker. Use key salting or `GroupIntoBatches` to distribute work.
4. **EVENT_TIME timers vs PROCESSING_TIME timers**:
   - `EVENT_TIME`: timer fires when watermark passes the set timestamp (based on data arrival).
   - `PROCESSING_TIME`: timer fires based on wall clock (best for rate limiting, max buffering time).
5. **Don't use `ValueState` as a list** — use `BagState` for append-only accumulation. `ValueState` requires read-modify-write (3 state operations per element); `BagState` is O(1) append.

---

## 8. Load More Detail

| Reference File | Contents |
|---|---|
| [references/triggers-and-watermarks.md](references/triggers-and-watermarks.md) | Full trigger catalog with examples, accumulation modes, watermark deep-dive, debugging stalls |
| [references/state-and-timers.md](references/state-and-timers.md) | All state types with code, timer types, full stateful DoFn examples (sessionization, rate limiting, deduplication) |
