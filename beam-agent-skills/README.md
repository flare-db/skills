# Beam Agent Skills

A standalone, self-contained collection of coding-agent skills for building, testing, and debugging Apache Beam data pipelines in Java and Python.

These skills are designed to be loaded by AI coding agents (Gemini, Claude, Codex, etc.) to provide complete, actionable guidance — without requiring the agent to search external documentation.

---

## Skills Catalog

| Skill | Description | When to Use |
|---|---|---|
| [beam-pipeline](skills/beam-pipeline/SKILL.md) | End-to-end batch and streaming pipeline construction in Java and Python | Writing new pipelines, choosing transforms/IO connectors, testing |
| [beam-streaming](skills/beam-streaming/SKILL.md) | Streaming-specific guide: windows, triggers, watermarks, stateful processing | Unbounded sources (Kafka, Pub/Sub), event-time semantics, late data |
| [beam-debugging](skills/beam-debugging/SKILL.md) | Diagnosing and fixing pipeline failures, wrong output, performance issues | Serialization errors, OOM, no output, wrong window assignments |

---

## Directory Structure

```
beam-agent-skills/
├── README.md                          ← This file
├── skills/
│   ├── beam-pipeline/
│   │   ├── SKILL.md                   ← Main agent skill (load this first)
│   │   ├── references/
│   │   │   ├── transforms.md          ← All transforms with Java + Python examples
│   │   │   ├── windowing.md           ← Window types, triggers, accumulation modes
│   │   │   ├── coders-and-schemas.md  ← Coders, TypeDescriptors, Beam Schemas
│   │   │   └── io-connectors.md       ← TextIO, AvroIO, KafkaIO, BigQueryIO, JdbcIO...
│   │   └── examples/
│   │       ├── WordCountBatch.java    ← Canonical Java batch pipeline
│   │       └── wordcount_batch.py     ← Canonical Python batch pipeline
│   │
│   ├── beam-streaming/
│   │   ├── SKILL.md                   ← Streaming pipeline guide
│   │   └── references/
│   │       ├── triggers-and-watermarks.md  ← Trigger catalog, watermark deep-dive
│   │       └── state-and-timers.md         ← Stateful DoFns, all state types, timers
│   │
│   └── beam-debugging/
│       ├── SKILL.md                   ← Debugging guide with decision tree
│       └── references/
│           ├── common-errors.md       ← Full error table + root-cause analysis
│           └── testing-patterns.md    ← TestPipeline, PAssert, TestStream, integration tests
```

---

## How to Use These Skills

### As an Agent

1. **Discovery**: Read skill descriptions in this README to identify which skill applies.
2. **Activation**: Load the matching `SKILL.md` file and follow its workflow.
3. **Deep Reference**: When the SKILL.md points to a `references/` file, load it on demand for detailed code patterns.

### Skill Activation Triggers

| User Intent | Load This Skill |
|---|---|
| "Write a Beam pipeline that reads from Kafka..." | `beam-streaming/SKILL.md` |
| "Create a batch ETL pipeline in Java..." | `beam-pipeline/SKILL.md` |
| "My pipeline is throwing NotSerializableException" | `beam-debugging/SKILL.md` |
| "Configure session windows with late data handling" | `beam-streaming/SKILL.md` → `references/triggers-and-watermarks.md` |
| "I need to join two PCollections by key" | `beam-pipeline/SKILL.md` → `references/transforms.md` |
| "How do I write to BigQuery from a streaming pipeline?" | `beam-streaming/SKILL.md` + `beam-pipeline/references/io-connectors.md` |
| "Pipeline produces no output on streaming" | `beam-debugging/SKILL.md` |
| "How do I test windowed streaming logic?" | `beam-debugging/references/testing-patterns.md` |

---

## Coverage

### Supported Languages
- **Java** (Apache Beam Java SDK 2.x)
- **Python** (Apache Beam Python SDK 2.x)

### Supported Runners
- **DirectRunner** (local testing)
- **DataflowRunner** (Google Cloud Dataflow)
- **FlinkRunner** (Apache Flink)
- **SparkRunner** (Apache Spark)

### Topics Covered

| Topic | Skill | Reference |
|---|---|---|
| Pipeline construction (batch) | beam-pipeline | `SKILL.md` |
| PipelineOptions and command-line flags | beam-pipeline | `SKILL.md` |
| TextIO, AvroIO, ParquetIO | beam-pipeline | `references/io-connectors.md` |
| KafkaIO (streaming) | beam-streaming | `SKILL.md` + `references/` |
| PubsubIO (streaming) | beam-streaming | `SKILL.md` |
| BigQueryIO (batch + streaming) | beam-pipeline | `references/io-connectors.md` |
| JdbcIO | beam-pipeline | `references/io-connectors.md` |
| MapElements, FlatMapElements, Filter | beam-pipeline | `references/transforms.md` |
| ParDo and DoFn lifecycle | beam-pipeline | `references/transforms.md` |
| Combine, CombineFn | beam-pipeline | `references/transforms.md` |
| GroupByKey, CoGroupByKey | beam-pipeline | `references/transforms.md` |
| Side inputs | beam-pipeline | `references/transforms.md` |
| Dead-letter queue (multi-output) | beam-pipeline | `references/transforms.md` |
| Composite transforms | beam-pipeline | `references/transforms.md` |
| Beam Schemas (@DefaultSchema) | beam-pipeline | `references/coders-and-schemas.md` |
| TypeDescriptors (Java) | beam-pipeline | `references/coders-and-schemas.md` |
| AvroCoder, custom coders | beam-pipeline | `references/coders-and-schemas.md` |
| Fixed, Sliding, Session windows | beam-pipeline | `references/windowing.md` |
| AfterWatermark trigger | beam-streaming | `references/triggers-and-watermarks.md` |
| AfterProcessingTime trigger | beam-streaming | `references/triggers-and-watermarks.md` |
| ACCUMULATING vs DISCARDING | beam-streaming | `references/triggers-and-watermarks.md` |
| Allowed lateness | beam-streaming | `references/triggers-and-watermarks.md` |
| Watermark configuration | beam-streaming | `references/triggers-and-watermarks.md` |
| Stateful DoFn (ValueState, BagState, SetState, MapState, CombiningState) | beam-streaming | `references/state-and-timers.md` |
| Timers (EVENT_TIME, PROCESSING_TIME) | beam-streaming | `references/state-and-timers.md` |
| Session detection, deduplication, rate limiting | beam-streaming | `references/state-and-timers.md` |
| NotSerializableException | beam-debugging | `SKILL.md` + `references/common-errors.md` |
| PicklingError (Python) | beam-debugging | `references/common-errors.md` |
| Cannot infer coder | beam-debugging | `references/common-errors.md` |
| Hot key skew / OOM | beam-debugging | `references/common-errors.md` |
| Watermark stalls | beam-debugging | `references/common-errors.md` |
| TestPipeline, PAssert, assert_that | beam-debugging | `references/testing-patterns.md` |
| TestStream (streaming unit tests) | beam-debugging | `references/testing-patterns.md` |

---

## License

Licensed under the Apache License, Version 2.0. See individual files for license headers.
