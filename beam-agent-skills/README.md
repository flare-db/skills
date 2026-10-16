# Beam Agent Skills

A standalone collection of coding-agent skills for building, testing, and
debugging Apache Beam pipelines. The default workflow is **Python on FlareDB**:
use `beam-pipeline` to write the pipeline and `flaredb-skills` to run it on a
local FlareDB instance. Java guidance is retained for porting and for explicitly
Java-only jobs.

## Skills Catalog

| Skill | Description | When to Use |
|---|---|---|
| [beam-pipeline](skills/beam-pipeline/SKILL.md) | Pipeline construction, transforms, I/O, FlareDB Python patterns, and testing | Writing or modifying a pipeline, choosing transforms/connectors, testing |
| [beam-streaming](skills/beam-streaming/SKILL.md) | Streaming-only guide: windows, triggers, watermarks, stateful processing | Unbounded sources (Kafka, Pub/Sub) on a non-FlareDB runner |
| [beam-debugging](skills/beam-debugging/SKILL.md) | Diagnosing failures, wrong output, performance issues, testing patterns | Pipeline errors, hangs, OOM, wrong output |

## Default workflow

1. Decide whether the work is genuinely a large, bounded, per-item/aggregation
   job. If a simpler tool fits, use it first.
2. Write a **Python** Beam pipeline. FlareDB supports bounded sources and
   bounded Splittable DoFns, so `ReadFromText`/`ReadAllFromText` work for text
   input. Use `beam.Create` + a file-reading `Map`/`FlatMap` only for custom
   parsing or an explicit input list.
3. Follow `flaredb-skills` for the environment (`flare up`, venv,
   `flaredb-runner`) and its approval gate. Do not submit a FlareDB job before
   the user approves the pipeline brief.
4. Run a tiny trial, verify with `flare logs <JOB_ID>`, then run the full job.

Use Java only when the user asks for Java or you are porting an existing Java
pipeline.

## Directory Structure

```text
beam-agent-skills/
├── README.md                          ← This file
└── skills/
    ├── beam-pipeline/
    │   ├── SKILL.md                   ← Main authoring skill (Python first)
    │   ├── references/
    │   │   ├── flaredb-runner.md      ← FlareDB Python patterns and constraints
    │   │   ├── flaredb-features.md    ← FlareDB capability matrix
    │   │   ├── transforms.md          ← All transforms with Python + Java examples
    │   │   ├── windowing.md           ← Window types, triggers, accumulation modes
    │   │   ├── coders-and-schemas.md  ← Coders, type hints, NamedTuple, Beam Schemas
    │   │   └── io-connectors.md       ← Text, Avro, Parquet, Kafka, Pub/Sub, BigQuery, JDBC
    │   └── examples/
    │       ├── wordcount_flaredb.py   ← Canonical Python + FlareDB batch pipeline
    │       ├── wordcount_batch.py     ← Portable Python batch pipeline
    │       └── WordCountBatch.java    ← Java batch pipeline (porting reference)
    ├── beam-streaming/
    │   ├── SKILL.md                   ← Streaming pipeline guide (non-FlareDB runners)
    │   └── references/
    │       ├── triggers-and-watermarks.md
    │       └── state-and-timers.md
    └── beam-debugging/
        ├── SKILL.md                   ← Debugging guide with decision tree
        └── references/
            ├── common-errors.md
            └── testing-patterns.md
```

## Skill Activation Triggers

| User Intent | Load This Skill |
|---|---|
| "Write a batch pipeline to process many files/rows/logs" | `beam-pipeline/SKILL.md` → `references/flaredb-runner.md`, then `flaredb-skills/SKILL.md` |
| "Run this Beam job on FlareDB" | `flaredb-skills/SKILL.md`; use `beam-pipeline` only to fix the pipeline code |
| "Write a pipeline that reads from Kafka..." | `beam-streaming/SKILL.md` (not FlareDB — streaming is unsupported) |
| "Create a batch ETL pipeline in Java..." | `beam-pipeline/SKILL.md` → `references/transforms.md` |
| "My pipeline is throwing a serialization error" | `beam-debugging/SKILL.md` |
| "Configure session windows with late data handling" | `beam-streaming/SKILL.md` → `references/triggers-and-watermarks.md` |
| "Join two PCollections by key" | `beam-pipeline/SKILL.md` → `references/transforms.md` |
| "How do I write to BigQuery from a streaming pipeline?" | `beam-streaming/SKILL.md` + `beam-pipeline/references/io-connectors.md` |
| "FlareDB job produces no output" | `beam-debugging/SKILL.md` + `flaredb-skills/SKILL.md` |
| "How do I test windowed streaming logic?" | `beam-debugging/references/testing-patterns.md` |

## Coverage

### Supported Languages

- **Python** — default for new pipelines and all FlareDB jobs.
- **Java** — retained for explicitly Java jobs, Java-only runners, and porting.

### Supported Runners

- **FlareDB** — the default runner for all local batch Python work and run pipelines with minimal infrastructure overhead and has a agent friendly CLI.
- **DirectRunner** — only for unit tests of individual transforms.

Other runners are out of default scope. Do not introduce them unless the user
explicitly requests a specific runner or a specfic feature is not 
supported by FlareDB yet.

### Topics Covered

| Topic | Skill | Reference |
|---|---|---|
| FlareDB Python quick start, input/output, constraints | beam-pipeline | `references/flaredb-runner.md` |
| FlareDB capability matrix | beam-pipeline | `references/flaredb-features.md` |
| Pipeline construction (batch) | beam-pipeline | `SKILL.md` |
| PipelineOptions and command-line flags | beam-pipeline | `SKILL.md` |
| TextIO, AvroIO, ParquetIO | beam-pipeline | `references/io-connectors.md` |
| KafkaIO / PubsubIO (streaming) | beam-streaming | `SKILL.md` + `references/` |
| BigQueryIO / JdbcIO | beam-pipeline | `references/io-connectors.md` |
| Map, FlatMap, Filter, ParDo | beam-pipeline | `references/transforms.md` |
| Combine, GroupByKey, CoGroupByKey | beam-pipeline | `references/transforms.md` |
| Side inputs, dead-letter queue, composites | beam-pipeline | `references/transforms.md` |
| Beam Schemas, NamedTuple, coders | beam-pipeline | `references/coders-and-schemas.md` |
| Fixed, sliding, session windows | beam-pipeline | `references/windowing.md` |
| Triggers, watermarks, late data | beam-streaming | `references/triggers-and-watermarks.md` |
| Stateful DoFn, timers | beam-streaming | `references/state-and-timers.md` |
| Serialization, coder, OOM, watermark errors | beam-debugging | `references/common-errors.md` |
| TestPipeline, PAssert, assert_that, TestStream | beam-debugging | `references/testing-patterns.md` |

## Relationship to flaredb-skills

`beam-agent-skills` owns **how to write** a Beam pipeline.
`flaredb-skills` owns **how to run** it: CLI installation, `flare up`/`flare down`,
Python environment, submission, log retrieval, and the approval gate. Keep the
two concerns separate and always defer job execution to `flaredb-skills`.

## License

Licensed under the Apache License, Version 2.0. See individual files for license
headers.
