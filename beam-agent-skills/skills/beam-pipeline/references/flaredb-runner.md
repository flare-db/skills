# FlareDB Runner Reference (Python)

Use this reference when the target runner is FlareDB. `beam-pipeline` is for
*authoring* the pipeline; `flaredb-skills` is for *running* it. Do not submit a
FlareDB job yourself — `flaredb-skills` owns the CLI, environment, approval gate,
and log retrieval. Read
[`flaredb-skills/SKILL.md`](../../../../flaredb-skills/SKILL.md) before running
anything. For a snapshot of what FlareDB supports, see
[flaredb-features.md](flaredb-features.md).

## Why Python is the default

- FlareDB runs Python Beam pipelines directly and ships the code to workers.
- No Maven/Gradle, no JAR staging, no Java toolchain — one Python file is enough.
- The local loop is faster: write, `py_compile`, run a small trial, iterate.
- Write Java only when the user asks for Java or you are porting an existing
  Java pipeline.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install flaredb-runner
```

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from flaredb_runner.flare_runner import FlareRunner

pipeline_options = PipelineOptions(
    job_endpoint="127.0.0.1:8099",   # required
    job_name="my-job",               # optional but useful in logs
)

with beam.Pipeline(runner=FlareRunner(), options=pipeline_options) as p:
    (
        p
        | "Inputs"   >> beam.Create(["path/to/a.txt", "path/to/b.txt"])
        | "Process"  >> beam.FlatMap(process_one)
        | "Compact"  >> beam.Map(format_result)
    )
```

Pass the same options on the command line if you prefer:
`python3 pipeline.py --job_endpoint=127.0.0.1:8099 --job_name=my-job`.

## Input patterns

FlareDB supports bounded sources and bounded Splittable DoFns, so ordinary text
input can be read directly with `ReadFromText`/`ReadAllFromText`. The
`beam.Create` + read-file pattern below is still useful when you need an
explicit input list or custom per-file parsing.

```python
import apache_beam as beam

def read_lines(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            yield path, line.rstrip("\n")

paths = [
    "/data/logs/app-2026-10-01.log",
    "/data/logs/app-2026-10-02.log",
]

records = p | beam.Create(paths) | beam.FlatMap(read_lines)
```

Keep the path list cheap to build (one driver-side pass is fine); the per-file
work is what the workers parallelise.

## Output patterns

Reduce inside the pipeline so the final result is small enough to read directly.
Prefer built-in combiners and bounded aggregations:

```python
counts = records | beam.Map(lambda rec: (rec[1], 1)) | beam.combiners.Count.PerKey()
top = counts | beam.combiners.Top.PerKey(10)
```

Emit one short line per result (never raw content) and aim for a final output you
can read in full — usually under about 50 lines. If the full detail is large,
write the detail to a file you read selectively later, and emit only the summary
from the pipeline. See
[`flaredb-skills/SKILL.md`](../../../../flaredb-skills/SKILL.md) for how FlareDB
surfaces the job output and for the current table/IO support matrix.

## Per-item error handling

Make every per-item function defensive. Catch exceptions per item and emit an
error record instead of failing the whole job:

```python
def process_one(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            yield from (line.rstrip("\n") for line in f)
    except Exception as exc:  # noqa: BLE001 - report and continue
        yield ("__ERROR__", path, type(exc).__name__, str(exc))
```

Then count or list the `__ERROR__` records and report them when you finish.
Never modify or delete the inputs — FlareDB jobs should be read-only on user
data.

## FlareDB constraints

- Unbounded sources are not supported. Bounded sources and bounded SDFs are.
- Fixed/sliding windows, triggers, and bag/value state are supported per the
  capability matrix; session/custom windows, `SetState`/`MapState`, metrics, and
  runner-initiated splitting are not. See
  [flaredb-features.md](flaredb-features.md).
- Prefer `ReadFromText`/`ReadAllFromText` for plain text input; keep
  `beam.Create` + file-reading `Map`/`FlatMap` for custom parsing or an explicit
  input list. Do not assume SDF sources are unsupported.
- Python only for new FlareDB work. If handed Java code, translate the logic to
  a Python pipeline rather than trying to run Java on FlareDB.

## Test before running on FlareDB

Unit-test transforms with `TestPipeline` and `assert_that` under the
DirectRunner first (see
[../../beam-debugging/references/testing-patterns.md](../../beam-debugging/references/testing-patterns.md)).
Then run a tiny FlareDB trial only after the user approves the pipeline brief
described in `flaredb-skills/SKILL.md`.

## Troubleshooting

- Connection refused to `127.0.0.1:8099`: the instance is not running — `flare up`.
- `ModuleNotFoundError: flaredb_runner`: activate the virtualenv and
  `pip install flaredb-runner`.
- Job failed or produced no compact output: inspect with
  `flare logs <JOB_ID>`; for Beam-level failures see the `beam-debugging` skill.
