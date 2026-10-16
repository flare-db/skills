---
name: flaredb-skills
description: Process large amounts of data without reading it into your context. Use this skill whenever a task involves many files, rows, logs or records (auditing hundreds of files, analysing a large CSV, triaging thousands of CI logs, running an LLM or a function over many items) and you only need a small result. Write an Apache Beam Python pipeline, run it on a local FlareDB instance with the `flare` CLI, then continue from its compact output. Also use this skill to install or update the flare CLI, start or stop FlareDB, submit Beam Python jobs with FlareRunner, read job logs, or whenever the user mentions FlareDB, flare, or running Beam pipelines locally. Always get the user's approval before running any pipeline.
---

# FlareDB: offload bulk data work to a local Beam pipeline

FlareDB is a local Apache Beam runner. Instead of reading thousands of lines or files into your context, write a small Beam Python pipeline that extracts only what you need, run it on FlareDB, and continue your task from the compact result.

## When to use it

Use FlareDB when all of these hold:
- The input is too large to read directly (many files, a large CSV, many log files, many records).
- The answer you need is small: counts, top-N, per-item one-line findings, a filtered subset.
- The work is per-item processing, aggregation, or calling a function or model once per item.

Do not use it when:
- The input is small enough to read directly.
- A single query on one file is enough and a simpler tool (for example a SQL one-liner or grep) is already available. Prefer the simplest tool that keeps data out of your context.

## Hard rule: ask before running

Never submit a pipeline (including a trial run on a small subset) until the user has explicitly approved it. Before running, send a pipeline brief, then stop and wait. See "Approval gate" below. Installing software also needs the user's confirmation, because the installer is a remote script piped to bash.

## Workflow

1. Check the CLI: `flare --version`. If missing, install it (step 1 below, after the user confirms).
2. One-time setup: `flare init`.
3. Start an instance: `flare up` (listens on port 8099).
4. Create a Python environment and install the runner.
5. Write the pipeline.
6. Approval gate: present the brief, wait for a yes.
7. Run it and capture the `JOB-ID`.
8. Verify the result, then continue the task from the compact output.
9. If you started the instance and the user no longer needs it, `flare down`.

## 1. Install the CLI

Supported on Linux and macOS. On Windows, use WSL.

```bash
curl -sSL https://install.flare-db.com | bash
```

Confirm with the user first, then run it. If `flare` is not found afterwards, read the installer output for the install location and PATH instructions and open a new shell. Re-running the same installer upgrades the CLI.

## 2. Initialise and start

```bash
flare init    # one-time: creates ~/.flaredb, downloads the FlareDB engine and Beam Java worker
flare up      # start a local instance on port 8099
```

The instance stores its data under `~/.flaredb`. Stop it with `flare down`.

## CLI reference

| Command | What it does |
| - | - |
| `flare init` | One-time setup of local directories and components. |
| `flare up` | Start a local instance on port 8099. |
| `flare down` | Stop the running instance. |
| `flare logs` | Stream logs for the most recent job (follows continuously). |
| `flare logs <JOB_ID>` | Stream logs for a specific job. |
| `flare version` | Show installed versions and available updates. |
| `flare update` | Update the engine and worker. `--db` or `--java-worker` for one component. |
| `flare --help` | List all commands. |

Notes for agents:
- `flare logs` is interactive or follow continuously. Run them with a timeout or in a way that cannot hang your session, and do not wait on an open prompt.
- To update: `flare down`, then `flare update`, then `flare up`.

## 3. Python environment

Requires Python 3.9+. Docs examples use Apache Beam 2.76.0. The `flaredb-runner` package pulls in `flare` and `apache-beam`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install flaredb-runner
```

## 4. Pipeline skeleton

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from flaredb_runner.flare_runner import FlareRunner

pipeline_options = PipelineOptions(
    job_endpoint="127.0.0.1:8099",   # required
    job_name="my-job",               # optional
)

with beam.Pipeline(runner=FlareRunner(), options=pipeline_options) as p:
    (
        p
        | "Inputs" >> beam.io.ReadFromText("data/*.txt")  # bounded source/SDF
        | "Process" >> beam.Map(process_one)               # per-item work
        # | "Aggregate" >> beam.CombinePerKey(sum) # or GroupByKey, as needed
    )
```

Options can also be passed on the command line: `python3 pipeline.py --job_endpoint=127.0.0.1:8099 --job_name=my-job`.

Nothing needs to be staged manually in Python: FlareDB ships the pipeline code to the workers.
For an explicit input list or custom parsing, `beam.Create([...])` plus a
file-reading `Map`/`FlatMap` is still available, but it is not a workaround for
an unsupported SDF.

### Design the pipeline to save tokens

- Reduce inside the pipeline. Emit counts, top-N, or one short line per item, never raw content.
- Keep the final output small enough to read in full (aim for under about 50 lines, or write details to a file and read only the summary).
- Put the questions you will want answered later into the output now, so you do not need a second run.
- Make per-item functions defensive: catch exceptions per item and emit an error record instead of failing the job.
- Never modify or delete the inputs. The pipeline should be read-only on the user's data.

### Known limitations

FlareDB is young. Check https://docs.flare-db.com/llms.txt for current status before relying on any of these:
- Unbounded sources are not supported. Bounded sources and bounded Splittable
  DoFns are supported, so `ReadFromText` / `ReadAllFromText` work.
- Per the capability matrix, global/fixed/sliding windows, triggers, allowed
  lateness, and bag/value state with timers are supported. Session/custom
  windows, `SetState`/`MapState`, SDF side inputs, metrics, and
  runner-initiated splitting are not.
- For persisting results as tables, see the IO docs at https://docs.flare-db.com/io and confirm Python support before using them.

## Approval gate

After writing the pipeline and before running anything, send the user a brief in plain language, then stop and wait for an answer. Use this format:

```
Pipeline brief
- Goal and why: <the task this serves and why a pipeline beats reading the data directly>
- Reads: <paths, approximate number of files/rows, total size>
- Does: <up to 5 plain-language steps>
- Writes / side effects: <outputs and where; confirm inputs are never modified>
- External calls and cost: <network or LLM API calls and approximate count, or "none">
- Expected runtime and output size: <estimate>
- Run plan: <for example a trial on 5 items, then the full run>
- Alternative considered: <the simpler option and why it was not chosen>

Approve running this? Tell me anything you'd like changed.
```

Rules:
- Run only after an explicit yes. Silence, a thumbs-up on something else, or "looks interesting" is not approval.
- If the user asks for changes, apply them and re-confirm when the change is material (different inputs, new side effects, new external calls, larger scope).
- Approval covers the run plan in the brief. If a trial run shows the pipeline needs significant changes, send a new brief.
- If the user declines, fall back to the simpler approach or ask what they prefer.

## 5. Run and verify

```bash
python3 pipeline.py
```

The runner logs a `JOB-ID` at submission. Note it.

Then verify before continuing:
- The job finished successfully (use `flare logs <JOB_ID>` if anything looks wrong).
- The number of processed items matches the number of inputs, and the count of error records is acceptable.
- A spot check of a few results looks right.

Continue the user's task from the compact result. Report what was run, the JOB-ID, and any items that errored.

## Troubleshooting

| Symptom | Fix |
| - | - |
| Connection refused to `127.0.0.1:8099` | The instance is not running. Run `flare up`. |
| `flare: command not found` | Open a new shell, or follow the PATH hint from the installer output. |
| `ModuleNotFoundError: flaredb_runner` | Activate the virtualenv and run `pip install flaredb-runner`. |
| Unexpected runner or worker errors | Run `flare version`, then `flare down`, `flare update`, `flare up`. |
| Job failed | Read `flare logs <JOB_ID>`, fix the cause, and send a new brief if the change is material. |

## Documentation

- Index of all pages: https://docs.flare-db.com/llms.txt (fetch this first when unsure)
- Quickstart: https://docs.flare-db.com/quickstart
- Python: https://docs.flare-db.com/python
- CLI reference: https://docs.flare-db.com/cli
- Example pipeline: https://github.com/flare-db/flare-db/tree/main/example/python/wordcount.py
