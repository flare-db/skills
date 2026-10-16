# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
FlareDB batch WordCount example in Python.

FlareDB supports bounded sources and bounded Splittable DoFns, so this example
reads text directly with ReadAllFromText. The final output is compact: one
"word<TAB>count" line per word.

Before running, follow flaredb-skills: create a venv, install flaredb-runner,
start FlareDB with `flare up`, and get the user's approval for this run plan.

    python3 wordcount_flaredb.py /data/a.txt /data/b.txt
"""

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from flaredb_runner.flare_runner import FlareRunner


def format_count(word_count):
    """Format a (word, count) tuple as one compact line."""
    word, count = word_count
    return f"{word}\t{count}"


def run(argv=None):
    parser = argparse.ArgumentParser(description="FlareDB batch WordCount")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="One or more input file paths",
    )
    known_args, _ = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        job_endpoint="127.0.0.1:8099",
        job_name="wordcount-flaredb",
    )

    with beam.Pipeline(runner=FlareRunner(), options=pipeline_options) as p:
        (
            p
            | "InputPaths"  >> beam.Create(known_args.inputs)
            | "ReadLines"   >> beam.io.ReadAllFromText()
            | "Tokenize"    >> beam.FlatMap(lambda line: line.lower().split())
            | "CountWords"  >> beam.combiners.Count.PerElement()
            | "Format"      >> beam.Map(format_count)
        )


if __name__ == "__main__":
    run()
