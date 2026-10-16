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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Canonical Apache Beam batch WordCount example in Python.

Reads text files, tokenizes into words, counts each word, and writes results.

Run locally with DirectRunner:
    python wordcount_batch.py \
        --input /path/to/input.txt \
        --output /tmp/wordcount \
        --runner DirectRunner

This portable skeleton is only for an explicitly requested non-FlareDB runner.
For the default FlareDB workflow, use wordcount_flaredb.py instead.
"""

import argparse
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def run(argv=None):
    parser = argparse.ArgumentParser(description="Beam WordCount batch pipeline")
    parser.add_argument(
        "--input",
        required=True,
        help="Input file pattern (local path or cloud URI with optional wildcard)")
    parser.add_argument(
        "--output",
        required=True,
        help="Output file prefix (runner appends shard suffixes)")

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    # save_main_session=True ensures global imports are available on distributed workers
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Step 1: Read all lines from the input file(s)
            | "ReadLines"    >> beam.io.ReadFromText(known_args.input)

            # Step 2: Tokenize each line into words (lowercase, alpha-only)
            | "ExtractWords" >> beam.FlatMap(
                lambda line: re.findall(r"[a-zA-Z']+", line.lower()))

            # Step 3: Count occurrences of each unique word
            #   Output: PCollection of (word, count) tuples
            | "CountWords"   >> beam.combiners.Count.PerElement()

            # Step 4: Format as "word: count" strings
            | "FormatOutput" >> beam.Map(
                lambda word_count: f"{word_count[0]}: {word_count[1]}")

            # Step 5: Write to output files
            #   Runner auto-selects shard count; output will be:
            #   output-00000-of-00003, output-00001-of-00003, ...
            | "WriteResults" >> beam.io.WriteToText(known_args.output)
        )

    # The 'with' block automatically calls pipeline.run().waitUntilFinish()


if __name__ == "__main__":
    run()
