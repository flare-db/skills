/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * A canonical Apache Beam batch WordCount example.
 *
 * <p>Reads text files, tokenizes into words, counts each word, and writes results.
 *
 * <p>Run with DirectRunner (local):
 * <pre>{@code
 *   mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCountBatch \
 *     -Dexec.args="--inputFile=/path/to/input.txt --outputFile=/tmp/wordcount"
 * }</pre>
 *
 * <p>Run with DataflowRunner:
 * <pre>{@code
 *   mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCountBatch \
 *     -Dexec.args="--runner=DataflowRunner \
 *                  --project=my-project \
 *                  --region=us-central1 \
 *                  --gcpTempLocation=gs://my-bucket/tmp \
 *                  --inputFile=gs://my-bucket/input/*.txt \
 *                  --outputFile=gs://my-bucket/output/wordcount"
 * }</pre>
 */
public class WordCountBatch {

  /** Pipeline options. All parameters are exposed via command-line flags. */
  public interface WordCountOptions extends PipelineOptions {
    @Description("Path of the input file(s) to read. Supports wildcards.")
    @Required
    String getInputFile();
    void setInputFile(String value);

    @Description("Path prefix for output files. Runner appends shard suffixes.")
    @Required
    String getOutputFile();
    void setOutputFile(String value);
  }

  public static void main(String[] args) {
    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        // Step 1: Read lines from input file(s)
        .apply("ReadLines", TextIO.read().from(options.getInputFile()))

        // Step 2: Tokenize each line into individual words
        //   - TypeDescriptors.strings() is required when using a Java lambda
        //     to avoid type-erasure coder inference errors.
        .apply("ExtractWords",
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))

        // Step 3: Drop empty tokens produced by splitting on punctuation
        .apply("FilterEmpty", Filter.by((String word) -> !word.isEmpty()))

        // Step 4: Count occurrences of each unique word
        //   - Count.perElement() returns PCollection<KV<String, Long>>
        .apply("CountPerWord", Count.perElement())

        // Step 5: Format results as "word: count"
        .apply("FormatOutput",
            MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> wordCount) ->
                    wordCount.getKey() + ": " + wordCount.getValue()))

        // Step 6: Write results to output file(s)
        //   - Runner auto-selects shard count for parallelism
        //   - Output: outputFile-00000-of-00005, outputFile-00001-of-00005, ...
        .apply("WriteResults", TextIO.write().to(options.getOutputFile()));

    // Execute the pipeline synchronously (blocks until complete)
    pipeline.run().waitUntilFinish();
  }
}
