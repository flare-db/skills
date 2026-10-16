
# Apache Beam IO Connectors Reference

Complete reference for built-in Beam IO connectors with full read and write examples in Java and Python.

> **Python first.** Use the Python connector for new pipelines; the Java class is
> for Java-only jobs and porting.
>
> **FlareDB note:** bounded sources and bounded Splittable DoFns are supported,
> so `ReadFromText`/`ReadAllFromText` work for text input. Prefer them; use
> `beam.Create` + a file-reading `Map`/`FlatMap` only for custom parsing or an
> explicit input list. Verify file sinks against the current FlareDB docs. See
> [flaredb-runner.md](flaredb-runner.md) and
> [flaredb-features.md](flaredb-features.md) before using a connector on FlareDB.

---

## 1. TextIO — Text Files

Line-by-line text file reading and writing. Supports local filesystems, GCS (`gs://`), S3 (`s3://`), and HDFS (`hdfs://`). Supports wildcards and compression.

### Java

```java
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Compression;

// --- READ ---
// Read from a single file
PCollection<String> lines = pipeline.apply(
    TextIO.read().from("gs://my-bucket/data/input.txt"));

// Read from multiple files using wildcard
PCollection<String> allLines = pipeline.apply(
    TextIO.read().from("gs://my-bucket/data/*.txt"));

// Read with explicit compression
PCollection<String> gzipLines = pipeline.apply(
    TextIO.read()
        .from("gs://my-bucket/data/large.txt.gz")
        .withCompression(Compression.GZIP));

// --- WRITE ---
// Write to sharded files (recommended for production — runner chooses shard count)
lines.apply(TextIO.write().to("gs://my-bucket/output/results"));
// Output: results-00000-of-00005, results-00001-of-00005, ...

// Write with custom suffix
lines.apply(TextIO.write()
    .to("gs://my-bucket/output/results")
    .withSuffix(".txt"));

// Write with compression
lines.apply(TextIO.write()
    .to("gs://my-bucket/output/compressed")
    .withCompression(Compression.GZIP)
    .withSuffix(".txt.gz"));

// Write to a single file (NOT recommended for large data — no parallelism)
lines.apply(TextIO.write()
    .to("gs://my-bucket/output/single")
    .withNumShards(1)
    .withSuffix(".txt"));
```

### Python

```python
import apache_beam as beam

# --- READ ---
lines = pipeline | beam.io.ReadFromText("gs://my-bucket/data/*.txt")

# With compression (auto-detected from .gz extension, or explicit):
gz_lines = pipeline | beam.io.ReadFromText(
    "gs://my-bucket/data/large.txt.gz",
    compression_type=beam.io.filesystem.CompressionTypes.GZIP)

# --- WRITE ---
lines | beam.io.WriteToText("gs://my-bucket/output/results", file_name_suffix=".txt")

# Single shard (dev only):
lines | beam.io.WriteToText(
    "gs://my-bucket/output/single",
    file_name_suffix=".txt",
    num_shards=1)
```

**Caveats:**
- TextIO reads line-by-line only — no structured schema. Parse with `ParDo` or `Map`.
- For Avro/Parquet, use `AvroIO` / `ParquetIO` (see below).
- `withNumShards(1)` eliminates parallelism in writes — only use for testing.

---

## 2. AvroIO — Apache Avro Files

**Java:**
```java
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroDefault;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;

// Define an Avro-serializable POJO
@DefaultCoder(AvroCoder.class)
public class PageView {
    public String url;
    public String userId;
    public long timestampMillis;
    public PageView() {}
    public PageView(String url, String userId, long ts) {
        this.url = url; this.userId = userId; this.timestampMillis = ts;
    }
}

// --- READ --- (typed)
PCollection<PageView> views = pipeline.apply(
    AvroIO.read(PageView.class).from("gs://bucket/input/*.avro"));

// --- READ --- (as GenericRecord for dynamic schemas)
String avroSchemaJson = "{"
    + "\"type\":\"record\",\"name\":\"PageView\",\"fields\":["
    + "{\"name\":\"url\",\"type\":\"string\"},"
    + "{\"name\":\"userId\",\"type\":\"string\"},"
    + "{\"name\":\"timestampMillis\",\"type\":\"long\"}"
    + "]}";
Schema schema = new Schema.Parser().parse(avroSchemaJson);
PCollection<GenericRecord> records = pipeline.apply(
    AvroIO.readGenericRecords(schema).from("gs://bucket/input/*.avro"));

// --- WRITE ---
views.apply(AvroIO.write(PageView.class)
    .to("gs://bucket/output/views")
    .withSuffix(".avro"));
```

**Python:**
```python
import apache_beam as beam

avro_schema = {
    "type": "record",
    "name": "PageView",
    "fields": [
        {"name": "url", "type": "string"},
        {"name": "userId", "type": "string"},
        {"name": "timestampMillis", "type": "long"}
    ]
}

# READ
records = pipeline | beam.io.ReadFromAvro("gs://bucket/input/*.avro")
# Each element is a dict: {"url": "...", "userId": "...", "timestampMillis": ...}

# WRITE
records | beam.io.WriteToAvro(
    "gs://bucket/output/views",
    schema=avro_schema,
    file_name_suffix=".avro")
```

---

## 3. ParquetIO — Apache Parquet Files

**Java:**
```java
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

String parquetSchemaJson = "{"
    + "\"type\":\"record\",\"name\":\"Sale\",\"fields\":["
    + "{\"name\":\"saleId\",\"type\":\"string\"},"
    + "{\"name\":\"amount\",\"type\":\"double\"},"
    + "{\"name\":\"regionCode\",\"type\":\"string\"}"
    + "]}";
Schema parquetSchema = new Schema.Parser().parse(parquetSchemaJson);

// READ
PCollection<GenericRecord> sales = pipeline.apply(
    ParquetIO.read(parquetSchema).from("gs://bucket/data/*.parquet"));

// READ with projection (read only specific columns)
PCollection<GenericRecord> projected = pipeline.apply(
    ParquetIO.read(parquetSchema)
        .from("gs://bucket/data/*.parquet")
        .withProjection(projectionSchema, parquetSchema));

// WRITE
sales.apply(
    ParquetIO.write(parquetSchema)
        .to("gs://bucket/output/sales")
        .withSuffix(".parquet")
        .withCompressionCodec(CompressionCodecName.SNAPPY));
```

**Python:**
```python
import apache_beam as beam
import pyarrow

arrow_schema = pyarrow.schema([
    pyarrow.field("sale_id", pyarrow.string()),
    pyarrow.field("amount", pyarrow.float64()),
    pyarrow.field("region_code", pyarrow.string()),
])

# READ
records = pipeline | beam.io.ReadFromParquet(
    "gs://bucket/data/*.parquet",
    as_rows=False)  # Each element is a dict

# WRITE
records | beam.io.WriteToParquet(
    "gs://bucket/output/sales",
    schema=arrow_schema,
    file_name_suffix=".parquet",
    codec="snappy")
```

---

## 4. KafkaIO — Apache Kafka

KafkaIO reads from and writes to Apache Kafka topics. Reading produces an unbounded `PCollection`.

**Java:**
```java
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

// --- READ ---
PCollection<KafkaRecord<Long, String>> records = pipeline.apply(
    KafkaIO.<Long, String>read()
        .withBootstrapServers("kafka-broker:9092")
        .withTopic("my-input-topic")
        .withKeyDeserializer(LongDeserializer.class)
        .withValueDeserializer(StringDeserializer.class)
        // Option 1: Use processing time as watermark (simple, ignores event time)
        .withProcessingTime()
        // Option 2: Use Kafka's log-append time as event time (recommended for most cases)
        // .withLogAppendTime()
        // Option 3: Use producer create-time with bounded lag (auto-advances on idle partitions)
        // .withCreateTime(Duration.standardSeconds(5))
        .withConsumerConfigUpdates(ImmutableMap.of(
            "auto.offset.reset", "latest",
            "group.id", "my-consumer-group"
        )));

// Extract just the values
PCollection<String> values = records
    .apply("ExtractValues",
        MapElements.into(TypeDescriptors.strings())
            .via(record -> record.getKV().getValue()));

// --- WRITE ---
values.apply("WriteToKafka",
    KafkaIO.<Void, String>write()
        .withBootstrapServers("kafka-broker:9092")
        .withTopic("my-output-topic")
        .withValueSerializer(StringSerializer.class)
        .values());  // write only values (Void key)
```

**Python:**
```python
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka

# READ
records = pipeline | ReadFromKafka(
    consumer_config={
        "bootstrap.servers": "kafka-broker:9092",
        "auto.offset.reset": "latest",
        "group.id": "my-consumer-group"
    },
    topics=["my-input-topic"]
)
# Each element is a (key_bytes, value_bytes) tuple
values = records | beam.Map(lambda kv: kv[1].decode("utf-8"))

# WRITE
values | WriteToKafka(
    producer_config={"bootstrap.servers": "kafka-broker:9092"},
    topic="my-output-topic"
)
```

**Critical Caveats:**
- **Idle partition watermark stall**: If any Kafka partition receives no data, the watermark freezes for all keys on that worker. Fix: use `.withCreateTime(Duration)` (Java) — it auto-advances the watermark to `now - maxDelay` on idle partitions — or set `timestamp_policy=ReadFromKafka.log_append_time` / `ReadFromKafka.create_time_policy` (Python).
- **Serializer/Deserializer types must match**: Mismatched SerDe classes cause silent decode failures or ClassCastExceptions.
- **Consumer group offsets**: Use `auto.offset.reset=earliest` to start from beginning of topic, `latest` to start from current end.

---

## 5. PubsubIO — Google Cloud Pub/Sub

Reads/writes Google Cloud Pub/Sub messages. This is an **unbounded** (streaming) source.

**Java:**
```java
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

// --- READ STRINGS ---
PCollection<String> messages = pipeline.apply(
    PubsubIO.readStrings()
        .fromSubscription("projects/my-project/subscriptions/my-sub"));

// --- READ WITH ATTRIBUTES ---
PCollection<PubsubMessage> fullMessages = pipeline.apply(
    PubsubIO.readMessages()
        .fromSubscription("projects/my-project/subscriptions/my-sub"));

PCollection<String> payloads = fullMessages.apply(
    MapElements.into(TypeDescriptors.strings())
        .via(msg -> new String(msg.getPayload(), StandardCharsets.UTF_8)));

// --- READ WITH EVENT TIME from attribute ---
// Use a message attribute as event timestamp (for proper event-time watermarking)
PCollection<String> timedMessages = pipeline.apply(
    PubsubIO.readStrings()
        .fromSubscription("projects/my-project/subscriptions/my-sub")
        .withTimestampAttribute("event_time"));  // attribute must be epoch millis as string

// --- WRITE STRINGS ---
results.apply(
    PubsubIO.writeStrings()
        .to("projects/my-project/topics/my-output-topic"));

// --- WRITE MESSAGES ---
results
    .apply("WrapMessage", MapElements.via(new SimpleFunction<String, PubsubMessage>() {
        @Override
        public PubsubMessage apply(String msg) {
            return new PubsubMessage(
                msg.getBytes(StandardCharsets.UTF_8),
                ImmutableMap.of("source", "beam-pipeline")
            );
        }
    }))
    .apply(PubsubIO.writeMessages().to("projects/my-project/topics/my-output-topic"));
```

**Python:**
```python
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub

# READ (returns bytes elements)
messages = pipeline | ReadFromPubSub(
    subscription="projects/my-project/subscriptions/my-sub")

# Decode bytes to string
strings = messages | beam.Map(lambda m: m.decode("utf-8"))

# READ with event timestamp from attribute
messages_with_ts = pipeline | ReadFromPubSub(
    subscription="projects/my-project/subscriptions/my-sub",
    timestamp_attribute="event_time")  # attribute = epoch millis as string

# WRITE
strings | beam.Map(lambda s: s.encode("utf-8")) | WriteToPubSub(
    topic="projects/my-project/topics/my-output-topic")
```

**Caveats:**
- PubsubIO is **streaming only** — does not work with `DirectRunner` for windowed aggregations; use the streaming runner the user explicitly requested.
- The DirectRunner supports PubsubIO for testing but with limited exactly-once guarantees.
- Exactly-once delivery requires a runner with built-in deduplication.

---

## 6. BigQueryIO — Google BigQuery

Reads and writes BigQuery tables. Supports both bounded (batch export) and unbounded (streaming inserts) modes.

**Java:**
```java
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;

// --- READ by table reference ---
PCollection<TableRow> rows = pipeline.apply(
    BigQueryIO.readTableRows()
        .from("my-project:my_dataset.my_table"));

// --- READ by SQL query ---
PCollection<TableRow> queryResults = pipeline.apply(
    BigQueryIO.readTableRows()
        .fromQuery("SELECT user_id, SUM(amount) AS total FROM `my-project.dataset.sales` GROUP BY user_id")
        .usingStandardSql());

// --- READ with Storage API (fast for large tables) ---
PCollection<TableRow> fastRead = pipeline.apply(
    BigQueryIO.readTableRows()
        .from("my-project:my_dataset.my_table")
        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ));

// --- WRITE ---
TableSchema schema = new TableSchema().setFields(Arrays.asList(
    new TableFieldSchema().setName("user_id").setType("STRING").setMode("REQUIRED"),
    new TableFieldSchema().setName("total").setType("FLOAT").setMode("REQUIRED"),
    new TableFieldSchema().setName("processed_at").setType("TIMESTAMP").setMode("NULLABLE")
));

results.apply(
    BigQueryIO.writeTableRows()
        .to(new TableReference()
            .setProjectId("my-project")
            .setDatasetId("my_dataset")
            .setTableId("my_output_table"))
        .withSchema(schema)
        // Truncate existing data before writing (batch):
        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
        // Append to existing data:
        // .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        // Create the table if it doesn't exist:
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
```

**Python:**
```python
import apache_beam as beam

# READ
rows = pipeline | beam.io.ReadFromBigQuery(
    query="SELECT user_id, SUM(amount) AS total FROM `my-project.dataset.sales` GROUP BY user_id",
    use_standard_sql=True)
# Each element is a dict: {"user_id": "...", "total": 123.4}

# READ by table
rows = pipeline | beam.io.ReadFromBigQuery(
    table="my-project:my_dataset.my_table",
    method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)

# WRITE
schema = {
    "fields": [
        {"name": "user_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "total", "type": "FLOAT", "mode": "REQUIRED"}
    ]
}

rows | beam.io.WriteToBigQuery(
    table="my-project:my_dataset.my_output_table",
    schema=schema,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
```

**Options:**
- **Streaming inserts** (unbounded): Use `withMethod(STREAMING_INSERTS)`. Higher cost, no ordering guarantee, best-effort deduplication.
- **File loads** (bounded, default): Write files to GCS then load. Atomic, cheaper, but has delay.
- **DIRECT_READ**: Use BigQuery Storage API for 10-100x faster reads than export-to-GCS.

---

## 7. JdbcIO — JDBC / Relational Databases

Read from and write to any JDBC-compatible database (PostgreSQL, MySQL, Oracle, etc.).

**Java:**
```java
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import java.sql.ResultSet;

// --- READ ---
PCollection<UserRecord> users = pipeline.apply(
    JdbcIO.<UserRecord>read()
        .withDataSourceConfiguration(
            JdbcIO.DataSourceConfiguration.create(
                    "org.postgresql.Driver",
                    "jdbc:postgresql://db-host:5432/mydb")
                .withUsername("app_user")
                .withPassword("secret123"))
        .withQuery("SELECT user_id, name, email, score FROM users WHERE active = true")
        .withCoder(SerializableCoder.of(UserRecord.class))
        .withRowMapper((ResultSet rs) -> new UserRecord(
            rs.getString("user_id"),
            rs.getString("name"),
            rs.getString("email"),
            rs.getDouble("score")
        )));

// --- WRITE ---
users.apply(
    JdbcIO.<UserRecord>write()
        .withDataSourceConfiguration(
            JdbcIO.DataSourceConfiguration.create(
                    "org.postgresql.Driver",
                    "jdbc:postgresql://db-host:5432/mydb")
                .withUsername("app_user")
                .withPassword("secret123"))
        .withStatement("INSERT INTO user_stats (user_id, name, score) VALUES (?, ?, ?) ON CONFLICT (user_id) DO UPDATE SET score = EXCLUDED.score")
        .withPreparedStatementSetter((UserRecord record, PreparedStatement stmt) -> {
            stmt.setString(1, record.getUserId());
            stmt.setString(2, record.getName());
            stmt.setDouble(3, record.getScore());
        }));
```

**Python:**
```python
import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc, WriteToJdbc
from typing import NamedTuple

class UserRecord(NamedTuple):
    user_id: str
    name: str
    score: float

jdbc_config = {
    "driverClassName": "org.postgresql.Driver",
    "jdbcUrl": "jdbc:postgresql://db-host:5432/mydb",
    "username": "app_user",
    "password": "secret123"
}

# READ
users = pipeline | ReadFromJdbc(
    table_name="users",
    driver_class_name=jdbc_config["driverClassName"],
    jdbc_url=jdbc_config["jdbcUrl"],
    username=jdbc_config["username"],
    password=jdbc_config["password"],
    query="SELECT user_id, name, score FROM users WHERE active = true"
)

# WRITE
users | WriteToJdbc(
    table_name="user_stats",
    driver_class_name=jdbc_config["driverClassName"],
    jdbc_url=jdbc_config["jdbcUrl"],
    username=jdbc_config["username"],
    password=jdbc_config["password"]
)
```

**Caveats:**
- JdbcIO is **bounded** — use for ETL batch pipelines, not streaming CDC.
- Parallelism depends on how many parallel bundles the runner creates; for large reads, ensure the SQL query can be partitioned (by range, by key hash, etc.).
- Connection count: each parallel bundle opens its own connection. Size the database's `max_connections` accordingly.

---

## 8. Custom Bounded Source

Use when no built-in connector exists for your data source.

**Java:**
```java
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;

public class MyBoundedSource extends BoundedSource<MyRecord> {

    private final String endpoint;

    public MyBoundedSource(String endpoint) { this.endpoint = endpoint; }

    @Override
    public List<? extends BoundedSource<MyRecord>> split(
            long desiredBundleSizeBytes, PipelineOptions options) {
        // Partition the source into parallelizable chunks
        List<MyBoundedSource> splits = new ArrayList<>();
        for (String shard : fetchShards(endpoint)) {
            splits.add(new ShardedMySource(endpoint, shard));
        }
        return splits;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
        return fetchSizeEstimate(endpoint);
    }

    @Override
    public BoundedReader<MyRecord> createReader(PipelineOptions options) {
        return new MyBoundedReader(this);
    }

    @Override
    public Coder<MyRecord> getOutputCoder() {
        return AvroCoder.of(MyRecord.class);
    }

    // --- Inner reader class ---
    private static class MyBoundedReader extends BoundedSource.BoundedReader<MyRecord> {
        private final MyBoundedSource source;
        private Iterator<MyRecord> iterator;
        private MyRecord current;

        MyBoundedReader(MyBoundedSource source) { this.source = source; }

        @Override
        public boolean start() throws IOException {
            iterator = fetchRecords(source.endpoint).iterator();
            return advance();
        }

        @Override
        public boolean advance() {
            if (iterator.hasNext()) { current = iterator.next(); return true; }
            return false;
        }

        @Override
        public MyRecord getCurrent() { return current; }

        @Override
        public BoundedSource<MyRecord> getCurrentSource() { return source; }

        @Override
        public void close() {}
    }
}

// Usage:
PCollection<MyRecord> records = pipeline.apply(Read.from(new MyBoundedSource("https://api.example.com")));
```

---

## 9. IO Best Practices

1. **Never hardcode shard count to 1 in production** — `withNumShards(1)` eliminates write parallelism. Omit it and let the runner choose optimal shard count.
2. **Use cloud object store URIs** — `gs://` for GCS, `s3://` for S3, `hdfs://` for HDFS. Never use local paths (`/tmp/`) — they don't exist on distributed workers.
3. **Kafka**: Always configure a watermark strategy or idle timeout — otherwise idle partitions cause the watermark to stall and downstream windows never fire.
4. **BigQuery**: Prefer `DIRECT_READ` for batch reads of large tables — it bypasses export-to-GCS overhead and can be 10x faster.
5. **JdbcIO**: Pre-create output tables before the pipeline runs, or ensure `CREATE_IF_NEEDED` with a correct schema.
6. **Avro/Parquet vs TextIO**: Use Avro or Parquet for structured data — they are significantly more efficient than CSV (columnar compression, schema enforcement, type safety).
