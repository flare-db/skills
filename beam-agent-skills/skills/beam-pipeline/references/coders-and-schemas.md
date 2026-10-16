# Apache Beam Coders and Schemas Reference

Complete reference for Beam's serialization system — coders, TypeDescriptors, Schemas, and common coder errors.

> **Python first.** New pipelines default to Python; prefer `NamedTuple`, type
> hints, and `beam.Row` from Section 5. Java `TypeDescriptors` and `@DefaultSchema`
> apply only to Java jobs and porting.

---

## 1. What Is a Coder?

A **Coder** tells Beam how to serialize and deserialize elements of a `PCollection`. Coders are needed when:
- Elements are shuffled across the network (e.g., after `GroupByKey`)
- Elements are persisted to state or checkpointed
- Elements are encoded for I/O (reading/writing files, messaging systems)

If Beam cannot determine the coder for a `PCollection`'s element type, the pipeline fails at construction time with a `Cannot infer coder` error.

**How Beam infers coders:**
1. Check `CoderRegistry` for registered type → coder mappings.
2. Check if the type has a `@DefaultCoder` annotation.
3. Check if the type has a registered Schema.
4. Attempt to use `SerializableCoder` (Java only, must implement `Serializable`).
5. Fail with an error if none of the above apply.

---

## 2. Built-in Coders

| Coder | Encodes | When to Use |
|---|---|---|
| `StringUtf8Coder` | `java.lang.String` / `str` | Text data |
| `ByteArrayCoder` | `byte[]` / `bytes` | Raw binary, custom serialization |
| `VarLongCoder` | `java.lang.Long` / `int` | Integers, counts, timestamps |
| `VarIntCoder` | `java.lang.Integer` | Integers (32-bit) |
| `DoubleCoder` | `java.lang.Double` / `float` | Floating point |
| `BooleanCoder` | `java.lang.Boolean` / `bool` | Flags, predicates |
| `KvCoder<K,V>` | `KV<K,V>` / `(k, v)` tuples | Key-value pairs (for GroupByKey etc.) |
| `IterableCoder<T>` | `Iterable<T>` / `list` | Lists of same-type elements |
| `ListCoder<T>` | `List<T>` | List with random access |
| `NullableCoder<T>` | nullable `T` | Fields that may be null |
| `AvroCoder<T>` | Any Avro-annotated class | POJO with Avro schema (efficient, schema evolution) |
| `SerializableCoder<T>` | Any `java.io.Serializable` | Quick & easy but verbose encoding |
| `RowCoder` | `org.apache.beam.sdk.values.Row` | Beam Schema rows |
| `FastPrimitivesCoder` (Python) | Python primitives | Python strings, ints, floats, bytes |

---

## 3. Java: TypeDescriptors and Type Erasure

### The Problem: Type Erasure with Lambdas

Java erases generic type parameters at runtime. When you write:
```java
MapElements.into(??).via((String x) -> x.length())
```
Beam cannot infer that the output type is `Integer` from the lambda alone — Java erases it at compile time.

### Solution: Explicit TypeDescriptors

`TypeDescriptors` provides static factory methods to create runtime type tokens:

```java
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

// Primitive types
TypeDescriptors.strings()          // TypeDescriptor<String>
TypeDescriptors.integers()         // TypeDescriptor<Integer>
TypeDescriptors.longs()            // TypeDescriptor<Long>
TypeDescriptors.doubles()          // TypeDescriptor<Double>
TypeDescriptors.booleans()         // TypeDescriptor<Boolean>

// Generic types
TypeDescriptors.kvs(
    TypeDescriptors.strings(),
    TypeDescriptors.longs())       // TypeDescriptor<KV<String, Long>>

TypeDescriptors.iterables(
    TypeDescriptors.strings())     // TypeDescriptor<Iterable<String>>

TypeDescriptors.lists(
    TypeDescriptors.strings())     // TypeDescriptor<List<String>>

// Custom class (Beam will use registered coder)
TypeDescriptor.of(MyClass.class)   // TypeDescriptor<MyClass>
```

### When You MUST Provide TypeDescriptors

You **must** provide explicit `TypeDescriptors` whenever you use a **lambda** with `MapElements` or `FlatMapElements`:

```java
// WRONG — Beam cannot infer the output type from a lambda
PCollection<Integer> lengths = words.apply(
    MapElements.via((String word) -> word.length())); // ERROR: Cannot infer coder

// CORRECT — explicit output type
PCollection<Integer> lengths = words.apply(
    MapElements.into(TypeDescriptors.integers())
        .via((String word) -> word.length()));

// CORRECT alternative — anonymous class (Beam can inspect generic type at runtime)
PCollection<Integer> lengths = words.apply(
    MapElements.via(new SimpleFunction<String, Integer>() {
        @Override
        public Integer apply(String input) { return input.length(); }
    }));
```

### Manual Coder Registration

Register a coder for a custom class globally in the pipeline:

```java
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

Pipeline pipeline = Pipeline.create(options);

// Register globally
pipeline.getCoderRegistry()
    .registerCoderForClass(MyClass.class, AvroCoder.of(MyClass.class));

// Or with a type descriptor
pipeline.getCoderRegistry()
    .registerCoderForType(TypeDescriptor.of(MyClass.class), AvroCoder.of(MyClass.class));
```

---

## 4. Java Schemas

Beam Schemas provide a relational-style, language-agnostic type description. Schema-aware `PCollection`s unlock SQL-style transforms (`Select`, `Filter`, `Join`, `Group`).

### 4.1 JavaFieldSchema — Public Fields

```java
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaFieldSchema.class)
public class Transaction {
    public String userId;
    public double amount;
    public long timestampMillis;
    public String currency;

    // Required: no-arg constructor
    public Transaction() {}

    public Transaction(String userId, double amount, long ts, String currency) {
        this.userId = userId;
        this.amount = amount;
        this.timestampMillis = ts;
        this.currency = currency;
    }
}
```

### 4.2 JavaBeanSchema — Getter/Setter Convention

```java
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaBeanSchema.class)
public class UserEvent {
    private String eventType;
    private long userId;
    private double value;

    @SchemaCreate
    public UserEvent(String eventType, long userId, double value) {
        this.eventType = eventType;
        this.userId = userId;
        this.value = value;
    }

    public String getEventType() { return eventType; }
    public long getUserId() { return userId; }
    public double getValue() { return value; }
}
```

### 4.3 Schema-Enabled Transforms (Java)

Once a class has a Schema, you can use relational transforms:

```java
PCollection<Transaction> transactions = pipeline.apply(/* ... */);

// Select specific fields (outputs Schema Row)
PCollection<Row> selected = transactions
    .apply(Select.fieldNames("userId", "amount"));

// Filter by field value
PCollection<Transaction> highValue = transactions
    .apply(Filter.<Transaction>create()
        .whereFieldName("amount", (Double amt) -> amt > 1000.0));

// Group by field and aggregate
PCollection<Row> groupedSums = transactions
    .apply(Group.<Transaction>byFieldNames("currency")
        .aggregateField("amount", Sum.ofDoubles(), "totalAmount"));
```

---

## 5. Python Type Hints and NamedTuples

### 5.1 NamedTuple — Schema-Aware PCollections

Using `typing.NamedTuple` gives the element a schema, enabling Beam to infer coders and use schema-based transforms:

```python
from typing import NamedTuple, Optional
import apache_beam as beam

class Transaction(NamedTuple):
    user_id: str
    amount: float
    timestamp_millis: int
    currency: str

# Beam automatically infers the coder for Transaction
transactions = (
    pipeline
    | beam.Create([
        Transaction("u1", 150.0, 1700000000000, "USD"),
        Transaction("u2", 2000.0, 1700000001000, "EUR"),
    ])
)

# Use type hint to get schema-aware operations
high_value = transactions | beam.Filter(lambda tx: tx.amount > 1000.0)
user_amounts = transactions | beam.Map(lambda tx: (tx.user_id, tx.amount))
```

### 5.2 beam.Row — Ad-Hoc Schema Rows

```python
import apache_beam as beam

# beam.Row creates a schema-aware element without a pre-defined class
events = (
    raw_strings
    | beam.Map(lambda line: beam.Row(
        user_id=line.split(",")[0],
        action=line.split(",")[1],
        ts=int(line.split(",")[2])
    ))
)
```

### 5.3 Type Hints on DoFn.process()

Type hints help Beam infer coders and perform type checking:

```python
from typing import Tuple
import apache_beam as beam
from apache_beam.typehints import typehints

class ParseLineDoFn(beam.DoFn):
    def process(self, element: str):
        parts = element.split(",")
        if len(parts) >= 2:
            yield (parts[0], int(parts[1]))

# Or use annotations decorator:
@beam.typehints.with_input_types(str)
@beam.typehints.with_output_types(Tuple[str, int])
class ParseLineDoFn2(beam.DoFn):
    def process(self, element: str):
        parts = element.split(",")
        yield (parts[0], int(parts[1]))
```

---

## 6. Custom Coder (Java)

When built-in coders don't fit your type:

```java
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import java.io.*;

public class GeoPointCoder extends Coder<GeoPoint> {

    private static final GeoPointCoder INSTANCE = new GeoPointCoder();

    public static GeoPointCoder of() { return INSTANCE; }

    @Override
    public void encode(GeoPoint value, OutputStream outStream) throws CoderException, IOException {
        DataOutputStream dos = new DataOutputStream(outStream);
        dos.writeDouble(value.getLat());
        dos.writeDouble(value.getLon());
        dos.flush();
    }

    @Override
    public GeoPoint decode(InputStream inStream) throws CoderException, IOException {
        DataInputStream dis = new DataInputStream(inStream);
        double lat = dis.readDouble();
        double lon = dis.readDouble();
        return new GeoPoint(lat, lon);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        // Deterministic encoding (same input → same bytes) is required for use as a key in GBK
        // Double encoding is deterministic.
    }
}

// Register and use:
pipeline.getCoderRegistry().registerCoderForClass(GeoPoint.class, GeoPointCoder.of());
```

---

## 7. Avro Integration

### 7.1 AvroCoder

```java
import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

// Annotate with @DefaultCoder to auto-use AvroCoder
@DefaultCoder(AvroCoder.class)
public class PageView {
    public String url;
    public String userId;
    public long timestampMillis;
    @Nullable public String referrer;      // nullable field
    @AvroDefault("\"desktop\"") public String deviceType; // default value
}

// Manually specify AvroCoder:
PCollection<PageView> views = pipeline
    .apply(/* read source */)
    .setCoder(AvroCoder.of(PageView.class));
```

> **Dependency.** `AvroCoder` lives in the `org.apache.beam.sdk.extensions.avro.coders`
> package and requires the `beam-sdks-java-extensions-avro` artifact (plus `org.apache.avro:avro`)
> on the classpath. The historical `org.apache.beam.sdk.coders.AvroCoder` was removed in
> Beam 2.x; importing it will not compile.

### 7.2 Reading/Writing Avro Files

```java
// Read
PCollection<PageView> views = pipeline.apply(
    AvroIO.read(PageView.class).from("gs://bucket/input/*.avro"));

// Read as GenericRecord (for dynamic schemas)
Schema avroSchema = new Schema.Parser().parse(schemaJson);
PCollection<GenericRecord> records = pipeline.apply(
    AvroIO.readGenericRecords(avroSchema).from("gs://bucket/input/*.avro"));

// Write
views.apply(
    AvroIO.write(PageView.class)
        .to("gs://bucket/output/views")
        .withSuffix(".avro"));
```

**Python:**
```python
import apache_beam as beam

# Read Avro
records = pipeline | beam.io.ReadFromAvro("gs://bucket/input/*.avro")

# Write Avro (requires schema)
import fastavro
avro_schema = {
    "type": "record",
    "name": "PageView",
    "fields": [
        {"name": "url", "type": "string"},
        {"name": "userId", "type": "string"},
        {"name": "timestampMillis", "type": "long"}
    ]
}
records | beam.io.WriteToAvro(
    "gs://bucket/output/views",
    schema=avro_schema,
    file_name_suffix=".avro"
)
```

### 7.3 Schema Evolution Considerations

- **Adding nullable fields**: Safe — old data reads as `null`, new data includes value.
- **Renaming fields**: Requires alias in the new schema; old data uses the alias.
- **Removing fields**: Safe for reads (old data ignores the missing field), but breaks writes that still supply it.
- **Changing field types**: Generally NOT safe — causes `AvroTypeException` at decode time.

---

## 8. Common Coder Errors and Fixes

| Error Message | Root Cause | Fix |
|---|---|---|
| `No coder registered for Tuple2` | Beam doesn't know how to encode a Python/Java generic tuple | In Java: use `KvCoder` explicitly. In Python: use `NamedTuple` or register a coder. |
| `Cannot infer coder for type T` | Java generics erased; Beam can't determine `T` at runtime | Use `TypeDescriptors.xxx()` in `MapElements`/`FlatMapElements`; annotate class with `@DefaultSchema`. |
| `CoderException: Failed to encode` | Element contains a value the coder cannot handle (e.g., `null` in a non-nullable coder) | Wrap with `NullableCoder.of(innerCoder)`, or filter nulls before encoding. |
| `java.io.NotSerializableException` inside coder | `SerializableCoder` used on a non-serializable class | Switch to `AvroCoder` or `@DefaultSchema(JavaFieldSchema.class)`, or implement `Serializable`. |
| `Schema mismatch` | Reading Avro data with a schema that doesn't match the written schema | Ensure schemas are compatible; use Avro schema evolution rules (aliases for renames, nullable for removals). |
| `ClassCastException: cannot cast Foo to Bar` | Coder registered for wrong class, or mismatched generic type parameters in `KvCoder` | Verify coder registry entries; ensure `KvCoder<K,V>` matches actual element types. |
| Python `PicklingError: Can't pickle ...` | A closure captures an unpicklable object (e.g., lambda with module-level import, threading.Lock) | Move logic to a `DoFn` class; avoid closures; set `save_main_session=True`. |
