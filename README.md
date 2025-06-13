<p align="center"><img src="https://github.com/slingdata-io/sling-python/raw/main/logo-with-text.png" alt="logo" width="250"/></p>

<p align="center">Slings from a data source to a data target.</p>

## Installation

`pip install sling` or `pip install sling[arrow]` for streaming.

Then you should be able to run `sling --help` from command line.

## Running a Extract-Load Task

### CLI

```shell
sling run --src-conn MY_PG --src-stream myschema.mytable \
  --tgt-conn YOUR_SNOWFLAKE --tgt-object yourschema.yourtable \
  --mode full-refresh
```

Or passing a yaml/json string or file

```shell
cat '
source: MY_POSTGRES
target: MY_SNOWFLAKE

# default config options which apply to all streams
defaults:
  mode: full-refresh
  object: new_schema.{stream_schema}_{stream_table}

streams:
  my_schema.*:
' > /path/to/replication.yaml

sling run -r /path/to/replication.yaml
```

### Using the `Replication` class

Run a replication from file:

```python
import yaml
from sling import Replication

# From a YAML file
replication = Replication(file_path="path/to/replication.yaml")
replication.run()

# Or load into object
with open('path/to/replication.yaml') as file:
  config = yaml.load(file, Loader=yaml.FullLoader)

replication = Replication(**config)

replication.run()
```

Build a replication dynamically:

```python
from sling import Replication, ReplicationStream, Mode

# build sling replication
streams = {}
for (folder, table_name) in list(folders):
  streams[folder] = ReplicationStream(
    mode=Mode.FULL_REFRESH, object=table_name, primary_key='_hash_id')

replication = Replication(
  source='aws_s3',
  target='snowflake',
  streams=streams,
  env=dict(SLING_STREAM_URL_COLUMN='true', SLING_LOADED_AT_COLUMN='true'),
  debug=True,
)

replication.run()
```

### Using the `Sling` Class

For more direct control and streaming capabilities, you can use the `Sling` class, which mirrors the CLI interface.

#### Basic Usage with `run()` method

```python
import os
from sling import Sling, Mode

# Set postgres & snowflake connection
# see https://docs.slingdata.io/connections/database-connections
os.environ["POSTGRES"] = 'postgres://...'
os.environ["SNOWFLAKE"] = 'snowflake://...'

# Database to database transfer
Sling(
    src_conn="postgres",
    src_stream="public.users",
    tgt_conn="snowflake",
    tgt_object="public.users_copy",
    mode=Mode.FULL_REFRESH
).run()

# Database to file
Sling(
    src_conn="postgres", 
    src_stream="select * from users where active = true",
    tgt_object="file:///tmp/active_users.csv"
).run()

# File to database
Sling(
    src_stream="file:///path/to/data.csv",
    tgt_conn="snowflake",
    tgt_object="public.imported_data"
).run()
```


#### Input Streaming - Python Data to Target

> **💡 Tip:** Install `pip install sling[arrow]` for better streaming performance and improved data type handling.

> **📊 DataFrame Support:** The `input` parameter accepts lists of dictionaries, pandas DataFrames, or polars DataFrames. DataFrame support preserves data types when using Arrow format.

> **⚠️ Note:** Be careful with large numbers of `Sling` invocations using `input` or `stream()` methods when working with external systems (databases, file systems). Each call re-opens the connection since it invokes the underlying sling binary. For better performance and connection reuse, consider using the `Replication` class instead, which maintains open connections across multiple operations.

```python
import os
from sling import Sling, Format

# Set postgres connection
# see https://docs.slingdata.io/connections/database-connections
os.environ["POSTGRES"] = 'postgres://...'

# Stream Python data to CSV file
data = [
    {"id": 1, "name": "John", "age": 30},
    {"id": 2, "name": "Jane", "age": 25},
    {"id": 3, "name": "Bob", "age": 35}
]

Sling(
    input=data,
    tgt_object="file:///tmp/output.csv"
).run()

# Stream Python data to database
Sling(
    input=data,
    tgt_conn="postgres",
    tgt_object="public.users"
).run()

# Stream Python data to JSON Lines file
Sling(
    input=data,
    tgt_object="file:///tmp/output.jsonl",
    tgt_options={"format": Format.JSONLINES}
).run()

# Stream from generator (memory efficient for large datasets)
def data_generator():
    for i in range(10000):
        yield {"id": i, "value": f"item_{i}", "timestamp": "2023-01-01"}

Sling(input=data_generator(), tgt_object="file:///tmp/large_dataset.csv").run()

# Stream pandas DataFrame to database
import pandas as pd

df = pd.DataFrame({
    "id": [1, 2, 3, 4],
    "name": ["Alice", "Bob", "Charlie", "Diana"],
    "age": [25, 30, 35, 28],
    "salary": [50000, 60000, 70000, 55000]
})

Sling(
    input=df,
    tgt_conn="postgres",
    tgt_object="public.employees"
).run()

# Stream polars DataFrame to CSV file
import polars as pl

df = pl.DataFrame({
    "product_id": [101, 102, 103],
    "product_name": ["Laptop", "Mouse", "Keyboard"],
    "price": [999.99, 25.50, 75.00],
    "in_stock": [True, False, True]
})

Sling(
    input=df,
    tgt_object="file:///tmp/products.csv"
).run()

# DataFrame with column selection
Sling(
    input=df,
    select=["product_name", "price"],  # Only export specific columns
    tgt_object="file:///tmp/product_prices.csv"
).run()
```

#### Output Streaming with `stream()`

```python
import os
from sling import Sling

# Set postgres connection
# see https://docs.slingdata.io/connections/database-connections
os.environ["POSTGRES"] = 'postgres://...'

# Stream data from database
sling = Sling(
    src_conn="postgres",
    src_stream="public.users",
    limit=1000
)

for record in sling.stream():
    print(f"User: {record['name']}, Age: {record['age']}")

# Stream data from file
sling = Sling(
    src_stream="file:///path/to/data.csv"
)

# Process records one by one (memory efficient)
for record in sling.stream():
    # Process each record
    processed_data = transform_record(record)
    # Could save to another system, send to API, etc.

# Stream with parameters
sling = Sling(
    src_conn="postgres",
    src_stream="public.orders",
    select=["order_id", "customer_name", "total"],
    where="total > 100",
    limit=500
)

records = list(sling.stream())
print(f"Found {len(records)} high-value orders")
```

#### High-Performance Streaming with `stream_arrow()`

> **🚀 Performance:** The `stream_arrow()` method provides the highest performance streaming with full data type preservation by using Apache Arrow's columnar format. Requires `pip install sling[arrow]`.

> **📊 Type Safety:** Unlike `stream()` which may convert data types during CSV serialization, `stream_arrow()` preserves exact data types including integers, floats, timestamps, and more.

```python
import os
from sling import Sling

# Set postgres connection  
# see https://docs.slingdata.io/connections/database-connections
os.environ["POSTGRES"] = 'postgres://...'

# Basic Arrow streaming from database
sling = Sling(src_conn="postgres", src_stream="public.users", limit=1000)

# Get Arrow RecordBatchStreamReader for maximum performance
reader = sling.stream_arrow()

# Convert to Arrow Table for analysis
table = reader.read_all()
print(f"Received {table.num_rows} rows with {table.num_columns} columns")
print(f"Column names: {table.column_names}")
print(f"Schema: {table.schema}")

# Convert to pandas DataFrame with preserved types
if table.num_rows > 0:
    df = table.to_pandas()
    print(df.dtypes)  # Shows preserved data types

# Stream Arrow file with type preservation
sling = Sling(
    src_stream="file:///path/to/data.arrow",
    src_options={"format": "arrow"}
)

reader = sling.stream_arrow()
table = reader.read_all()

# Access columnar data directly (very efficient)
for column_name in table.column_names:
    column = table.column(column_name)
    print(f"{column_name}: {column.type}")

# Process Arrow batches for large datasets (memory efficient)
sling = Sling(
    src_conn="postgres", 
    src_stream="select * from large_table"
)

reader = sling.stream_arrow()
for batch in reader:
    # Process each batch separately to manage memory
    print(f"Processing batch with {batch.num_rows} rows")
    # Convert batch to pandas if needed
    batch_df = batch.to_pandas()
    # Process batch_df...

# Round-trip with Arrow format preservation
import pandas as pd

# Write DataFrame to Arrow file with type preservation
df = pd.DataFrame({
    "id": [1, 2, 3],
    "amount": [100.50, 250.75, 75.25],
    "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    "active": [True, False, True]
})

Sling(
    input=df,
    tgt_object="file:///tmp/data.arrow",
    tgt_options={"format": "arrow"}
).run()

# Read back with full type preservation
sling = Sling(
    src_stream="file:///tmp/data.arrow",
    src_options={"format": "arrow"}
)

reader = sling.stream_arrow()
restored_table = reader.read_all()
restored_df = restored_table.to_pandas()

# Types are exactly preserved (no string conversion)
print(restored_df.dtypes)
assert restored_df['active'].dtype == 'bool'
assert 'datetime64' in str(restored_df['timestamp'].dtype)
```

**Notes:**
- `stream_arrow()` requires PyArrow: `pip install sling[arrow]`
- Cannot be used with a target object (use `run()` instead)
- Provides the best performance for large datasets
- Preserves exact data types including timestamps, decimals, and booleans
- Ideal for analytics workloads and data science applications

#### Round-trip Examples

```python
import os
from sling import Sling

# Set postgres connection
# see https://docs.slingdata.io/connections/database-connections
os.environ["POSTGRES"] = 'postgres://...'

# Python → File → Python
original_data = [
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.2}
]

# Step 1: Python data to file
sling_write = Sling(
    input=original_data,
    tgt_object="file:///tmp/scores.csv"
)
sling_write.run()

# Step 2: File back to Python
sling_read = Sling(
    src_stream="file:///tmp/scores.csv"
)
loaded_data = list(sling_read.stream())

# Python → Database → Python (with transformations)
sling_to_db = Sling(
    input=original_data,
    tgt_conn="postgres",
    tgt_object="public.temp_scores"
)
sling_to_db.run()

sling_from_db = Sling(
    src_conn="postgres", 
    src_stream="select *, score * 1.1 as boosted_score from public.temp_scores",
)
transformed_data = list(sling_from_db.stream())

# DataFrame → Database → DataFrame (with pandas/polars)
import pandas as pd

# Start with pandas DataFrame
df = pd.DataFrame({
    "user_id": [1, 2, 3],
    "purchase_amount": [100.50, 250.75, 75.25],
    "category": ["electronics", "clothing", "books"]
})

# Write DataFrame to database
Sling(
    input=df,
    tgt_conn="postgres",
    tgt_object="public.purchases"
).run()

# Read back with SQL transformations as pandas DataFrame
sling_query = Sling(
    src_conn="postgres",
    src_stream="""
        SELECT category, 
               COUNT(*) as purchase_count,
               AVG(purchase_amount) as avg_amount
        FROM public.purchases 
        GROUP BY category
    """
)
summary_data = list(sling_query.stream())
summary_df = pd.DataFrame(summary_data)
print(summary_df)
```


### Using the `Pipeline` class

Run a [Pipeline](https://docs.slingdata.io/concepts/pipeline):

```python
from sling import Pipeline
from sling.hooks import StepLog, StepCopy, StepReplication, StepHTTP, StepCommand

# From a YAML file
pipeline = Pipeline(file_path="path/to/pipeline.yaml")
pipeline.run()

# Or using Hook objects for type safety
pipeline = Pipeline(
    steps=[
        StepLog(message="Hello world"),
        StepCopy(from_="sftp//path/to/file", to="aws_s3/path/to/file"),
        StepReplication(path="path/to/replication.yaml"),
        StepHTTP(url="https://trigger.webhook.com"),
        StepCommand(command=["ls", "-l"], print_output=True)
    ],
    env={"MY_VAR": "value"}
)
pipeline.run()

# Or programmatically using dictionaries
pipeline = Pipeline(
    steps=[
        {"type": "log", "message": "Hello world"},
        {"type": "copy", "from": "sftp//path/to/file", "to": "aws_s3/path/to/file"},
        {"type": "replication", "path": "path/to/replication.yaml"},
        {"type": "http", "url": "https://trigger.webhook.com"},
        {"type": "command", "command": ["ls", "-l"], "print": True}
    ],
    env={"MY_VAR": "value"}
)
pipeline.run()
```


## Testing

```bash
pytest sling/tests/tests.py -v
pytest sling/tests/test_sling_class.py -v
```
