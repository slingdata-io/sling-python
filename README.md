<p align="center"><img src="https://github.com/slingdata-io/sling-python/raw/main/logo-with-text.png" alt="logo" width="250"/></p>

<p align="center">Slings from a data source to a data target.</p>

## Installation

`pip install sling`

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

```python
import os
from sling import Sling

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
