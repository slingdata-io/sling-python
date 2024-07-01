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

### From Lib

Run a replication from file:

```python
import yaml
from sling import Replication

with open('path/to/replication.yaml') as file:
  config = yaml.load(file, Loader=yaml.FullLoader)

replication = Replication(**config)

replication.run()
```

Build a replication dynamically:

```python
from sling import Replication, ReplicationStream

# build sling replication
streams = {}
for (folder, table_name) in list(folders):
  streams[folder] = ReplicationStream(mode='full-refresh', object=table_name, primary_key='_hash_id')

replication = Replication(
  source='aws_s3',
  target='snowflake',
  streams=streams,
  env=dict(SLING_STREAM_URL_COLUMN='true', SLING_LOADED_AT_COLUMN='true'),
  debug=True,
)

replication.run()
```

## Config Schema

`--src-conn`/`source.conn` and `--tgt-conn`/`target.conn`  can be a name or URL of a folder:
- `MY_PG` (connection ref in db, profile or env)
- `postgresql://user:password!@host.loc:5432/database`
- `s3://my_bucket/my_folder/file.csv`
- `gs://my_google_bucket/my_folder/file.json`
- `file:///tmp/my_folder/file.csv` (local storage)

`--src-stream`/`source.stream` can be an object name to stream from:
- `TABLE1`
- `SCHEMA1.TABLE2`
- `OBJECT_NAME`
- `select * from SCHEMA1.TABLE3`
- `/path/to/file.sql` (if source conn is DB)

`--tgt-object`/`target.object` can be an object name to write to:
- `TABLE1`
- `SCHEMA1.TABLE2`

### Example as JSON

```json
{
  "source": {
    "conn": "MY_PG_URL",
    "stream": "select * from my_table",
    "options": {}
  },
  "target": {
    "conn": "s3://my_bucket/my_folder/new_file.csv",
    "options": {
      "header": false
    }
  }
}
```
