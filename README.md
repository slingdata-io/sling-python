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
sling run -c '
source:
  conn: MY_PG
  stream: myschema.mytable

target:
  conn: YOUR_SNOWFLAKE
  object: yourschema.yourtable

mode: full-refresh
'
# OR
sling run -c /path/to/config.json
```

### From Lib

```python
from sling import Sling

config = {
  'source': {
    'conn': 'MY_PG',
    'stream': "select * from my_table",
  },
  'target': {
    'conn':  "s3://my_bucket/my_folder/new_file.csv",
  },
}

Sling(**config).run()
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
