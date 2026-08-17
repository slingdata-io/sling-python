from enum import Enum


class Mode(Enum):
  """
  Enum representing the available Sling execution modes.

  FULL_REFRESH: Drop and recreate the target table/object
  INCREMENTAL: Update existing records and/or insert new ones
  TRUNCATE: Truncate the target table before loading
  SNAPSHOT: Create a snapshot of the source data
  BACKFILL: Load historical data based on a date range
  DEFINITION_ONLY: Create the target table/object definition, without data
  CHANGE_CAPTURE: Stream row changes from the source database log (CDC)
  """
  FULL_REFRESH = "full-refresh"
  INCREMENTAL = "incremental"
  TRUNCATE = "truncate"
  SNAPSHOT = "snapshot"
  BACKFILL = "backfill"
  DEFINITION_ONLY = "definition-only"
  CHANGE_CAPTURE = "change-capture"


class Format(Enum):
  """
  Enum representing the available Sling file formats.
  
  CSV: Comma-separated values format
  JSON: JavaScript Object Notation format
  JSONLINES: JSON Lines format (newline-delimited JSON)
  XML: Extensible Markup Language format
  XLSX: Excel spreadsheet format
  PARQUET: Apache Parquet columnar format
  AVRO: Apache Avro binary format
  SAS: SAS7BDAT file format
  RAW: Raw/binary file format
  """
  CSV = "csv"
  JSON = "json"
  JSONLINES = "jsonlines"
  XML = "xml"
  XLSX = "xlsx"
  PARQUET = "parquet"
  ARROW = "arrow"
  AVRO = "avro"
  SAS = "sas7bdat"
  RAW = "raw"


class Compression(Enum):
  """
  Enum representing the available Sling compression types.

  AUTO: Auto-detect compression type
  NONE: No compression
  ZIP: ZIP compression
  GZIP: Gzip compression
  SNAPPY: Snappy compression (high-speed compression/decompression)
  ZSTD: ZStandard compression (high compression ratio)
  """
  AUTO = "auto"
  NONE = "none"
  ZIP = "zip"
  GZIP = "gzip"
  SNAPPY = "snappy"
  ZSTD = "zstd"


class MergeStrategy(Enum):
  """
  Enum representing the available Sling merge strategies for incremental/backfill modes.

  UPDATE_INSERT: Update existing rows, insert new rows (standard upsert behavior)
  DELETE_INSERT: Delete matching rows, then insert all (safe and reliable)
  INSERT: Insert only, skip existing (append-only scenarios)
  UPDATE: Update only, skip new (update existing records only)
  HISTORY_INSERT: Insert each version of a row, to keep a history
  CHANGE_CAPTURE: Apply CDC events, with hard deletes (change-capture mode)
  CHANGE_CAPTURE_SOFT: Apply CDC events, with soft deletes (change-capture mode)
  """
  UPDATE_INSERT = "update_insert"
  DELETE_INSERT = "delete_insert"
  INSERT = "insert"
  UPDATE = "update"
  HISTORY_INSERT = "history_insert"
  CHANGE_CAPTURE = "change_capture"
  CHANGE_CAPTURE_SOFT = "change_capture_soft"


class SlotLevel(Enum):
  """
  Enum representing how the replication slot/reader is scoped in change-capture mode.

  STREAM: Use one replication slot for each stream
  SHARED: Use one replication slot for all streams of the source connection
  """
  STREAM = "stream"
  SHARED = "shared"
