from enum import Enum


class Mode(Enum):
  """
  Enum representing the available Sling execution modes.
  
  FULL_REFRESH: Drop and recreate the target table/object
  INCREMENTAL: Update existing records and/or insert new ones
  TRUNCATE: Truncate the target table before loading
  SNAPSHOT: Create a snapshot of the source data
  BACKFILL: Load historical data based on a date range
  """
  FULL_REFRESH = "full-refresh"
  INCREMENTAL = "incremental"
  TRUNCATE = "truncate"
  SNAPSHOT = "snapshot"
  BACKFILL = "backfill"


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