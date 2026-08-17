from typing import Union
from .enum import (
  Format, Compression, MergeStrategy, SlotLevel, ColumnCasing, Encoding,
  IsolationLevel,
)

class SourceOptions:
  empty_as_null: bool
  header: bool
  flatten: bool
  fields_per_rec: int
  chunk_size: str
  chunk_count: int
  chunk_expr: str
  compression: Union[Compression, str]
  format: Union[Format, str]
  null_if: str
  datetime_format: str
  skip_blank_lines: bool
  skip_lines: int
  delimiter: str
  escape: str
  quote: str
  max_decimals: int
  jmespath: str
  jq: str
  sheet: str
  range: str
  limit: int
  offset: int
  encoding: Union[Encoding, str]
  columns: dict
  transforms: list

  def __init__(self,
              empty_as_null: bool = None,
              header: bool = None,
              flatten: bool = None,
              fields_per_rec: int = None,
              chunk_size: str = None,
              chunk_count: int = None,
              chunk_expr: str = None,
              compression: Union[Compression, str] = None,
              format: Union[Format, str] = None,
              null_if: str = None,
              datetime_format: str = None,
              skip_blank_lines: bool = None,
              skip_lines: int = None,
              delimiter: str = None,
              escape: str = None,
              quote: str = None,
              max_decimals: int = None,
              jmespath: str = None,
              jq: str = None,
              sheet: str = None,
              range: str = None,
              limit: int = None,
              offset: int = None,
              encoding: Union[Encoding, str] = None,
              columns: dict = {},
              transforms: list = None,
              ) -> None:
    self.empty_as_null = empty_as_null
    self.header = header
    self.flatten = flatten
    self.fields_per_rec = fields_per_rec
    self.chunk_size = chunk_size
    self.chunk_count = chunk_count
    self.chunk_expr = chunk_expr
    self.compression = compression
    self.format = format
    self.null_if = null_if
    self.datetime_format = datetime_format
    self.skip_blank_lines = skip_blank_lines
    self.skip_lines = skip_lines
    self.delimiter = delimiter
    self.escape = escape
    self.quote = quote
    self.max_decimals = max_decimals
    self.jmespath = jmespath
    self.jq = jq
    self.sheet = sheet
    self.range = range
    self.limit = limit
    self.offset = offset
    self.encoding = encoding
    self.columns = columns
    self.transforms = transforms


class TargetOptions:
  header: bool
  compression: Union[Compression, str]
  concurrency: int
  batch_limit: int
  batch_max_duration: str
  datetime_format: str
  delimiter: str
  file_max_rows: int
  file_max_bytes: int
  format: Union[Format, str]
  max_decimals: int
  use_bulk: bool
  ignore_existing: bool
  delete_missing: bool
  merge_strategy: Union[MergeStrategy, str]
  column_casing: Union[ColumnCasing, str]
  column_typing: dict
  add_new_columns: bool
  adjust_column_type: bool
  encoding: Union[Encoding, str]
  direct_insert: bool
  isolation_level: Union[IsolationLevel, str]
  table_keys: dict
  table_ddl: str
  table_tmp: str
  pre_sql: str
  post_sql: str

  def __init__(self,
              header: bool = None,
              compression: Union[Compression, str] = None,
              concurrency: int = None,
              batch_limit: int = None,
              batch_max_duration: str = None,
              datetime_format: str = None,
              delimiter: str = None,
              file_max_rows: int = None,
              file_max_bytes: int = None,
              format: Union[Format, str] = None,
              max_decimals: int = None,
              use_bulk: bool = None,
              ignore_existing: bool = None,
              delete_missing: bool = None,
              merge_strategy: Union[MergeStrategy, str] = None,
              column_casing: Union[ColumnCasing, str] = None,
              column_typing: dict = None,
              add_new_columns: bool = None,
              adjust_column_type: bool = None,
              encoding: Union[Encoding, str] = None,
              direct_insert: bool = None,
              isolation_level: Union[IsolationLevel, str] = None,
              table_keys: dict = {},
              table_ddl: str = None,
              table_tmp: str = None,
              pre_sql: str = None,
              post_sql: str = None,
              ) -> None:
    self.header = header
    self.compression = compression
    self.concurrency = concurrency
    self.batch_limit = batch_limit
    self.batch_max_duration = batch_max_duration
    self.datetime_format = datetime_format
    self.delimiter = delimiter
    self.file_max_rows = file_max_rows
    self.file_max_bytes = file_max_bytes
    self.format = format
    self.max_decimals = max_decimals
    self.use_bulk = use_bulk
    self.ignore_existing = ignore_existing
    self.delete_missing = delete_missing
    self.merge_strategy = merge_strategy
    self.column_casing = column_casing
    self.column_typing = column_typing
    self.add_new_columns = add_new_columns
    self.adjust_column_type = adjust_column_type
    self.encoding = encoding
    self.direct_insert = direct_insert
    self.isolation_level = isolation_level
    self.table_keys = table_keys
    self.table_ddl = table_ddl
    self.table_tmp = table_tmp
    self.pre_sql = pre_sql
    self.post_sql = post_sql


class CDCOptions:
  """
  Options for change-capture mode. See https://docs.slingdata.io/concepts/replication for details.

  Sling applies its own default for each option you do not set.

  `snapshot_start` sets where the initial load starts. Use "now" or "beginning".
  `snapshot_chunk_size` sets the row count of each initial load chunk.
  `snapshot_run_duration` sets the maximum duration of the initial load, for example "30m".
  `run_max_events` sets the maximum event count of one run.
  `run_max_duration` sets the maximum duration of one run, for example "10m".
  `soft_delete` marks deleted rows in the target, instead of a delete.
  `retry_attempts` sets the retry count after an error.
  `retry_delay` sets the delay between retries, for example "5s".
  `replay_from` sets the source log position to replay from.
  `slot_level` sets the replication slot scope. Use "stream" or "shared".
  `change_feed` names a server-side CDC object. This is a PostgreSQL publication,
  a SQL Server capture instance, or an Oracle GoldenGate data stream.
  """
  snapshot_start: str
  snapshot_chunk_size: int
  snapshot_run_duration: str
  run_max_events: int
  run_max_duration: str
  soft_delete: bool
  retry_attempts: int
  retry_delay: str
  replay_from: str
  slot_level: Union[SlotLevel, str]
  change_feed: str

  def __init__(self,
              snapshot_start: str = None,
              snapshot_chunk_size: int = None,
              snapshot_run_duration: str = None,
              run_max_events: int = None,
              run_max_duration: str = None,
              soft_delete: bool = None,
              retry_attempts: int = None,
              retry_delay: str = None,
              replay_from: str = None,
              slot_level: Union[SlotLevel, str] = None,
              change_feed: str = None,
              ) -> None:
    self.snapshot_start = snapshot_start
    self.snapshot_chunk_size = snapshot_chunk_size
    self.snapshot_run_duration = snapshot_run_duration
    self.run_max_events = run_max_events
    self.run_max_duration = run_max_duration
    self.soft_delete = soft_delete
    self.retry_attempts = retry_attempts
    self.retry_delay = retry_delay
    self.replay_from = replay_from
    self.slot_level = slot_level
    self.change_feed = change_feed

  def to_dict(self) -> dict:
    """Returns the options which are set. Unset options keep the Sling default."""
    return {k: v for k, v in self.__dict__.items() if v is not None}
