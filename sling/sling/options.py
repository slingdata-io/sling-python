from typing import Union
from .enum import Format, Compression

class SourceOptions:
  trim_space: bool
  empty_as_null: bool
  header: bool
  flatten: bool
  fields_per_rec: int
  chunk_size: str
  compression: Union[Compression, str]
  format: Union[Format, str]
  null_if: str
  datetime_format: str
  skip_blank_lines: bool
  delimiter: str
  max_decimals: int
  jmespath: str
  sheet: str
  range: str
  limit: int
  offset: int
  columns: dict
  transforms: list

  def __init__(self, 
              trim_space: bool = None,
              empty_as_null: bool = None,
              header: bool = None,
              flatten: bool = None,
              fields_per_rec: int = None,
              chunk_size: str = None,
              compression: Union[Compression, str] = None,
              format: Union[Format, str] = None,
              null_if: str = None,
              datetime_format: str = None,
              skip_blank_lines: bool = None,
              delimiter: str = None,
              max_decimals: int = None,
              jmespath: str = None,
              sheet: str = None,
              range: str = None,
              limit: int = None,
              offset: int = None,
              columns: dict = {},
              transforms: list = None,
              ) -> None:
    self.trim_space = trim_space
    self.empty_as_null = empty_as_null
    self.header = header
    self.flatten = flatten
    self.fields_per_rec = fields_per_rec
    self.chunk_size = chunk_size
    self.compression = compression
    self.format = format
    self.null_if = null_if
    self.datetime_format = datetime_format
    self.skip_blank_lines = skip_blank_lines
    self.delimiter = delimiter
    self.max_decimals = max_decimals
    self.jmespath = jmespath
    self.sheet = sheet
    self.range = range
    self.limit = limit
    self.offset = offset
    self.columns = columns
    self.transforms = transforms


class TargetOptions:
  header: bool
  compression: Union[Compression, str]
  concurrency: int
  batch_limit: int
  datetime_format: str
  delimiter: str
  file_max_rows: int
  file_max_bytes: int
  format: Union[Format, str]
  max_decimals: int
  use_bulk: bool
  ignore_existing: bool
  delete_missing: bool
  column_casing: str
  add_new_columns: bool
  adjust_column_type: bool
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
              datetime_format: str = None,
              delimiter: str = None,
              file_max_rows: int = None,
              file_max_bytes: int = None,
              format: Union[Format, str] = None,
              max_decimals: int = None,
              use_bulk: bool = None,
              ignore_existing: bool = None,
              delete_missing: bool = None,
              column_casing: str = None,
              add_new_columns: bool = None,
              adjust_column_type: bool = None,
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
    self.datetime_format = datetime_format
    self.delimiter = delimiter
    self.file_max_rows = file_max_rows
    self.file_max_bytes = file_max_bytes
    self.format = format
    self.max_decimals = max_decimals
    self.use_bulk = use_bulk
    self.ignore_existing = ignore_existing
    self.delete_missing = delete_missing
    self.column_casing = column_casing
    self.add_new_columns = add_new_columns
    self.adjust_column_type = adjust_column_type
    self.table_keys = table_keys
    self.table_ddl = table_ddl
    self.table_tmp = table_tmp
    self.pre_sql = pre_sql
    self.post_sql = post_sql
