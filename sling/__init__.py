
import os, sys, tempfile, uuid, json, platform
from subprocess import PIPE, Popen, STDOUT
from typing import Iterable, List, Union
from json import JSONEncoder

# set binary
BIN_FOLDER = os.path.join(os.path.dirname(__file__), 'bin')
if platform.system() == 'Linux':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-linux')
elif platform.system() == 'Windows':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-win.exe')
elif platform.system() == 'Darwin':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-mac')

class JsonEncoder(JSONEncoder):
  def default(self, o):
    return o.__dict__

class SourceOptions:
  trim_space: bool
  empty_as_null: bool
  header: bool
  flatten: bool
  compression: str
  format: str
  null_if: str
  datetime_format: str
  skip_blank_lines: bool
  delimiter: str
  max_decimals: int
  jmespath: str
  sheet: str
  range: str
  transforms: list
  columns: dict

  def __init__(self, 
              trim_space: bool = None,
              empty_as_null: bool = None,
              header: bool = None,
              flatten: bool = None,
              compression: str = None,
              format: str = None,
              null_if: str = None,
              datetime_format: str = None,
              skip_blank_lines: bool = None,
              delimiter: str = None,
              max_decimals: int = None,
              jmespath: str = None,
              sheet: str = None,
              range: str = None,
              transforms: list = None,
              columns: dict = None,
              ) -> None:
    self.trim_space = trim_space
    self.empty_as_null = empty_as_null
    self.header = header
    self.flatten = flatten
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
    self.transforms = transforms
    self.columns = columns

class Source:
  conn: str
  stream: str
  primary_key: List[str]
  update_key: str
  limit: int
  options: SourceOptions

  def __init__(self, 
                conn: str = None,
                stream: str = None,
                primary_key: List[str] = None,
                update_key: str = None,
                limit: int = None,
                options: dict = {},
               ) -> None:
    self.conn = conn
    self.stream = stream
    self.primary_key = primary_key
    self.update_key = update_key
    self.limit = limit
    self.options = SourceOptions(**options)


class TargetOptions:
  header: bool
  compression: str
  concurrency: int
  datetime_format: str
  delimiter: str
  file_max_rows: int
  file_max_bytes: int
  format: str
  max_decimals: int
  use_bulk: bool
  column_casing: str
  add_new_columns: bool
  adjust_column_type: bool
  table_ddl: str
  table_tmp: str
  pre_sql: str
  post_sql: str

  def __init__(self, 
              header: bool = None,
              compression: str = None,
              concurrency: int = None,
              datetime_format: str = None,
              delimiter: str = None,
              file_max_rows: int = None,
              file_max_bytes: int = None,
              format: str = None,
              max_decimals: int = None,
              use_bulk: bool = None,
              column_casing: str = None,
              add_new_columns: bool = None,
              adjust_column_type: bool = None,
              table_ddl: str = None,
              table_tmp: str = None,
              pre_sql: str = None,
              post_sql: str = None,
              ) -> None:
    self.header = header
    self.compression = compression
    self.concurrency = concurrency
    self.datetime_format = datetime_format
    self.delimiter = delimiter
    self.file_max_rows = file_max_rows
    self.file_max_bytes = file_max_bytes
    self.format = format
    self.max_decimals = max_decimals
    self.use_bulk = use_bulk
    self.column_casing = column_casing
    self.add_new_columns = add_new_columns
    self.adjust_column_type = adjust_column_type
    self.table_ddl = table_ddl
    self.table_tmp = table_tmp
    self.pre_sql = pre_sql
    self.post_sql = post_sql

class Target:
  conn: str
  object: str
  options: TargetOptions

  def __init__(self, 
                conn: str = None,
                object: str = None,
                options: dict = {},
               ) -> None:
    self.conn = conn
    self.object = object
    self.options = TargetOptions(**options)

class Options:
  stdout: bool

  def __init__(self, **kwargs) -> None:
    self.stdout = kwargs.get('stdout')


class Sling:
  """
  Sling represents the main object to define a
  sling task. Call the `run` method to execute the task.

  `source` represent the source object using the `Source` class.
  `target` represent the target object using the `Target` class.
  `options` represent the optinos object using the `Options` class.
  """
  source: Source
  target: Target
  options: Options
  mode: str
  env: dict

  temp_file: str

  def __init__(self, source: Union[Source, dict]={}, target: Union[Target, dict]={}, mode: str = 'full-refresh', options: Union[Options, dict]={}, env: dict = {}) -> None:
    if isinstance(source, dict):
      source = Source(**source)
    self.source = source

    if isinstance(target, dict):
      target = Target(**target)
    self.target = target

    self.mode = mode
    self.env = env

    if isinstance(options, dict):
      options = Options(**options)
    self.options = options

  def _prep_cmd(self):

    # generate temp file
    uid = uuid.uuid4()
    temp_dir = tempfile.gettempdir()
    self.temp_file = os.path.join(temp_dir, f'sling-cfg-{uid}.json')

    # dump config
    with open(self.temp_file, 'w') as file:
      config = dict(
        source=self.source,
        target=self.target,
        mode=self.mode,
        env=self.env,
        options=self.options,
      )
      json.dump(config, file, cls=JsonEncoder)

    cmd = f'{SLING_BIN} run -c "{self.temp_file}"'

    return cmd

  def _cleanup(self):
      os.remove(self.temp_file)

  def run(self, return_output=False, env:dict=None, stdin=None):
    """
    Runs the task. Use `return_output` as `True` to return the stdout+stderr output at end. `env` accepts a dictionary which defines the environment.
    """
    cmd = self._prep_cmd()
    lines = []
    try:
      for k,v in os.environ.items():
        env = env or {}
        env[k] = env.get(k, v)

      for line in _exec_cmd(cmd, env=env, stdin=stdin):
        if return_output:
          lines.append(line)
        else:
          print(line, flush=True)

    except Exception as E:
      if return_output:
        lines.append(str(E))
        raise Exception('\n'.join(lines))
      raise E

    finally:
      self._cleanup()

    return '\n'.join(lines)

  def stream(self, env:dict=None, stdin=None) -> Iterable[list]:
    """
    Runs the task and streams the stdout output as iterable. `env` accepts a dictionary which defines the environment. `stdin` can be any stream-like object, which will be used as input stream.
    """
    cmd = self._prep_cmd()

    lines = []
    try:
      for k,v in os.environ.items():
        env[k] = env.get(k, v)

      for stdout_line in _exec_cmd(cmd, env=env, stdin=stdin, stderr=PIPE):
        lines.append(stdout_line)
        if len(lines) > 20:
          lines.pop(0) # max size of 100 lines
        yield stdout_line

    except Exception as E:
      lines.append(str(E))
      raise Exception('\n'.join(lines))

    finally:
      self._cleanup()

def cli(*args, return_output=False):
  "calls the sling binary with the provided args"
  args = args or sys.argv[1:]
  escape = lambda a: a.replace('"', '\\"')
  cmd = f'''{SLING_BIN} {" ".join([f'"{escape(a)}"' for a in args])}'''
  lines = []
  try:
    stdout = PIPE if return_output else sys.stdout
    stderr = STDOUT if return_output else sys.stderr
    for line in _exec_cmd(cmd, stdin=sys.stdin, stdout=stdout, stderr=stderr):
      if return_output:
        lines.append(line)
      else:
        print(line, flush=True)
  except Exception as E:
    if return_output:
      raise E
  return '\n'.join(lines)



def _exec_cmd(cmd, stdin=None, stdout=PIPE, stderr=STDOUT, env:dict=None):
  with Popen(cmd, shell=True, env=env, stdin=stdin, stdout=stdout, stderr=stderr) as proc:
    for line in proc.stdout:
      line = str(line.strip(), 'utf-8')
      yield line

    proc.wait()

    lines = line
    if stderr and stderr != STDOUT:
      lines = '\n'.join(list(proc.stderr))

    if proc.returncode != 0:
      raise Exception(f'Sling command failed:\n{lines}')
