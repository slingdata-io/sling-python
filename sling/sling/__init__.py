
import os, sys, tempfile, uuid, json, platform, traceback
from subprocess import PIPE, Popen, STDOUT
from typing import Iterable, List, Union, Dict
from json import JSONEncoder

#################################################################
# Logic to import the proper binary for the respective operating 
# systems and architecture. Since the binaries are built in Go, 
# they need to be added to the PyPi sling package via a `MANIFEST.in` file.
# And since there is approximately one binary per OS/ARCH,
# it is necessary to split them out into their own PyPi package
# to avoid exceeding the PyPi quotas. This also allows a faster 
# install via pip and saves bandwidth.

# For development
SLING_BASE = os.path.join(os.path.dirname(__file__), '..', '..', 'sling_base')
insert = lambda f: sys.path.insert(1, os.path.join(SLING_BASE, f))
insert('sling-windows-amd64')
insert('sling-linux-amd64')
insert('sling-linux-arm64')
insert('sling-mac-amd64')
insert('sling-mac-arm64')

SLING_BIN = None

if platform.system() == 'Linux':
  if platform.machine() == 'aarch64':
    exec('from sling_linux_arm64 import SLING_BIN')
  else:
    exec('from sling_linux_amd64 import SLING_BIN')
elif platform.system() == 'Windows':
  if platform.machine() == 'ARM64':
    exec('from sling_windows_arm64 import SLING_BIN')
  else:
    exec('from sling_windows_amd64 import SLING_BIN')
elif platform.system() == 'Darwin':
  if platform.machine() == 'arm64':
    exec('from sling_mac_arm64 import SLING_BIN')
  else:
    exec('from sling_mac_amd64 import SLING_BIN')

#################################################################

is_package = lambda text: any([
    text in line.lower()
    for line in traceback.format_stack()[:-1]])

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
  limit: int
  columns: dict
  transforms: list

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
              limit: int = None,
              columns: dict = {},
              transforms: list = None,
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
    self.limit = limit
    self.columns = columns
    self.transforms = transforms

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
                primary_key: List[str] = [],
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
  table_keys: dict
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
              table_keys: dict = {},
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
    self.table_keys = table_keys
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
                options: Union[TargetOptions, dict]={},
               ) -> None:
    self.conn = conn
    self.object = object

    if isinstance(options, dict):
      options = TargetOptions(**options)

    self.options = options

class TaskOptions:
  stdout: bool

  def __init__(self, **kwargs) -> None:
    self.stdout = kwargs.get('stdout')
    self.debug = kwargs.get('debug')

class ReplicationStream:
  mode: str
  object: str
  primary_key: List[str]
  update_key: str
  sql: str
  source_options: SourceOptions
  target_options: TargetOptions
  disabled: bool

  def __init__(
          self,
          mode: str = None,
          object: str = None,
          primary_key: List[str] = [],
          update_key: str = None,
          sql: str = None,
          source_options: Union[SourceOptions, dict]={},
          target_options: Union[TargetOptions, dict]={},
          disabled: bool = None,
  ):
    self.mode = mode
    self.object = object
    self.primary_key = primary_key
    self.update_key = update_key
    self.sql = sql

    if isinstance(source_options, dict):
      source_options = SourceOptions(**source_options)
    self.source_options = source_options

    if isinstance(target_options, dict):
      target_options = TargetOptions(**target_options)
    self.target_options = target_options

    self.disabled = disabled

  def enable(self):
    self.disabled = False

  def disable(self):
    self.disabled = True


class Replication:
  """
  Task represents a sling replication. Call the `run` method to execute it.

  `source` represents the source connection name.
  `target` represents the target connection name.
  `defaults` represents the default stream properties to use.
  `streams` represents a dictionary of streams.
  `env` represents the environment variable to apply.
  `debug` represents the whether the logger should be set at DEBUG level.
  """

  source: str
  target: str
  defaults: ReplicationStream
  streams: Dict[str, ReplicationStream]
  env: dict
  debug: bool

  def __init__(
          self,
          source: str=None,
          target: str=None,
          defaults: Union[ReplicationStream, dict]={},
          streams: Dict[str, Union[ReplicationStream, dict]] = {},
          env: dict={},
          debug=False
  ):
    self.source: str = source
    self.target: str = target

    if isinstance(defaults, dict):
      defaults = ReplicationStream(**defaults)
    self.defaults = defaults

    if isinstance(streams, dict):
      for key, replication in streams.items():
        if isinstance(replication, dict):
          replication = ReplicationStream(**replication)
        streams[key] = replication
        
    self.streams = streams
    self.env = env
    self.debug = debug

  def add_streams(self, streams: Dict[str, ReplicationStream]):
    self.streams.update(streams)

  def enable_streams(self, stream_names: List[str]):
    for stream_name in stream_names:
      if stream_name in self.streams:
        self.streams[stream_name].enable()

  def disable_streams(self, stream_names: List[str]):
    for stream_name in stream_names:
      if stream_name in self.streams:
        self.streams[stream_name].disable()

  def set_default_mode(self, mode: str):
    self.defaults.mode = mode

  def _prep_cmd(self):

    # generate temp file
    uid = uuid.uuid4()
    temp_dir = tempfile.gettempdir()
    self.temp_file = os.path.join(temp_dir, f'sling-replication-{uid}.json')

    # dump config
    with open(self.temp_file, 'w') as file:
      config = dict(
        source=self.source,
        target=self.target,
        defaults=self.defaults,
        streams=self.streams,
        env=self.env,
      )

      json.dump(config, file, cls=JsonEncoder)
    
    debug = '-d' if self.debug else ''

    return f'{SLING_BIN} run {debug} -r "{self.temp_file}"'
  
  def run(self, return_output=False, env:dict=None, stdin=None):
    cmd = self._prep_cmd()
    return _run(cmd, self.temp_file, return_output=return_output, env=env, stdin=stdin)

class Task:
  """
  Task represents the main object to define a
  sling task. Call the `run` method to execute the task.

  `source` represents the source object using the `Source` class.
  `target` represents the target object using the `Target` class.
  `replication` represents the replication object using the `Replication` class
  `options` represent the options object using the `Options` class.
  """
  source: Source
  target: Target
  options: TaskOptions
  mode: str
  env: dict

  temp_file: str

  def __init__(
      self,
      source: Union[Source, dict]={},
      target: Union[Target, dict]={},
      mode: str = 'full-refresh', 
      options: Union[TaskOptions, dict]={},
      env: dict = {},
    ) -> None:
    if isinstance(source, dict):
      source = Source(**source)
    self.source = source

    if isinstance(target, dict):
      target = Target(**target)
    self.target = target

    self.mode = mode
    self.env = env

    if isinstance(options, dict):
      options = TaskOptions(**options)
    self.options = options

  def _prep_cmd(self):

    # generate temp file
    uid = uuid.uuid4()
    temp_dir = tempfile.gettempdir()
    self.temp_file = os.path.join(temp_dir, f'sling-task-{uid}.json')

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

    return f'{SLING_BIN} run -c "{self.temp_file}"'
  
  def run(self, return_output=False, env:dict=None, stdin=None):
    cmd = self._prep_cmd()
    return _run(cmd, self.temp_file, return_output=return_output, env=env, stdin=stdin)

  def stream(self, env:dict=None, stdin=None) -> Iterable[list]:
    """
    Runs the task and streams the stdout output as iterable. `env` accepts a dictionary which defines the environment. `stdin` can be any stream-like object, which will be used as input stream.
    """
    cmd = self._prep_file()

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
      os.remove(self.temp_file)

# conform to legacy module
Sling = Task

def _run(cmd: str, temp_file: str, return_output=False, env:dict=None, stdin=None):
  """
  Runs the task. Use `return_output` as `True` to return the stdout+stderr output at end. `env` accepts a dictionary which defines the environment.
  """
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
    
    os.remove(temp_file)

  except Exception as E:
    print(f'config file for debugging: {temp_file}')

    if return_output:
      lines.append(str(E))
      raise Exception('\n'.join(lines))
    raise E

  finally:
    pass

  return '\n'.join(lines)

def cli(*args, return_output=False):
  "calls the sling binary with the provided args"
  args = args or sys.argv[1:]
  escape = lambda a: a.replace('"', '\\"')
  cmd = f'''{SLING_BIN} {" ".join([f'"{escape(a)}"' for a in args])}'''
  lines = []
  try:
    stdout = PIPE if return_output else sys.stdout
    stderr = STDOUT if return_output else sys.stderr
    env = { k: v for k,v in os.environ.items() }
    for line in _exec_cmd(cmd, stdin=sys.stdin, stdout=stdout, stderr=stderr, env=env):
      if return_output:
        lines.append(line)
      else:
        print(line, flush=True)
  except Exception as E:
    if return_output:
      raise E
    else:
      return 11

  if return_output:
    return '\n'.join(lines)

  return 0


def _exec_cmd(cmd, stdin=None, stdout=PIPE, stderr=STDOUT, env:dict=None):
  lines = []
  env = env or {}

  for pkg in ['dagster', 'airflow', 'temporal', 'orkes']:
    if is_package(pkg):
      env['SLING_PACKAGE'] = pkg

  with Popen(cmd, shell=True, env=env, stdin=stdin, stdout=stdout, stderr=stderr) as proc:
    if stdout and stdout != STDOUT and proc.stdout:
      for line in proc.stdout:
        line = str(line.strip(), 'utf-8')
        yield line

    proc.wait()

    if stderr and stderr != STDOUT and proc.stderr:
      lines = '\n'.join(list(proc.stderr))

    if proc.returncode != 0:
      raise Exception(f'Sling command failed:\n{lines}')
