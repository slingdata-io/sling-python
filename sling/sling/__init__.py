import os, sys, tempfile, uuid, json, platform, traceback
from subprocess import PIPE, Popen, STDOUT
from typing import Iterable, List, Union, Dict
from json import JSONEncoder
from .hooks import HookMap, Hook, hooks_to_dict
from .options import SourceOptions, TargetOptions

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

# allows provision of a custom path for sling binary
SLING_BIN = os.getenv("SLING_BINARY")

if not SLING_BIN:
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
    if hasattr(o, 'to_dict'):
      return o.to_dict()
    return o.__dict__


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

class ReplicationStream:
  id: str
  description: str
  mode: str
  object: str
  select: List[str]
  files: List[str]
  where: str
  primary_key: List[str]
  update_key: str
  sql: str
  tags: List[str]
  source_options: SourceOptions
  target_options: TargetOptions
  schedule: str
  disabled: bool
  hooks: HookMap

  def __init__(
          self,
          id: str = None,
          description: str = None,
          mode: str = None,
          object: str = None,
          select: List[str] = [],
          files: List[str] = [],
          where: str = None,
          primary_key: List[str] = [],
          update_key: str = None,
          sql: str = None,
          tags: List[str] = [],
          source_options: Union[SourceOptions, dict]={},
          target_options: Union[TargetOptions, dict]={},
          schedule: str = None,
          disabled: bool = None,
          transforms = None,
          columns = None,
          hooks: Union[HookMap, dict] = None,
  ):
    self.id = id
    self.description = description
    self.mode = mode
    self.object = object
    self.select = select
    self.files = files
    self.where = where
    self.primary_key = primary_key
    self.update_key = update_key
    self.sql = sql
    self.tags = tags
    self.schedule = schedule
    self.transforms = transforms
    self.columns = columns

    if isinstance(hooks, dict):
      hooks = HookMap(**hooks)
    self.hooks = hooks

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
  `hooks` represents the replication level hooks to use.
  `streams` represents a dictionary of streams.
  `env` represents the environment variable to apply.
  `debug` represents the whether the logger should be set at DEBUG level.
  """

  source: str
  target: str
  defaults: ReplicationStream
  hooks: HookMap
  streams: Dict[str, ReplicationStream]
  env: dict
  debug: bool

  file_path: str
  temp_file: str

  def __init__(
          self,
          source: str=None,
          target: str=None,
          defaults: Union[ReplicationStream, dict]={},
          hooks: Union[HookMap, dict] = None,
          streams: Dict[str, Union[ReplicationStream, dict]] = {},
          env: dict={},
          debug=False,
          file_path: str=None
  ):
    self.source: str = source
    self.target: str = target

    if isinstance(hooks, dict):
      hooks = HookMap(**hooks)
    self.hooks = hooks

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

    self.file_path = file_path
    self.temp_file = None

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
    debug = '-d' if self.debug else ''

    if self.file_path:
      return f'{SLING_BIN} run {debug} -r "{self.file_path}"'

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
        hooks=self.hooks,
      )

      json.dump(config, file, cls=JsonEncoder)
    
    return f'{SLING_BIN} run {debug} -r "{self.temp_file}"'
  
  def run(self, return_output=False, env:dict=None, stdin=None):
    cmd = self._prep_cmd()
    env = env or self.env
    return _run(cmd, self.temp_file, return_output=return_output, env=env, stdin=stdin)

class Pipeline:
  """
  Pipeline represents a sling pipeline. Call the `run` method to execute it.

  `steps` represents a list of pipeline steps (Hook objects or dictionaries).
  `env` represents the environment variables to apply.
  `file_path` represents the path to the pipeline YAML file.
  """
  steps: List[Union[Hook, dict]]
  env: dict
  file_path: str
  temp_file: str

  def __init__(
          self,
          steps: List[Union[Hook, dict]] = [],
          env: dict = {},
          file_path: str = None
  ):
    self.steps = steps or []
    self.env = env
    self.file_path = file_path
    self.temp_file = None

  def _prep_cmd(self):
    if self.file_path:
      return f'{SLING_BIN} run -p "{self.file_path}"'

    # generate temp file
    uid = uuid.uuid4()
    temp_dir = tempfile.gettempdir()
    self.temp_file = os.path.join(temp_dir, f'sling-pipeline-{uid}.yaml')

    # dump config
    with open(self.temp_file, 'w') as file:
      config = dict(
        steps=hooks_to_dict(self.steps),
        env=self.env,
      )
      json.dump(config, file, cls=JsonEncoder)
    
    return f'{SLING_BIN} run -p "{self.temp_file}"'
  
  def run(self, return_output=False, env:dict=None, stdin=None):
    """
    Runs the pipeline. Use `return_output` as `True` to return the stdout+stderr output at end. 
    `env` accepts a dictionary which defines the environment.
    """
    cmd = self._prep_cmd()
    env = env or self.env
    return _run(cmd, self.temp_file, return_output=return_output, env=env, stdin=stdin)


class TaskOptions:
  stdout: bool
  debug: bool

  def __init__(self, **kwargs) -> None:
    self.stdout = kwargs.get('stdout')
    self.debug = kwargs.get('debug')


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
    env = env or self.env
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
    env = env or {}
    for k,v in os.environ.items():
      env[k] = env.get(k, v)

    for line in _exec_cmd(cmd, env=env, stdin=stdin):
      if return_output:
        lines.append(line)
      else:
        print(line, flush=True)
    
    if temp_file:
      os.remove(temp_file)

  except Exception as E:
    if temp_file:
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
  for k,v in os.environ.items():
    env[k] = env.get(k, v)

  env['SLING_PACKAGE'] = 'python'
  for pkg in ['dagster', 'airflow', 'temporal', 'orkes']:
    if is_package(pkg):
      env['SLING_PACKAGE'] = pkg

  with Popen(cmd, shell=True, env=env, stdin=stdin, stdout=stdout, stderr=stderr) as proc:
    if stdout and stdout != STDOUT and proc.stdout:
      for line in proc.stdout:
        line = str(line.strip(), 'utf-8', errors='replace')
        yield line

    proc.wait()

    if stderr and stderr != STDOUT and proc.stderr:
      lines = '\n'.join(list(proc.stderr))

    if proc.returncode != 0:
      if len(lines) > 0:
          raise Exception(f'Sling command failed:\n{lines}')
      raise Exception(f'Sling command failed')
