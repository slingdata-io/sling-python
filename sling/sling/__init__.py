import os, sys, tempfile, uuid, json, traceback, subprocess, csv, warnings
from io import StringIO
from subprocess import PIPE, Popen, STDOUT
from typing import Iterable, List, Union, Dict, Any, Optional, IO
from json import JSONEncoder
from .hooks import HookMap, Hook, hooks_to_dict
from .options import SourceOptions, TargetOptions
from .enum import Mode, Format, Compression
from .bin import SLING_BIN

# Try to import pyarrow, fallback to CSV if not available
_ARROW_WARNING_SHOWN = False
try:
    import pyarrow as pa
    HAS_ARROW = True
except ImportError:
    from .arrow import FakePA
    pa = FakePA()
    HAS_ARROW = False

is_package = lambda text: any([
    text in line.lower()
    for line in traceback.format_stack()[:-1]])

class JsonEncoder(JSONEncoder):
  def default(self, o):
    if hasattr(o, 'to_dict'):
      return o.to_dict()
    elif isinstance(o, Mode):
      return o.value
    elif isinstance(o, Format):
      return o.value
    elif isinstance(o, Compression):
      return o.value
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
  mode: Union[Mode, str]
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
          mode: Union[Mode, str] = None,
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
  Task represents a sling replication. Call the `run` method to execute it. See https://docs.slingdata.io/concepts/replication for details.

  `source` represents the source connection name.
  `target` represents the target connection name.
  `defaults` represents the default stream properties to use.
  `hooks` represents the replication level hooks to use. See https://docs.slingdata.io/concepts/hooks for details.
  `streams` represents a dictionary of streams.
  `env` represents the environment variable to apply. See https://docs.slingdata.io/sling-cli/variables for Sling environment variable keys.
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

  def set_default_mode(self, mode: Union[Mode, str]):
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
  @deprecated Use `Replication` or `Sling` classes instead.
  
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
  mode: Union[Mode, str]
  env: dict

  temp_file: str

  def __init__(
      self,
      source: Union[Source, dict]={},
      target: Union[Target, dict]={},
      mode: Union[Mode, str] = Mode.FULL_REFRESH, 
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



class SlingError(Exception):
    """Custom exception for Sling-related errors"""
    pass


class Sling:
    """
    Sling class that mirrors the sling CLI functionalities.
    
    This class provides a Python interface to the Sling CLI with support for:
    - All CLI parameters
    - Streaming input from Python iterables (memory efficient, uses CSV format)
    - Streaming output to Python iterables (memory efficient)
    
    Usage:
        ```
        # Write data to a target (run method)
        sling = Sling(src_conn="postgres", src_stream="users", tgt_conn="file://", tgt_object="output.csv")
        sling.run()
        
        # With input data to target
        data = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        sling = Sling(input=data, tgt_conn="postgres", tgt_object="users")
        sling.run()
        
        # Get output data as iterator (stream method)
        sling = Sling(src_conn="snowflake", src_stream="public.users")
        for record in sling.stream():
            print(record)
            
        # Stream with target object (just runs normally)
        sling = Sling(src_conn="snowflake.", src_stream="select * from users", tgt_conn="file://", tgt_object="output.csv")
        list(sling.stream())  # Equivalent to sling.run()
        ```
    """
    
    def __init__(
        self,
        # Source parameters
        src_conn: Optional[str] = None,
        src_stream: Optional[str] = None,
        src_options: Optional[Union[SourceOptions, Dict[str, Any]]] = None,
        
        # Target parameters
        tgt_conn: Optional[str] = None,
        tgt_object: Optional[str] = None,
        tgt_options: Optional[Union[TargetOptions, Dict[str, Any]]] = None,
        
        # Stream manipulation
        select: Optional[Union[str, List[str]]] = None,
        where: Optional[str] = None,
        transforms: Optional[Union[str, Dict[str, Any], List[Any]]] = None,
        columns: Optional[Union[str, Dict[str, Any]]] = None,
        streams: Optional[Union[str, List[str]]] = None,
        
        # Mode and limits
        mode: Optional[Union[Mode, str]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        range: Optional[str] = None,
        primary_key: Optional[Union[str, List[str]]] = None,
        update_key: Optional[str] = None,
        
        # Environment and config
        env: Optional[Union[str, Dict[str, Any]]] = None,
        replication: Optional[str] = None,
        pipeline: Optional[str] = None,
        config: Optional[str] = None,
        
        # Logging
        debug: bool = False,
        trace: bool = False,
        
        # Python-specific options
        input: Optional[Iterable[Dict[str, Any]]] = None,
    ):
        """
        Initialize Sling with CLI parameters.
        
        Args:
            src_conn: Source database/storage connection
            src_stream: Source table, file path, or SQL query
            src_options: Source configuration options (SourceOptions instance or dict)
            tgt_conn: Target database connection
            tgt_object: Target table or file path
            tgt_options: Target configuration options (TargetOptions instance or dict)
            select: Columns to select (comma-separated string or list)
            where: WHERE clause for filtering
            transforms: Transform configuration (JSON/YAML string, dict, or list)
            columns: Column type casting (JSON/YAML string or dict)
            streams: Specific streams for replication (comma-separated string or list)
            mode: Load mode (Mode enum or string: full-refresh, incremental, etc.)
            limit: Maximum number of rows
            offset: Number of rows to offset
            range: Range for backfill mode
            primary_key: Primary key for incremental (comma-separated string or list)
            update_key: Update key for incremental
            env: Environment variables (JSON/YAML string or dict)
            replication: Replication config file path
            pipeline: Pipeline config file path
            config: Task config string or file (deprecated)
            debug: Enable debug logging
            trace: Enable trace logging
            input: Python iterable to use as input data
        """
        # Store all parameters
        self.src_conn = src_conn
        self.src_stream = src_stream
        self.src_options = src_options
        self.tgt_conn = tgt_conn
        self.tgt_object = tgt_object
        self.tgt_options = tgt_options
        self.select = select
        self.where = where
        self.transforms = transforms
        self.columns = columns
        self.streams = streams
        self.mode = mode
        self.limit = limit
        self.offset = offset
        self.range = range
        self.primary_key = primary_key
        self.update_key = update_key
        self.env = env
        self.replication = replication
        self.pipeline = pipeline
        self.config = config
        self.debug = debug
        self.trace = trace
        self.input = input
        self.stdout = False
        
    def _format_option(self, value: Union[SourceOptions, TargetOptions, Dict[str, Any], List[Any]]) -> str:
        """Convert option value to JSON string if needed"""
        if hasattr(value, '__dict__'):
            # Handle SourceOptions/TargetOptions objects - convert to dict first
            value_dict = {}
            for key, val in value.__dict__.items():
                if val is not None:
                    value_dict[key] = val
            return json.dumps(value_dict)
        return json.dumps(value)
    
    def _format_list(self, value: Union[str, List[str]]) -> str:
        """Convert list to comma-separated string if needed"""
        if isinstance(value, list):
            return ",".join(value)
        return value
    
    def _build_command(self) -> List[str]:
        """Build the sling command arguments"""
        cmd = [SLING_BIN, "run"]
        
        # Add parameters
        if self.replication:
            cmd.extend(["-r", self.replication])
        if self.pipeline:
            cmd.extend(["-p", self.pipeline])
        if self.config:
            cmd.extend(["-c", self.config])
        
        # Handle source configuration
        if self.input is not None:
            # When input data is provided, we don't add source parameters
            # The sling binary will auto-detect stdin
            pass
        else:
            if self.src_conn:
                # Handle file:// URLs - use LOCAL connection
                if self.src_conn.startswith("file://"):
                    cmd.extend(["--src-conn", "LOCAL"])
                    # If no src_stream specified, use the file path
                    if not self.src_stream:
                        cmd.extend(["--src-stream", self.src_conn])
                    else:
                        cmd.extend(["--src-stream", self.src_stream])
                else:
                    cmd.extend(["--src-conn", self.src_conn])
                    if self.src_stream:
                        cmd.extend(["--src-stream", self.src_stream])
            elif self.src_stream:
                # Just src_stream without src_conn
                cmd.extend(["--src-stream", self.src_stream])
        if self.src_options:
            cmd.extend(["--src-options", self._format_option(self.src_options)])
        if self.tgt_conn:
            # Handle file:// URLs - use LOCAL connection and file:// in tgt_object
            if self.tgt_conn.startswith("file://"):
                cmd.extend(["--tgt-conn", "LOCAL"])
                if not self.tgt_object:
                    cmd.extend(["--tgt-object", self.tgt_conn])
            else:
                cmd.extend(["--tgt-conn", self.tgt_conn])
        if self.tgt_object:
            cmd.extend(["--tgt-object", self.tgt_object])
        if self.tgt_options:
            cmd.extend(["--tgt-options", self._format_option(self.tgt_options)])
        if self.select:
            cmd.extend(["-s", self._format_list(self.select)])
        if self.where:
            cmd.extend(["--where", self.where])
        if self.transforms:
            cmd.extend(["--transforms", self._format_option(self.transforms)])
        if self.columns:
            cmd.extend(["--columns", self._format_option(self.columns)])
        if self.streams:
            cmd.extend(["--streams", self._format_list(self.streams)])
        if self.stdout:
            cmd.append("--stdout")
        if self.env:
            cmd.extend(["--env", self._format_option(self.env)])
        if self.mode:
            cmd.extend(["-m", self.mode])
        if self.limit is not None:
            cmd.extend(["-l", str(self.limit)])
        if self.offset is not None:
            cmd.extend(["-o", str(self.offset)])
        if self.range:
            cmd.extend(["--range", self.range])
        if self.primary_key:
            cmd.extend(["--primary-key", self._format_list(self.primary_key)])
        if self.update_key:
            cmd.extend(["--update-key", self.update_key])
        if self.debug:
            cmd.append("-d")
        if self.trace:
            cmd.append("--trace")
            
        return cmd
    
    def _write_input_data_sync(self, stdin: IO, input_data: Iterable[Dict[str, Any]]):
        """Write input data to stdin, using Arrow IPC format if available, otherwise CSV"""
        if HAS_ARROW and self._should_use_arrow():
            self._write_input_data_arrow(stdin, input_data)
        else:
            self._write_input_data_csv(stdin, input_data)
    
    def _should_use_arrow(self) -> bool:
        """Determine if Arrow format should be used"""
        # Use Arrow if available and not disabled via env var
        return HAS_ARROW and os.environ.get('SLING_USE_ARROW', 'true').lower() != 'false'
    
    def _write_input_data_arrow(self, stdin: IO, input_data: Iterable[Dict[str, Any]]):
        """Write input data to stdin in Arrow IPC format"""
        try:
            # Collect records into batches for efficient Arrow processing
            batch_size = 10000
            current_batch = []
            schema = None
            
            # Determine columns to use
            selected_columns = None
            if self.select:
                if isinstance(self.select, list):
                    selected_columns = self.select
                else:
                    selected_columns = [col.strip() for col in self.select.split(',')]
            
            # Create Arrow IPC stream writer
            writer = None
            
            for record in input_data:
                if not record:
                    continue
                
                # Filter columns if specified
                if selected_columns:
                    record = {k: v for k, v in record.items() if k in selected_columns}
                
                current_batch.append(record)
                
                # Process batch when full
                if len(current_batch) >= batch_size:
                    if schema is None:
                        schema = self._infer_arrow_schema(current_batch)
                        writer = pa.ipc.new_stream(stdin, schema)
                    
                    batch = self._records_to_arrow_batch(current_batch, schema)
                    writer.write_batch(batch)
                    current_batch = []
            
            # Process final batch
            if current_batch:
                if schema is None:
                    schema = self._infer_arrow_schema(current_batch)
                    writer = pa.ipc.new_stream(stdin, schema)
                
                batch = self._records_to_arrow_batch(current_batch, schema)
                writer.write_batch(batch)
            
            # Handle empty input
            if schema is None:
                # Create empty schema and write empty stream
                schema = pa.schema([])
                writer = pa.ipc.new_stream(stdin, schema)
            
            if writer:
                writer.close()
                
        except Exception as e:
            if self.debug:
                sys.stderr.write(f"Error in Arrow input stream: {e}\n")
                sys.stderr.flush()
            raise
    
    def _infer_arrow_schema(self, records: List[Dict[str, Any]]) -> pa.Schema:
        """Infer Arrow schema from a sample of records"""
        if not records:
            return pa.schema([])
        
        # Collect all unique field names
        field_names = set()
        for record in records[:100]:  # Sample first 100 records for schema inference
            field_names.update(record.keys())
        
        fields = []
        for field_name in sorted(field_names):
            # Sample values for type inference
            sample_values = []
            for record in records[:100]:
                if field_name in record and record[field_name] is not None:
                    sample_values.append(record[field_name])
            
            # Infer type from sample values
            arrow_type = self._infer_arrow_type(sample_values)
            fields.append(pa.field(field_name, arrow_type, nullable=True))
        
        return pa.schema(fields)
    
    def _infer_arrow_type(self, sample_values: List[Any]) -> pa.DataType:
        """Infer Arrow data type from sample values"""
        if not sample_values:
            return pa.string()
        
        # Check for boolean
        if all(isinstance(v, bool) for v in sample_values):
            return pa.bool_()
        
        # Check for int64
        if all(isinstance(v, int) and not isinstance(v, bool) for v in sample_values):
            return pa.int64()
        
        # Check for float64
        if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in sample_values):
            return pa.float64()
        
        # Check for timestamp
        import datetime
        if all(isinstance(v, datetime.datetime) for v in sample_values):
            return pa.timestamp('us')
        
        # Check for date
        if all(isinstance(v, datetime.date) for v in sample_values):
            return pa.date32()
        
        # Default to string
        return pa.string()
    
    def _records_to_arrow_batch(self, records: List[Dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
        """Convert a list of records to an Arrow RecordBatch"""
        arrays = []
        for field in schema:
            column_data = []
            for record in records:
                value = record.get(field.name)
                column_data.append(value)
            
            # Create Arrow array with proper type conversion
            try:
                if field.type == pa.bool_():
                    array = pa.array(column_data, type=pa.bool_())
                elif field.type == pa.int64():
                    array = pa.array(column_data, type=pa.int64())
                elif field.type == pa.float64():
                    array = pa.array(column_data, type=pa.float64())
                elif field.type == pa.timestamp('us'):
                    array = pa.array(column_data, type=pa.timestamp('us'))
                elif field.type == pa.date32():
                    array = pa.array(column_data, type=pa.date32())
                else:
                    # Convert to string for safety
                    string_data = [str(v) if v is not None else None for v in column_data]
                    array = pa.array(string_data, type=pa.string())
            except Exception:
                # Fallback to string conversion if type conversion fails
                string_data = [str(v) if v is not None else None for v in column_data]
                array = pa.array(string_data, type=pa.string())
            
            arrays.append(array)
        
        return pa.record_batch(arrays, schema=schema)
    
    def _write_input_data_csv(self, stdin: IO, input_data: Iterable[Dict[str, Any]]):
        """Write input data to stdin in CSV format synchronously"""
        headers = None
        headers_written = False
        record_count = 0
        
        # Determine columns to use
        selected_columns = None
        if self.select:
            if isinstance(self.select, list):
                selected_columns = self.select
            else:
                # Parse comma-separated string
                selected_columns = [col.strip() for col in self.select.split(',')]
        
        try:
            for record in input_data:
                record_count += 1
                if not record:  # Skip empty records but count them
                    continue
                    
                # Initialize headers from first record
                if not headers_written:
                    if selected_columns:
                        # Use only selected columns that exist in the record
                        headers = [col for col in selected_columns if col in record]
                    else:
                        headers = list(record.keys())
                    
                    # Write CSV header
                    csv_buffer = StringIO()
                    csv_writer = csv.writer(csv_buffer)
                    csv_writer.writerow(headers)
                    header_line = csv_buffer.getvalue()
                    if self.debug:
                        sys.stderr.write(f"Debug: Writing headers: {header_line.strip()}\n")
                        sys.stderr.flush()
                    stdin.write(header_line.encode('utf-8'))
                    stdin.flush()
                    headers_written = True
                
                if headers:
                    # Write the record as CSV
                    csv_buffer = StringIO()
                    csv_writer = csv.writer(csv_buffer)
                    # Ensure record has all fields, fill missing with empty string
                    row = [str(record.get(h, '')) for h in headers]
                    csv_writer.writerow(row)
                    csv_line = csv_buffer.getvalue()
                    if self.debug:
                        sys.stderr.write(f"Debug: Writing row: {csv_line.strip()}\n")
                        sys.stderr.flush()
                    stdin.write(csv_line.encode('utf-8'))
                    stdin.flush()
            
            # Handle empty input - write an empty CSV with no headers
            if record_count == 0:
                stdin.write(b'')  # Empty input
                        
        except Exception as e:
            if self.debug:
                print(f"Error in input stream: {e}")
            raise
    
    def _read_output_stream(self, stdout: IO) -> Iterable[Dict[str, Any]]:
        """Read and parse output from stdout, using Arrow IPC format if available, otherwise CSV"""
        if HAS_ARROW and self._should_use_arrow():
            yield from self._read_output_stream_arrow(stdout)
        else:
            yield from self._read_output_stream_csv(stdout)
    
    def _read_output_stream_arrow(self, stdout: IO) -> Iterable[Dict[str, Any]]:
        """Read and parse Arrow IPC output from stdout"""
        try:
            # Create Arrow IPC stream reader
            reader = pa.ipc.open_stream(stdout)
            
            # Read batches and yield records
            for batch in reader:
                # Convert batch to list of dicts
                table = pa.table([batch[i] for i in range(batch.num_columns)], 
                               names=batch.schema.names)
                
                # Convert to Python objects with type preservation
                for row_idx in range(table.num_rows):
                    record = {}
                    for col_idx, column_name in enumerate(table.column_names):
                        column = table.column(col_idx)
                        value = column[row_idx].as_py()  # Converts to native Python type
                        record[column_name] = value
                    
                    if self.debug:
                        sys.stderr.write(f"Debug: Arrow record: {record}\n")
                        sys.stderr.flush()
                    
                    yield record
                    
        except Exception as e:
            if self.debug:
                sys.stderr.write(f"Error reading Arrow output stream: {e}\n")
                sys.stderr.flush()
            # Fallback to CSV parsing if Arrow fails
            try:
                # Reset stdout position if possible
                if hasattr(stdout, 'seek'):
                    stdout.seek(0)
                yield from self._read_output_stream_csv(stdout)
            except:
                return
    
    def _read_output_stream_csv(self, stdout: IO) -> Iterable[Dict[str, Any]]:
        """Read and parse CSV output from stdout"""
        try:
            # Read the first line to get headers
            first_line = stdout.readline()
            if not first_line:
                return
                
            first_line = first_line.decode('utf-8').strip()
            if not first_line:
                return
                
            if self.debug:
                print(f"Debug: First line (headers): '{first_line}'")
                
            # Parse headers
            headers = list(csv.reader([first_line]))[0]
            
            if self.debug:
                print(f"Debug: Parsed headers: {headers}")
            
            # Read and parse remaining lines one by one
            for line in stdout:
                line_str = line.decode('utf-8').strip()
                if line_str:
                    try:
                        if self.debug:
                            print(f"Debug: Processing line: '{line_str}'")
                        values = list(csv.reader([line_str]))[0]
                        # Pad values if fewer than headers
                        while len(values) < len(headers):
                            values.append('')
                        # Truncate values if more than headers
                        values = values[:len(headers)]
                        # Create record dict
                        record = dict(zip(headers, values))
                        if self.debug:
                            print(f"Debug: Created record: {record}")
                        yield record
                    except Exception as e:
                        if self.debug:
                            print(f"Error parsing CSV line '{line_str}': {e}")
                        # Skip malformed lines
                        continue
        except Exception as e:
            if self.debug:
                print(f"Error reading output stream: {e}")
            return
    
    def stream(self) -> Iterable[Dict[str, Any]]:
        """
        Execute the sling command and return output data as an iterator.
        
        If a target object is specified, this will execute normally and return an empty iterator.
        If no target object is specified, this will stream the output data.
        
        Returns:
            An iterator of records from the output stream
        """
        # If target object is specified, just run normally and return empty iterator
        if self.tgt_object:
            self.run()
            return iter([])  # Return empty iterator
        
        # Enable stdout for streaming output
        original_stdout = self.stdout
        self.stdout = True
        
        try:
            cmd = self._build_command()
            
            # Warn about column typing when not using Arrow (only once)
            if not (HAS_ARROW and self._should_use_arrow()):
                global _ARROW_WARNING_SHOWN
                if not _ARROW_WARNING_SHOWN:
                    warnings.warn(
                        "Data typing will be lost during CSV serialization when reading output. "
                        "Install 'pip install sling[arrow]' for better performance and type preservation.",
                        UserWarning,
                        stacklevel=2
                    )
                    _ARROW_WARNING_SHOWN = True
            
            # Prepare environment
            env = dict(os.environ)
            env['SLING_PACKAGE'] = 'python'
            for pkg in ['dagster', 'airflow', 'temporal', 'orkes']:
              if is_package(pkg):
                env['SLING_PACKAGE'] = pkg
            
            # Set Arrow format flag if using Arrow
            if HAS_ARROW and self._should_use_arrow():
                env['SLING_STREAM_FORMAT'] = 'arrow'
            
            # Allow empty files when input is empty
            if self.input is not None:
                env['SLING_ALLOW_EMPTY'] = 'TRUE'
            
            # Setup stdin/stdout for streaming
            stdin = subprocess.PIPE if self.input is not None else subprocess.DEVNULL
            stdout = subprocess.PIPE
            
            try:
                if self.debug:
                    sys.stderr.write(f"Debug: Running streaming command: {' '.join(cmd)}\n")
                    sys.stderr.flush()
                    
                # Start process
                process = subprocess.Popen(
                    cmd,
                    stdin=stdin,
                    stdout=stdout,
                    stderr=subprocess.PIPE,  # Capture stderr for error messages
                    env=env
                )
                
                # Handle input streaming
                if self.input is not None:
                    if self.debug:
                        sys.stderr.write("Debug: Starting input streaming\n")
                        sys.stderr.flush()
                    # Write input data directly in the main thread
                    self._write_input_data_sync(process.stdin, self.input)
                
                # Handle output streaming
                try:
                    yield from self._read_output_stream(process.stdout)
                finally:
                    # Wait for process to complete
                    process.wait()
                    
                    if process.returncode != 0:
                        # Read stderr for error message
                        stderr_output = process.stderr.read().decode('utf-8') if process.stderr else ""
                        raise SlingError(f"Sling command failed with return code: {process.returncode}\n{stderr_output}")
                        
            except Exception as e:
                if isinstance(e, SlingError):
                    raise
                raise SlingError(f"Error executing sling streaming command: {str(e)}")
        finally:
            # Restore original stdout setting
            self.stdout = original_stdout

    def run(self) -> None:
        """
        Execute the sling command and wait for completion.
        
        This method requires a target object to write data to.
        If you need to get output data, use the stream() method instead.
        
        Raises:
            SlingError: If no target object is specified (use stream() instead)
        """
        # Check if target object is specified
        if not self.tgt_object:
            raise SlingError("No target object specified. Use stream() method instead of run() to get output data.")
            
        cmd = self._build_command()
        
        # Prepare environment
        env = dict(os.environ)
        env['SLING_PACKAGE'] = 'python'
        for pkg in ['dagster', 'airflow', 'temporal', 'orkes']:
          if is_package(pkg):
            env['SLING_PACKAGE'] = pkg
        
        # Set Arrow format flag if using Arrow
        if HAS_ARROW and self._should_use_arrow():
            env['SLING_STREAM_FORMAT'] = 'arrow'
        
        # Allow empty files when input is empty
        if self.input is not None:
            env['SLING_ALLOW_EMPTY'] = 'TRUE'
        
        # Setup stdin/stdout
        stdin = subprocess.PIPE if self.input is not None else subprocess.DEVNULL
        
        try:
            # Warn about column typing when using input data without Arrow (only once)
            if self.input is not None and not (HAS_ARROW and self._should_use_arrow()):
                global _ARROW_WARNING_SHOWN
                if not _ARROW_WARNING_SHOWN:
                    warnings.warn(
                        "Data typing will be lost during CSV serialization when providing input data. "
                        "Install 'pip install sling[arrow]' for better performance and type preservation.",
                        UserWarning,
                        stacklevel=2
                    )
                    _ARROW_WARNING_SHOWN = True
            
            if self.debug:
                sys.stderr.write(f"Debug: Running command: {' '.join(cmd)}\n")
                sys.stderr.flush()
                
            # Start process
            process = subprocess.Popen(
                cmd,
                stdin=stdin,
                stdout=subprocess.PIPE,  # Capture stdout for potential output
                stderr=subprocess.PIPE,  # Capture stderr for error messages
                env=env
            )
            
            # Handle input streaming if provided
            if self.input is not None:
                if self.debug:
                    sys.stderr.write("Debug: Starting input streaming\n")
                    sys.stderr.flush()
                # Write input data directly in the main thread
                self._write_input_data_sync(process.stdin, self.input)
            
            # Wait for process to complete
            stdout_output, stderr_output = process.communicate()
            
            # Always decode stderr for debugging
            stderr_text = stderr_output.decode('utf-8') if stderr_output else ""
            
            if self.debug and stderr_text:
                sys.stderr.write(f"Debug: Sling stderr output:\n{stderr_text}")
                sys.stderr.flush()
            
            if process.returncode != 0:
                raise SlingError(f"Sling command failed with return code: {process.returncode}\n{stderr_text}")
            
            # Print stdout if debug mode or if stdout was requested
            if stdout_output and (self.debug or self.stdout):
                print(stdout_output.decode('utf-8'), end='')
                    
        except Exception as e:
            if isinstance(e, SlingError):
                raise
            raise SlingError(f"Error executing sling command: {str(e)}")


