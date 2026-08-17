# Hook classes for Sling Python package
# These classes are used for building replication configurations that get passed to the Go binary
from typing import Any, List, Union, Dict
from .enum import Mode

# Hook Base Class and Hook Types
class Hook:
  """Base hook class for all hook/step types"""
  
  def __init__(self, 
               id: str = None,
               if_condition: str = None,
               on_failure: str = None,
               **kwargs) -> None:
    self.id = id
    self.if_condition = if_condition  # 'if' is a reserved keyword in Python
    self.on_failure = on_failure
    for key, value in kwargs.items():
      setattr(self, key, value)
  
  def to_dict(self) -> dict:
    """Convert hook to dictionary for serialization"""
    result = {"type": self.get_type()}
    for key, value in self.__dict__.items():
      if value is not None:
        # Handle special case for 'if' keyword
        dict_key = "if" if key == "if_condition" else key
        result[dict_key] = value
    return result
  
  def get_type(self) -> str:
    """Override in subclasses to return hook type"""
    raise NotImplementedError("Subclasses must implement get_type()")

class HookQuery(Hook):
  """Hook for executing SQL queries"""
  
  def __init__(self,
               connection: str,
               query: str,
               transient: bool = None,
               into: str = None,
               transaction: Any = None,
               operation: str = None,
               params: Dict[str, Any] = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.connection = connection
    self.query = query
    self.transient = transient
    self.into = into
    self.transaction = transaction
    self.operation = operation
    self.params = params

  def get_type(self) -> str:
    return "query"

class HookHTTP(Hook):
  """Hook for making HTTP requests"""
  
  def __init__(self,
               url: str,
               method: str = None,
               payload: Any = None,
               headers: Dict[str, str] = None,
               timeout: int = None,
               auth: Dict[str, Any] = None,
               write_to: str = None,
               proxy: str = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.url = url
    self.method = method
    self.payload = payload
    self.headers = headers or {}
    self.timeout = timeout
    self.auth = auth
    self.write_to = write_to
    self.proxy = proxy

  def get_type(self) -> str:
    return "http"

class HookCheck(Hook):
  """Hook for performing data quality checks"""
  
  def __init__(self,
               check: str,
               failure_message: str = None,
               success_goto: str = None,
               success_message: str = None,
               vars: Dict[str, Any] = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.check = check
    self.failure_message = failure_message
    self.success_goto = success_goto
    self.success_message = success_message
    self.vars = vars or {}

  def get_type(self) -> str:
    return "check"

class HookRead(Hook):
  """Hook for reading content from files"""
  
  def __init__(self,
               from_: str,  # 'from' is a reserved keyword
               into: str = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.from_ = from_
    self.into = into
  
  def to_dict(self) -> dict:
    result = super().to_dict()
    if hasattr(self, 'from_'):
      result['from'] = self.from_
      del result['from_']
    return result
  
  def get_type(self) -> str:
    return "read"

class HookWrite(Hook):
  """Hook for writing content to files"""
  
  def __init__(self,
               to: str,
               content: str,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.to = to
    self.content = content
  
  def get_type(self) -> str:
    return "write"

class HookCopy(Hook):
  """Hook for copying files between locations"""
  
  def __init__(self,
               from_: str,  # 'from' is a reserved keyword
               to: str,
               single_file: bool = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.from_ = from_
    self.to = to
    self.single_file = single_file

  def to_dict(self) -> dict:
    result = super().to_dict()
    if hasattr(self, 'from_'):
      result['from'] = self.from_
      del result['from_']
    return result

  def get_type(self) -> str:
    return "copy"

class HookDelete(Hook):
  """Hook for deleting files or directories"""
  
  def __init__(self,
               location: str = None,
               connection: str = None,
               path: str = None,
               recursive: bool = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.location = location
    self.connection = connection
    self.path = path
    self.recursive = recursive
  
  def get_type(self) -> str:
    return "delete"

class HookLog(Hook):
  """Hook for logging messages"""
  
  def __init__(self,
               message: str = None,
               level: str = None,
               log: str = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.message = message
    self.level = level
    self.log = log

  def get_type(self) -> str:
    return "log"

class HookInspect(Hook):
  """Hook for inspecting file metadata"""

  def __init__(self,
               location: str = None,
               connection: str = None,
               object: str = None,
               recursive: bool = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.location = location
    self.connection = connection
    self.object = object
    self.recursive = recursive

  def get_type(self) -> str:
    return "inspect"

class HookList(Hook):
  """Hook for listing files in a directory"""
  
  def __init__(self,
               location: str = None,
               connection: str = None,
               path: str = None,
               recursive: bool = None,
               only: str = None,
               into: str = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.location = location
    self.connection = connection
    self.path = path
    self.recursive = recursive
    self.only = only
    self.into = into

  def get_type(self) -> str:
    return "list"

class HookReplication(Hook):
  """Hook for running another replication"""
  
  def __init__(self,
               path: str = None,
               working_dir: str = None,
               range_param: str = None,  # 'range' is a reserved keyword
               mode: Union[Mode, str] = None,
               streams: List[str] = None,
               env: Dict[str, Any] = None,
               replication: Any = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.path = path
    self.working_dir = working_dir
    self.range_param = range_param
    self.mode = mode
    self.streams = streams or []
    self.env = env or {}
    self.replication = replication
  
  def to_dict(self) -> dict:
    result = super().to_dict()
    if hasattr(self, 'range_param') and self.range_param is not None:
      result['range'] = self.range_param
      del result['range_param']
    return result
  
  def get_type(self) -> str:
    return "replication"

class HookCommand(Hook):
  """Hook for running shell commands"""
  
  def __init__(self,
               command: Union[str, List[str]] = None,
               print_output: bool = None,  # 'print' is a reserved keyword
               capture: bool = None,
               working_dir: str = None,
               env: Dict[str, str] = None,
               timeout: int = None,
               ssh_conn: str = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.command = command or []
    self.print_output = print_output
    self.capture = capture
    self.working_dir = working_dir
    self.env = env or {}
    self.timeout = timeout
    self.ssh_conn = ssh_conn
  
  def to_dict(self) -> dict:
    result = super().to_dict()
    if hasattr(self, 'print_output') and self.print_output is not None:
      result['print'] = self.print_output
      del result['print_output']
    return result
  
  def get_type(self) -> str:
    return "command"

class HookGroup(Hook):
  """Hook for grouping multiple steps with optional looping"""
  
  def __init__(self,
               steps: List[Union[Hook, dict]],
               loop: Any = None,
               env: Dict[str, str] = None,
               params: Dict[str, Any] = None,
               concurrency: int = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.steps = steps or []
    self.loop = loop
    self.env = env or {}
    self.params = params
    self.concurrency = concurrency
  
  def to_dict(self) -> dict:
    result = super().to_dict()
    # Convert step hooks to dictionaries
    if hasattr(self, 'steps'):
      result['steps'] = []
      for step in self.steps:
        if isinstance(step, Hook):
          result['steps'].append(step.to_dict())
        else:
          result['steps'].append(step)
    return result
  
  def get_type(self) -> str:
    return "group"

class HookSet(Hook):
  """Hook for setting values in the runtime state"""

  def __init__(self,
               key: str = None,
               value: Any = None,
               map: Dict[str, Any] = None,
               delete: bool = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.key = key
    self.value = value
    self.map = map
    self.delete = delete

  def get_type(self) -> str:
    return "set"

class HookStore(HookSet):
  """
  Hook for storing values in memory.

  This is the legacy name of `HookSet`. Sling reads both.
  """

  def get_type(self) -> str:
    return "store"

class HookRoutine(Hook):
  """Hook for calling a named routine"""

  def __init__(self,
               routine: str,
               params: Dict[str, Any] = None,
               env: Dict[str, str] = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.routine = routine
    self.params = params
    self.env = env

  def get_type(self) -> str:
    return "routine"

class HookBuild(Hook):
  """Hook for running a build project (for example dbt)"""

  def __init__(self,
               build: str,
               target: str = None,
               select: Union[str, List[str]] = None,
               exclude: Union[str, List[str]] = None,
               vars: Dict[str, Any] = None,
               fail_fast: bool = None,
               full_refresh: bool = None,
               threads: int = None,
               schema: str = None,
               prod: bool = None,
               no_seeds: bool = None,
               range_param: str = None,  # 'range' is a reserved keyword
               recursive: bool = None,
               test: bool = None,
               **kwargs) -> None:
    super().__init__(**kwargs)
    self.build = build
    self.target = target
    self.select = select
    self.exclude = exclude
    self.vars = vars
    self.fail_fast = fail_fast
    self.full_refresh = full_refresh
    self.threads = threads
    self.schema = schema
    self.prod = prod
    self.no_seeds = no_seeds
    self.range_param = range_param
    self.recursive = recursive
    self.test = test

  def to_dict(self) -> dict:
    result = super().to_dict()
    if hasattr(self, 'range_param') and self.range_param is not None:
      result['range'] = self.range_param
      del result['range_param']
    return result

  def get_type(self) -> str:
    return "build"

# Helper function to convert hooks to dictionaries
def hooks_to_dict(hooks: List[Union[Hook, dict]]) -> List[dict]:
  """Convert a list of hooks to dictionaries for serialization"""
  result = []
  for hook in hooks or []:
    if isinstance(hook, Hook):
      result.append(hook.to_dict())
    else:
      result.append(hook)
  return result


class HookMap:
  start: List[Union[Hook, dict]]
  end: List[Union[Hook, dict]]
  pre: List[Union[Hook, dict]]
  post: List[Union[Hook, dict]]
  pre_merge: List[Union[Hook, dict]]
  post_merge: List[Union[Hook, dict]]

  def __init__(self, 
              start: List[Union[Hook, dict]] = None,
              end: List[Union[Hook, dict]] = None,
              pre: List[Union[Hook, dict]] = None,
              post: List[Union[Hook, dict]] = None,
              pre_merge: List[Union[Hook, dict]] = None,
              post_merge: List[Union[Hook, dict]] = None,
              ) -> None:
    self.start = start or []
    self.end = end or []
    self.pre = pre or []
    self.post = post or []
    self.pre_merge = pre_merge or []
    self.post_merge = post_merge or []
  
  def to_dict(self) -> dict:
    """Convert HookMap to dictionary for serialization"""
    result = {}
    if self.start:
      result['start'] = hooks_to_dict(self.start)
    if self.end:
      result['end'] = hooks_to_dict(self.end)
    if self.pre:
      result['pre'] = hooks_to_dict(self.pre)
    if self.post:
      result['post'] = hooks_to_dict(self.post)
    if self.pre_merge:
      result['pre_merge'] = hooks_to_dict(self.pre_merge)
    if self.post_merge:
      result['post_merge'] = hooks_to_dict(self.post_merge)
    return result

# Step aliases for all hook classes
Step = Hook
StepQuery = HookQuery
StepHTTP = HookHTTP
StepCheck = HookCheck
StepRead = HookRead
StepWrite = HookWrite
StepCopy = HookCopy
StepDelete = HookDelete
StepLog = HookLog
StepInspect = HookInspect
StepList = HookList
StepReplication = HookReplication
StepCommand = HookCommand
StepGroup = HookGroup
StepStore = HookStore
StepSet = HookSet
StepRoutine = HookRoutine
StepBuild = HookBuild