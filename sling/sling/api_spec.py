# API Spec classes for Sling Python package
# These classes are used for building API specification YAML files programmatically
from enum import Enum
from typing import List, Union, Dict, Any, Optional


# ──────────────────────────────────────────────────────────────
# Enums
# ──────────────────────────────────────────────────────────────

class AuthType(Enum):
  NONE = ""
  STATIC = "static"
  SEQUENCE = "sequence"
  BASIC = "basic"
  OAUTH2 = "oauth2"
  AWS_SIGV4 = "aws-sigv4"
  HMAC = "hmac"
  MTLS = "mtls"

class OAuthFlow(Enum):
  CLIENT_CREDENTIALS = "client_credentials"
  AUTHORIZATION_CODE = "authorization_code"
  DEVICE_CODE = "device_code"

class HTTPMethod(Enum):
  GET = "GET"
  HEAD = "HEAD"
  POST = "POST"
  PUT = "PUT"
  PATCH = "PATCH"
  DELETE = "DELETE"
  CONNECT = "CONNECT"
  OPTIONS = "OPTIONS"
  TRACE = "TRACE"

class AggregationType(Enum):
  NONE = ""
  MAXIMUM = "maximum"
  MINIMUM = "minimum"
  COLLECT = "collect"
  FIRST = "first"
  LAST = "last"

class RuleAction(Enum):
  RETRY = "retry"
  CONTINUE = "continue"
  STOP = "stop"
  BREAK = "break"
  SKIP = "skip"
  FAIL = "fail"

class BackoffType(Enum):
  NONE = ""
  CONSTANT = "constant"
  LINEAR = "linear"
  EXPONENTIAL = "exponential"
  JITTER = "jitter"

class ResponseFormat(Enum):
  NONE = ""
  CSV = "csv"
  JSON = "json"
  JSONLINES = "jsonlines"
  PARQUET = "parquet"
  AVRO = "avro"
  XML = "xml"
  EXCEL = "excel"
  SAS = "sas"
  TEXT = "text"


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _to_dict(obj):
  """Call obj.to_dict() if available, else return obj as-is."""
  if hasattr(obj, 'to_dict'):
    return obj.to_dict()
  return obj

def _enum_val(v):
  """Return v.value if Enum, else v."""
  if isinstance(v, Enum):
    return v.value
  return v

def _convert_list(items, cls, remap_if=False):
  """Convert a list of dicts to typed objects."""
  if items is None:
    return None
  result = []
  for item in items:
    if isinstance(item, dict):
      d = dict(item)
      if remap_if and 'if' in d:
        d['if_condition'] = d.pop('if')
      result.append(cls(**d))
    else:
      result.append(item)
  return result

def _list_to_dict(items):
  """Convert list of objects with to_dict() to list of dicts."""
  if items is None:
    return None
  return [_to_dict(item) for item in items]

def _remap_response_modifiers(d):
  """Remap +rules/rules+/+processors/processors+ to Python-valid names."""
  mapping = {
    '+rules': 'prepend_rules',
    'rules+': 'append_rules',
    '+processors': 'prepend_processors',
    'processors+': 'append_processors',
  }
  for yaml_key, py_key in mapping.items():
    if yaml_key in d:
      d[py_key] = d.pop(yaml_key)

def _remap_endpoint_modifiers(d):
  """Remap +setup/setup+/+teardown/teardown+ and delegate response remapping."""
  mapping = {
    '+setup': 'prepend_setup',
    'setup+': 'append_setup',
    '+teardown': 'prepend_teardown',
    'teardown+': 'append_teardown',
  }
  for yaml_key, py_key in mapping.items():
    if yaml_key in d:
      d[py_key] = d.pop(yaml_key)
  # remap response modifiers if response is a dict
  if 'response' in d and isinstance(d['response'], dict):
    _remap_response_modifiers(d['response'])


# ──────────────────────────────────────────────────────────────
# Classes
# ──────────────────────────────────────────────────────────────

class Request:
  url: str
  timeout: Union[int, str]
  method: Union[HTTPMethod, str]
  headers: Dict[str, str]
  parameters: dict
  payload: Any
  rate: Union[int, float]
  concurrency: int

  def __init__(self,
               url: str = None,
               timeout: Union[int, str] = None,
               method: Union[HTTPMethod, str] = None,
               headers: Dict[str, str] = None,
               parameters: dict = None,
               payload: Any = None,
               rate: Union[int, float] = None,
               concurrency: int = None,
               ) -> None:
    self.url = url
    self.timeout = timeout
    self.method = method
    self.headers = headers
    self.parameters = parameters
    self.payload = payload
    self.rate = rate
    self.concurrency = concurrency

  def to_dict(self) -> dict:
    d = {}
    if self.url is not None:
      d['url'] = self.url
    if self.timeout is not None:
      d['timeout'] = self.timeout
    if self.method is not None:
      d['method'] = _enum_val(self.method)
    if self.headers is not None:
      d['headers'] = self.headers
    if self.parameters is not None:
      d['parameters'] = self.parameters
    if self.payload is not None:
      d['payload'] = self.payload
    if self.rate is not None:
      d['rate'] = self.rate
    if self.concurrency is not None:
      d['concurrency'] = self.concurrency
    return d


class Pagination:
  next_state: dict
  stop_condition: str

  def __init__(self,
               next_state: dict = None,
               stop_condition: str = None,
               ) -> None:
    self.next_state = next_state
    self.stop_condition = stop_condition

  def to_dict(self) -> dict:
    d = {}
    if self.next_state is not None:
      d['next_state'] = self.next_state
    if self.stop_condition is not None:
      d['stop_condition'] = self.stop_condition
    return d


class Records:
  jmespath: str
  jq: str
  primary_key: Union[str, List[str]]
  update_key: str
  limit: int
  casing: str
  select: list
  duplicate_tolerance: str

  def __init__(self,
               jmespath: str = None,
               jq: str = None,
               primary_key: Union[str, List[str]] = None,
               update_key: str = None,
               limit: int = None,
               casing: str = None,
               select: list = None,
               duplicate_tolerance: str = None,
               ) -> None:
    self.jmespath = jmespath
    self.jq = jq
    self.primary_key = primary_key
    self.update_key = update_key
    self.limit = limit
    self.casing = casing
    self.select = select
    self.duplicate_tolerance = duplicate_tolerance

  def to_dict(self) -> dict:
    d = {}
    if self.jmespath is not None:
      d['jmespath'] = self.jmespath
    if self.jq is not None:
      d['jq'] = self.jq
    if self.primary_key is not None:
      d['primary_key'] = self.primary_key
    if self.update_key is not None:
      d['update_key'] = self.update_key
    if self.limit is not None:
      d['limit'] = self.limit
    if self.casing is not None:
      d['casing'] = self.casing
    if self.select is not None:
      d['select'] = self.select
    if self.duplicate_tolerance is not None:
      d['duplicate_tolerance'] = self.duplicate_tolerance
    return d


class Processor:
  expression: str
  output: str
  aggregation: Union[AggregationType, str]
  if_condition: str  # serialized as "if" in YAML

  def __init__(self,
               expression: str = None,
               output: str = None,
               aggregation: Union[AggregationType, str] = None,
               if_condition: str = None,
               **kwargs,
               ) -> None:
    self.expression = expression
    self.output = output
    self.aggregation = aggregation
    # accept 'if' from dict unpacking
    self.if_condition = if_condition or kwargs.get('if')

  def to_dict(self) -> dict:
    d = {}
    if self.expression is not None:
      d['expression'] = self.expression
    if self.output is not None:
      d['output'] = self.output
    if self.aggregation is not None:
      d['aggregation'] = _enum_val(self.aggregation)
    if self.if_condition is not None:
      d['if'] = self.if_condition
    return d


class Rule:
  action: Union[RuleAction, str]
  condition: str
  max_attempts: int
  backoff: Union[BackoffType, str]
  backoff_base: Union[int, float]
  message: str

  def __init__(self,
               action: Union[RuleAction, str] = None,
               condition: str = None,
               max_attempts: int = None,
               backoff: Union[BackoffType, str] = None,
               backoff_base: Union[int, float] = None,
               message: str = None,
               ) -> None:
    self.action = action
    self.condition = condition
    self.max_attempts = max_attempts
    self.backoff = backoff
    self.backoff_base = backoff_base
    self.message = message

  def to_dict(self) -> dict:
    d = {}
    if self.action is not None:
      d['action'] = _enum_val(self.action)
    if self.condition is not None:
      d['condition'] = self.condition
    if self.max_attempts is not None:
      d['max_attempts'] = self.max_attempts
    if self.backoff is not None:
      d['backoff'] = _enum_val(self.backoff)
    if self.backoff_base is not None:
      d['backoff_base'] = self.backoff_base
    if self.message is not None:
      d['message'] = self.message
    return d


class Iterate:
  over: Union[str, list]
  into: str
  concurrency: int

  def __init__(self,
               over: Union[str, list] = None,
               into: str = None,
               concurrency: int = None,
               ) -> None:
    self.over = over
    self.into = into
    self.concurrency = concurrency

  def to_dict(self) -> dict:
    d = {}
    if self.over is not None:
      d['over'] = self.over
    if self.into is not None:
      d['into'] = self.into
    if self.concurrency is not None:
      d['concurrency'] = self.concurrency
    return d


class Response:
  format: Union[ResponseFormat, str]
  records: Union['Records', dict]
  processors: List[Union['Processor', dict]]
  rules: List[Union['Rule', dict]]
  prepend_rules: List[Union['Rule', dict]]
  append_rules: List[Union['Rule', dict]]
  prepend_processors: List[Union['Processor', dict]]
  append_processors: List[Union['Processor', dict]]

  def __init__(self,
               format: Union[ResponseFormat, str] = None,
               records: Union['Records', dict] = None,
               processors: List[Union['Processor', dict]] = None,
               rules: List[Union['Rule', dict]] = None,
               prepend_rules: List[Union['Rule', dict]] = None,
               append_rules: List[Union['Rule', dict]] = None,
               prepend_processors: List[Union['Processor', dict]] = None,
               append_processors: List[Union['Processor', dict]] = None,
               **kwargs,
               ) -> None:
    # handle modifier keys from **kwargs (e.g. from direct dict unpacking)
    prepend_rules = prepend_rules or kwargs.get('+rules')
    append_rules = append_rules or kwargs.get('rules+')
    prepend_processors = prepend_processors or kwargs.get('+processors')
    append_processors = append_processors or kwargs.get('processors+')
    self.format = format
    self.records = Records(**records) if isinstance(records, dict) else records
    self.processors = _convert_list(processors, Processor, remap_if=True)
    self.rules = _convert_list(rules, Rule)
    self.prepend_rules = _convert_list(prepend_rules, Rule)
    self.append_rules = _convert_list(append_rules, Rule)
    self.prepend_processors = _convert_list(prepend_processors, Processor, remap_if=True)
    self.append_processors = _convert_list(append_processors, Processor, remap_if=True)

  def to_dict(self) -> dict:
    d = {}
    if self.format is not None:
      d['format'] = _enum_val(self.format)
    if self.records is not None:
      d['records'] = _to_dict(self.records)
    if self.processors is not None:
      d['processors'] = _list_to_dict(self.processors)
    if self.rules is not None:
      d['rules'] = _list_to_dict(self.rules)
    if self.prepend_rules is not None:
      d['+rules'] = _list_to_dict(self.prepend_rules)
    if self.append_rules is not None:
      d['rules+'] = _list_to_dict(self.append_rules)
    if self.prepend_processors is not None:
      d['+processors'] = _list_to_dict(self.prepend_processors)
    if self.append_processors is not None:
      d['processors+'] = _list_to_dict(self.append_processors)
    return d


class Call:
  if_condition: str  # serialized as "if" in YAML
  request: Union['Request', dict]
  pagination: Union['Pagination', dict]
  response: Union['Response', dict]
  authentication: dict
  iterate: str
  into: str

  def __init__(self,
               request: Union['Request', dict] = None,
               pagination: Union['Pagination', dict] = None,
               response: Union['Response', dict] = None,
               authentication: dict = None,
               iterate: str = None,
               into: str = None,
               if_condition: str = None,
               **kwargs,
               ) -> None:
    # accept 'if' from dict unpacking
    self.if_condition = if_condition or kwargs.get('if')
    self.request = Request(**request) if isinstance(request, dict) else request
    self.pagination = Pagination(**pagination) if isinstance(pagination, dict) else pagination
    if isinstance(response, dict):
      rd = dict(response)
      _remap_response_modifiers(rd)
      self.response = Response(**rd)
    else:
      self.response = response
    self.authentication = authentication
    self.iterate = iterate
    self.into = into

  def to_dict(self) -> dict:
    d = {}
    if self.if_condition is not None:
      d['if'] = self.if_condition
    if self.request is not None:
      d['request'] = _to_dict(self.request)
    if self.pagination is not None:
      d['pagination'] = _to_dict(self.pagination)
    if self.response is not None:
      d['response'] = _to_dict(self.response)
    if self.authentication is not None:
      d['authentication'] = self.authentication
    if self.iterate is not None:
      d['iterate'] = self.iterate
    if self.into is not None:
      d['into'] = self.into
    return d


def _convert_calls(items):
  """Convert a list of dicts to Call objects."""
  if items is None:
    return None
  result = []
  for item in items:
    if isinstance(item, dict):
      d = dict(item)
      if 'if' in d:
        d['if_condition'] = d.pop('if')
      result.append(Call(**d))
    else:
      result.append(item)
  return result


class Endpoint:
  name: str
  description: str
  docs: str
  disabled: bool
  state: dict
  sync: list
  request: Union['Request', dict]
  pagination: Union['Pagination', dict]
  response: Union['Response', dict]
  iterate: Union['Iterate', dict, str]
  setup: List[Union['Call', dict]]
  teardown: List[Union['Call', dict]]
  depends_on: list
  overrides: dict
  authentication: dict
  # modifier fields
  prepend_setup: List[Union['Call', dict]]
  append_setup: List[Union['Call', dict]]
  prepend_teardown: List[Union['Call', dict]]
  append_teardown: List[Union['Call', dict]]

  def __init__(self,
               name: str = None,
               description: str = None,
               docs: str = None,
               disabled: bool = None,
               state: dict = None,
               sync: list = None,
               request: Union['Request', dict] = None,
               pagination: Union['Pagination', dict] = None,
               response: Union['Response', dict] = None,
               iterate: Union['Iterate', dict, str] = None,
               setup: List[Union['Call', dict]] = None,
               teardown: List[Union['Call', dict]] = None,
               depends_on: list = None,
               overrides: dict = None,
               authentication: dict = None,
               prepend_setup: List[Union['Call', dict]] = None,
               append_setup: List[Union['Call', dict]] = None,
               prepend_teardown: List[Union['Call', dict]] = None,
               append_teardown: List[Union['Call', dict]] = None,
               **kwargs,
               ) -> None:
    # handle modifier keys from **kwargs (e.g. from direct dict unpacking)
    prepend_setup = prepend_setup or kwargs.get('+setup')
    append_setup = append_setup or kwargs.get('setup+')
    prepend_teardown = prepend_teardown or kwargs.get('+teardown')
    append_teardown = append_teardown or kwargs.get('teardown+')
    self.name = name
    self.description = description
    self.docs = docs
    self.disabled = disabled
    self.state = state
    self.sync = sync
    self.request = Request(**request) if isinstance(request, dict) else request
    self.pagination = Pagination(**pagination) if isinstance(pagination, dict) else pagination
    if isinstance(response, dict):
      rd = dict(response)
      _remap_response_modifiers(rd)
      self.response = Response(**rd)
    else:
      self.response = response
    # iterate can be a string (shorthand), dict, or Iterate object
    if isinstance(iterate, dict):
      self.iterate = Iterate(**iterate)
    else:
      self.iterate = iterate
    self.setup = _convert_calls(setup)
    self.teardown = _convert_calls(teardown)
    self.depends_on = depends_on
    self.overrides = overrides
    self.authentication = authentication
    self.prepend_setup = _convert_calls(prepend_setup)
    self.append_setup = _convert_calls(append_setup)
    self.prepend_teardown = _convert_calls(prepend_teardown)
    self.append_teardown = _convert_calls(append_teardown)

  def to_dict(self) -> dict:
    d = {}
    if self.name is not None:
      d['name'] = self.name
    if self.description is not None:
      d['description'] = self.description
    if self.docs is not None:
      d['docs'] = self.docs
    if self.disabled is not None:
      d['disabled'] = self.disabled
    if self.state is not None:
      d['state'] = self.state
    if self.sync is not None:
      d['sync'] = self.sync
    if self.request is not None:
      d['request'] = _to_dict(self.request)
    if self.pagination is not None:
      d['pagination'] = _to_dict(self.pagination)
    if self.response is not None:
      d['response'] = _to_dict(self.response)
    if self.iterate is not None:
      d['iterate'] = _to_dict(self.iterate)
    if self.setup is not None:
      d['setup'] = _list_to_dict(self.setup)
    if self.teardown is not None:
      d['teardown'] = _list_to_dict(self.teardown)
    if self.depends_on is not None:
      d['depends_on'] = self.depends_on
    if self.overrides is not None:
      d['overrides'] = self.overrides
    if self.authentication is not None:
      d['authentication'] = self.authentication
    if self.prepend_setup is not None:
      d['+setup'] = _list_to_dict(self.prepend_setup)
    if self.append_setup is not None:
      d['setup+'] = _list_to_dict(self.append_setup)
    if self.prepend_teardown is not None:
      d['+teardown'] = _list_to_dict(self.prepend_teardown)
    if self.append_teardown is not None:
      d['teardown+'] = _list_to_dict(self.append_teardown)
    return d


class DynamicEndpoint:
  setup: List[Union['Call', dict]]
  iterate: str
  into: str
  endpoint: Union['Endpoint', dict]

  def __init__(self,
               setup: List[Union['Call', dict]] = None,
               iterate: str = None,
               into: str = None,
               endpoint: Union['Endpoint', dict] = None,
               ) -> None:
    self.setup = _convert_calls(setup)
    self.iterate = iterate
    self.into = into
    if isinstance(endpoint, dict):
      ed = dict(endpoint)
      _remap_endpoint_modifiers(ed)
      self.endpoint = Endpoint(**ed)
    else:
      self.endpoint = endpoint

  def to_dict(self) -> dict:
    d = {}
    if self.setup is not None:
      d['setup'] = _list_to_dict(self.setup)
    if self.iterate is not None:
      d['iterate'] = self.iterate
    if self.into is not None:
      d['into'] = self.into
    if self.endpoint is not None:
      d['endpoint'] = _to_dict(self.endpoint)
    return d


class ApiSpec:
  name: str
  description: str
  queues: list
  authentication: dict
  defaults: Union['Endpoint', dict]
  endpoints: Dict[str, Union['Endpoint', dict]]
  dynamic_endpoints: List[Union['DynamicEndpoint', dict]]

  def __init__(self,
               name: str = None,
               description: str = None,
               queues: list = None,
               authentication: dict = None,
               defaults: Union['Endpoint', dict] = None,
               endpoints: Dict[str, Union['Endpoint', dict]] = None,
               dynamic_endpoints: List[Union['DynamicEndpoint', dict]] = None,
               ) -> None:
    self.name = name
    self.description = description
    self.queues = queues
    self.authentication = authentication
    if isinstance(defaults, dict):
      dd = dict(defaults)
      _remap_endpoint_modifiers(dd)
      self.defaults = Endpoint(**dd)
    else:
      self.defaults = defaults
    # convert endpoint dicts
    if endpoints is not None:
      self.endpoints = {}
      for k, v in endpoints.items():
        if isinstance(v, dict):
          ed = dict(v)
          _remap_endpoint_modifiers(ed)
          self.endpoints[k] = Endpoint(**ed)
        else:
          self.endpoints[k] = v
    else:
      self.endpoints = None
    # convert dynamic endpoint dicts
    if dynamic_endpoints is not None:
      self.dynamic_endpoints = []
      for item in dynamic_endpoints:
        if isinstance(item, dict):
          self.dynamic_endpoints.append(DynamicEndpoint(**item))
        else:
          self.dynamic_endpoints.append(item)
    else:
      self.dynamic_endpoints = None

  def to_dict(self) -> dict:
    d = {}
    if self.name is not None:
      d['name'] = self.name
    if self.description is not None:
      d['description'] = self.description
    if self.queues is not None:
      d['queues'] = self.queues
    if self.authentication is not None:
      d['authentication'] = self.authentication
    if self.defaults is not None:
      d['defaults'] = _to_dict(self.defaults)
    if self.endpoints is not None:
      d['endpoints'] = {k: _to_dict(v) for k, v in self.endpoints.items()}
    if self.dynamic_endpoints is not None:
      d['dynamic_endpoints'] = _list_to_dict(self.dynamic_endpoints)
    return d

  def to_yaml(self) -> str:
    """Serialize to YAML string. Requires PyYAML."""
    import yaml
    return yaml.dump(self.to_dict(), sort_keys=False, default_flow_style=False, allow_unicode=True)

  def to_yaml_file(self, path: str) -> None:
    """Write YAML to a file. Requires PyYAML."""
    with open(path, 'w') as f:
      f.write(self.to_yaml())

  def to_json(self) -> str:
    """Serialize to JSON string."""
    import json
    return json.dumps(self.to_dict(), indent=2)

  @classmethod
  def parse(cls, yaml_string: str) -> 'ApiSpec':
    """Parse a YAML string into an ApiSpec object. Requires PyYAML."""
    import yaml
    data = yaml.safe_load(yaml_string)
    if not isinstance(data, dict):
      raise ValueError("Expected a YAML mapping at the top level")
    return cls(**data)

  @classmethod
  def parse_file(cls, path: str) -> 'ApiSpec':
    """Parse a YAML file into an ApiSpec object. Requires PyYAML."""
    with open(path, 'r') as f:
      return cls.parse(f.read())

  def validate(self) -> List[str]:
    """Validate the API spec and return a list of error messages. Empty list means valid."""
    errors = []

    # 1. name is required
    if not self.name:
      errors.append("'name' is required")

    # 2. must have endpoints or dynamic_endpoints
    has_endpoints = self.endpoints and len(self.endpoints) > 0
    has_dynamic = self.dynamic_endpoints and len(self.dynamic_endpoints) > 0
    if not has_endpoints and not has_dynamic:
      errors.append("at least one of 'endpoints' or 'dynamic_endpoints' must have entries")

    # 3. jmespath and jq are mutually exclusive in Records
    def _check_records(records, context):
      if isinstance(records, Records):
        if records.jmespath is not None and records.jq is not None:
          errors.append(f"'jmespath' and 'jq' are mutually exclusive in {context}")

    def _check_response(resp, context):
      if isinstance(resp, Response) and resp.records is not None:
        _check_records(resp.records, context)

    if self.defaults is not None:
      _check_response(getattr(self.defaults, 'response', None), 'defaults.response.records')

    if self.endpoints:
      for name, ep in self.endpoints.items():
        if isinstance(ep, Endpoint):
          _check_response(getattr(ep, 'response', None), f"endpoints.{name}.response.records")

    # 4. queue references must be declared
    declared_queues = set(self.queues) if self.queues else set()

    def _check_queue_ref(ref, context):
      if ref and ref.startswith('queue.'):
        queue_name = ref[len('queue.'):]
        if declared_queues and queue_name not in declared_queues:
          errors.append(f"queue '{queue_name}' referenced in {context} is not declared in 'queues'")

    def _check_processors_queues(processors, context):
      if not processors:
        return
      for i, proc in enumerate(processors):
        if isinstance(proc, Processor) and proc.output:
          _check_queue_ref(proc.output, f"{context}.processors[{i}].output")

    def _check_iterate_queues(iterate, context):
      if isinstance(iterate, Iterate) and isinstance(iterate.over, str):
        _check_queue_ref(iterate.over, f"{context}.iterate.over")
      elif isinstance(iterate, str):
        _check_queue_ref(iterate, f"{context}.iterate")

    if self.endpoints:
      for name, ep in self.endpoints.items():
        if isinstance(ep, Endpoint):
          if ep.response and isinstance(ep.response, Response):
            _check_processors_queues(ep.response.processors, f"endpoints.{name}")
            _check_processors_queues(ep.response.append_processors, f"endpoints.{name}")
            _check_processors_queues(ep.response.prepend_processors, f"endpoints.{name}")
          _check_iterate_queues(ep.iterate, f"endpoints.{name}")

    # 5. sync keys should have corresponding state processors
    if self.endpoints:
      for name, ep in self.endpoints.items():
        if isinstance(ep, Endpoint) and ep.sync:
          state_outputs = set()
          if ep.response and isinstance(ep.response, Response):
            for proc_list in [ep.response.processors, ep.response.append_processors, ep.response.prepend_processors]:
              if proc_list:
                for proc in proc_list:
                  if isinstance(proc, Processor) and proc.output and proc.output.startswith('state.'):
                    state_outputs.add(proc.output[len('state.'):])
          for sync_key in ep.sync:
            if sync_key not in state_outputs:
              errors.append(f"sync key '{sync_key}' in endpoints.{name} has no processor writing to 'state.{sync_key}'")

    return errors
