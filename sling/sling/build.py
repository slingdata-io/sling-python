"""Sling Build operator — runs `sling build` projects (`sling_build.yml`).

Wraps the `run`, `test`, `list`, and `compile` subcommands of `sling build`.
Per-node results come from the CLI's machine-readable output (`--json`), which
is the same payload a pipeline `type: build` step receives in
`state.<step_id>.results`.

Requires a sling binary with the `build` subcommands (sling-cli 1.6+) and
`--json` support on the invoked subcommand. Set `SLING_BINARY` to test against
a local build.

```python
from sling import Build

build = Build(path="models", target="MY_SNOWFLAKE")
result = build.run(select=["+stg_users"])
for node in result.failed_nodes:
    print(node.name, node.error)
```
"""

import json
import os
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from .bin import SLING_BIN

RUN = "run"
TEST = "test"
LIST = "list"
COMPILE = "compile"

_SUBCOMMANDS = (RUN, TEST, LIST, COMPILE)

# `sling build --help` usage line; absent on binaries without the subcommands.
_BUILD_USAGE = "build [run|list|test|compile]"

# `test --json` reports ok|fail|skip; `run --json` reports success|error|skipped.
_STATUS = {"ok": "success", "fail": "error", "skip": "skipped"}

_UNIT_SECONDS = {
    "ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3,
    "s": 1.0, "m": 60.0, "h": 3600.0,
}
_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)")

# `--help` output per subcommand, probed once per process.
_HELP_CACHE: Dict[str, str] = {}


class SlingBuildError(Exception):
    """Raised when a `sling build` command fails.

    `result` carries the parsed `BuildResult` when the invocation returned a
    machine-readable payload (a failed run still reports its per-node errors).
    """

    def __init__(self, message: str, result: Optional["BuildResult"] = None):
        super().__init__(message)
        self.result = result


@dataclass
class BuildNodeResult:
    """Outcome of one executed node (a model or a seed)."""

    name: str
    type: str = ""
    status: str = "success"  # success | error | skipped
    mode: str = ""
    duration: float = 0.0  # seconds
    error: str = ""
    rows: int = 0
    bytes: int = 0

    @property
    def success(self) -> bool:
        return self.status == "success"


@dataclass
class BuildNode:
    """A selected node, from `list` or the `nodes` of a `compile` payload."""

    name: str
    type: str = ""
    table: str = ""
    file: str = ""
    mode: str = ""
    sql: str = ""
    dependencies: List[str] = field(default_factory=list)
    tests: List[Any] = field(default_factory=list)
    project: str = ""  # set for independent sub-project payloads (`-R`)


@dataclass
class BuildCompileResult:
    """Compiled output: execution order plus the rendered SQL of each node."""

    order: List[str] = field(default_factory=list)
    nodes: List[BuildNode] = field(default_factory=list)
    target: str = ""
    compiled: bool = False
    sub_projects: List["BuildCompileResult"] = field(default_factory=list)
    stdout: str = ""


@dataclass
class BuildResult:
    """Outcome of a `run` or `test` invocation."""

    command: str
    success: bool  # the process exited 0
    exit_code: int = 0
    path: str = ""
    target: str = ""
    results: List[BuildNodeResult] = field(default_factory=list)
    total: int = 0
    ok: int = 0
    failed: int = 0
    skipped: int = 0
    rows: int = 0
    bytes: int = 0
    ok_names: str = ""
    sub_projects: List["BuildResult"] = field(default_factory=list)
    stdout: str = ""
    stderr: str = ""

    @property
    def failed_nodes(self) -> List[BuildNodeResult]:
        """Nodes that errored, with their messages."""
        return [r for r in self.results if r.status == "error"]

    @property
    def ok_nodes(self) -> List[BuildNodeResult]:
        return [r for r in self.results if r.status == "success"]


class Build:
    """Runs a Sling Build project (`sling_build.yml`) via `sling build`.

    Options mirror the CLI flags and the `HookBuild` pipeline step. `select`
    and `exclude` accept a comma-separated string or a list; `vars` accepts a
    dict serialized to the CLI's YAML/JSON `--vars` value.

    `run` and `test` raise `SlingBuildError` on a non-zero exit unless
    `raise_on_error=False`; the error carries `.result` with the per-node
    failures. `list`, `compile`, and `test` return parsed JSON.
    """

    path: str
    target: Optional[str]
    select: Optional[Union[str, List[str]]]
    exclude: Optional[Union[str, List[str]]]
    vars: Optional[Dict[str, Any]]
    threads: Optional[int]
    fail_fast: Optional[bool]
    full_refresh: Optional[bool]
    no_seeds: Optional[bool]
    schema: Optional[str]
    prod: bool
    recursive: bool
    range_param: Optional[str]
    env: Dict[str, str]
    cwd: Optional[str]
    debug: bool
    trace: bool

    def __init__(
        self,
        path: str = ".",
        target: Optional[str] = None,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        vars: Optional[Dict[str, Any]] = None,
        threads: Optional[int] = None,
        fail_fast: Optional[bool] = None,
        full_refresh: Optional[bool] = None,
        no_seeds: Optional[bool] = None,
        schema: Optional[str] = None,
        prod: bool = False,
        recursive: bool = False,
        range_param: Optional[str] = None,  # 'range' is a builtin
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        debug: bool = False,
        trace: bool = False,
    ):
        self.path = path
        self.target = target
        self.select = select
        self.exclude = exclude
        self.vars = vars
        self.threads = threads
        self.fail_fast = fail_fast
        self.full_refresh = full_refresh
        self.no_seeds = no_seeds
        self.schema = schema
        self.prod = prod
        self.recursive = recursive
        self.range_param = range_param
        self.env = env or {}
        self.cwd = cwd
        self.debug = debug
        self.trace = trace

    def __repr__(self) -> str:
        return f"Build(path={self.path!r}, target={self.target!r})"

    # --- commands ---------------------------------------------------------

    def run(self, raise_on_error: bool = True) -> BuildResult:
        """Materialize the selected models, then run their declarative tests."""
        return self._execute(RUN, raise_on_error=raise_on_error)

    def test(self, raise_on_error: bool = True) -> BuildResult:
        """Run declarative data tests only (no materialization, no seeds)."""
        return self._execute(TEST, raise_on_error=raise_on_error)

    def list(self) -> List[BuildNode]:
        """Selected nodes without executing (`build list --json`)."""
        stdout, stderr, code = self._invoke(LIST)
        self._raise_if_failed(code, stdout, stderr, LIST)
        payload = _load_json(stdout, stderr, LIST)

        # Independent sub-projects (`-R`) emit one payload per project, each
        # with its own `nodes`; a single project emits the bare node array.
        if isinstance(payload, list) and payload and "nodes" in payload[0]:
            nodes: List[BuildNode] = []
            for item in payload:
                nodes.extend(_nodes(item.get("nodes")))
            return nodes
        return _nodes(payload)

    def compile(self) -> BuildCompileResult:
        """Render SQL and the DAG without executing (`build compile --json`)."""
        stdout, stderr, code = self._invoke(COMPILE)
        self._raise_if_failed(code, stdout, stderr, COMPILE)
        payload = _load_json(stdout, stderr, COMPILE)

        if isinstance(payload, list):
            result = BuildCompileResult(stdout=stdout)
            result.sub_projects = [_compile_result(p, stdout) for p in payload]
            for sub in result.sub_projects:
                result.order.extend(sub.order)
                result.nodes.extend(sub.nodes)
            return result
        return _compile_result(payload, stdout)

    # --- internals --------------------------------------------------------

    def _execute(self, command: str, raise_on_error: bool) -> BuildResult:
        stdout, stderr, code = self._invoke(command)

        if not stdout.strip():
            # No machine-readable payload: a failed run (bad project, missing
            # connection, …) reports only on stderr, so surface that instead.
            if code != 0:
                if raise_on_error:
                    self._raise_if_failed(code, stdout, stderr, command)
                return BuildResult(
                    command=command,
                    success=False,
                    exit_code=code,
                    path=self.path,
                    target=self.target or "",
                    stdout=stdout,
                    stderr=stderr,
                )
            raise SlingBuildError(
                f"empty JSON from `sling build {command}`: stderr={stderr.strip()!r}"
            )

        payload = _load_json(stdout, stderr, command)

        if isinstance(payload, list):  # test --json emits a bare array
            results = [_node_result(item) for item in payload]
            result = BuildResult(
                command=command,
                success=code == 0,
                exit_code=code,
                path=self.path,
                target=self.target or "",
                results=results,
                total=len(results),
                ok=len([r for r in results if r.status == "success"]),
                failed=len([r for r in results if r.status == "error"]),
                skipped=len([r for r in results if r.status == "skipped"]),
                stdout=stdout,
                stderr=stderr,
            )
        else:
            result = _run_result(payload, command, code, stdout, stderr)

        if code != 0 and raise_on_error:
            self._raise_if_failed(code, stdout, stderr, command, result)
        return result

    def _invoke(self, command: str) -> "tuple[str, str, int]":
        """Runs `sling build <command>`. Returns (stdout, stderr, returncode)."""
        _check_support(command)
        args = self._cmd(command)
        env = dict(os.environ)
        env.update({k: str(v) for k, v in self.env.items()})
        # Attribute the call to the orchestrator when one is importing us,
        # matching the `Sling`/`Replication` classes.
        from . import is_package

        for pkg in ("dagster", "airflow", "temporal", "orkes"):
            if is_package(pkg):
                env["SLING_PACKAGE"] = pkg
                break
        else:
            env.setdefault("SLING_PACKAGE", "python")

        try:
            proc = subprocess.run(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                cwd=self.cwd,
                check=False,
            )
        except FileNotFoundError as e:
            raise SlingBuildError(f"sling binary not found at {args[0]}: {e}") from e

        return (
            proc.stdout.decode("utf-8", errors="replace"),
            proc.stderr.decode("utf-8", errors="replace"),
            proc.returncode,
        )

    def _cmd(self, command: str) -> List[str]:
        args = [SLING_BIN, "build", command, self.path]
        _opt(args, "--target", self.target)
        _opt(args, "--select", _csv(self.select))
        _opt(args, "--exclude", _csv(self.exclude))
        _opt(args, "--schema", self.schema)
        if self.prod:
            args.append("--prod")
        if self.vars:
            args.extend(["--vars", json.dumps(self.vars)])
        if self.recursive:
            args.append("--recursive")
        if self.debug:
            args.append("--debug")
        if self.trace:
            args.append("--trace")

        if command == RUN:
            if self.full_refresh:
                args.append("--full-refresh")
            _opt(args, "--range", self.range_param)
            if self.no_seeds:
                args.append("--no-seeds")
        if command in (RUN, TEST):
            _opt(args, "--threads", self.threads)
            if self.fail_fast:
                args.append("--fail-fast")

        args.append("--json")
        return args

    def _raise_if_failed(
        self,
        code: int,
        stdout: str,
        stderr: str,
        command: str,
        result: Optional[BuildResult] = None,
    ) -> None:
        if code == 0:
            return
        detail = stderr.strip() or stdout.strip() or "(no error message)"
        raise SlingBuildError(
            f"`sling build {command} {self.path}` failed (exit {code}): {detail}",
            result=result,
        )


def _opt(args: List[str], flag: str, value: Any) -> None:
    if value is not None and value != "":
        args.extend([flag, str(value)])


def _csv(value: Union[str, List[str], None]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return ",".join(str(v) for v in value)


def _help_output(*args: str) -> str:
    key = " ".join(args)
    if key not in _HELP_CACHE:
        try:
            proc = subprocess.run(
                [SLING_BIN, *args, "--help"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
        except FileNotFoundError as e:
            raise SlingBuildError(f"sling binary not found at {SLING_BIN}: {e}") from e
        _HELP_CACHE[key] = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
    return _HELP_CACHE[key]


def _check_support(command: str) -> None:
    """Fails early on a binary without `build <command> --json`, instead of
    letting the CLI print top-level help and return unparseable output."""
    if _BUILD_USAGE not in _help_output("build"):
        raise SlingBuildError(
            f"this sling binary (`{SLING_BIN}`) has no `build` subcommands "
            "(sling-cli 1.6+ is required). Update the binary or set SLING_BINARY."
        )
    if "--json" not in _help_output("build", command):
        raise SlingBuildError(
            f"`sling build {command}` of this binary (`{SLING_BIN}`) has no `--json` "
            "output, which this operator needs for structured results. "
            "Update the binary or set SLING_BINARY."
        )


def _load_json(stdout: str, stderr: str, command: str) -> Any:
    text = stdout.strip()
    if not text:
        raise SlingBuildError(
            f"empty JSON from `sling build {command}`: stderr={stderr.strip()!r}"
        )
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # With SLING_LOGGING=JSON the log lines also land on stdout; the
        # payload is the last value emitted.
        try:
            return json.loads(text.splitlines()[-1])
        except json.JSONDecodeError as e:
            raise SlingBuildError(
                f"could not parse JSON from `sling build {command}`: "
                f"stdout={stdout!r} stderr={stderr.strip()!r}"
            ) from e


def _duration_seconds(value: Any) -> float:
    """Accepts seconds (float) or a Go duration string such as '1m2.5s'."""
    if isinstance(value, (int, float)):
        return float(value)
    total = 0.0
    for num, unit in _DURATION_RE.findall(str(value or "")):
        total += float(num) * _UNIT_SECONDS[unit]
    return total


def _node_result(item: Dict[str, Any]) -> BuildNodeResult:
    status = str(item.get("status") or "success")
    return BuildNodeResult(
        name=item.get("name", ""),
        type=item.get("type", ""),
        status=_STATUS.get(status, status),
        mode=item.get("mode", ""),
        duration=_duration_seconds(item.get("duration")),
        error=item.get("error") or "",
        rows=int(item.get("rows") or 0),
        bytes=int(item.get("bytes") or 0),
    )


def _nodes(items: Any) -> List[BuildNode]:
    nodes = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        nodes.append(
            BuildNode(
                name=item.get("name", ""),
                type=item.get("type", ""),
                table=item.get("table", ""),
                file=item.get("file", ""),
                mode=item.get("mode", ""),
                sql=item.get("sql", ""),
                dependencies=list(item.get("dependencies") or []),
                tests=list(item.get("tests") or []),
            )
        )
    return nodes


def _run_result(
    payload: Dict[str, Any], command: str, code: int, stdout: str, stderr: str
) -> BuildResult:
    results = [_node_result(r) for r in payload.get("results") or []]
    sub_projects = [
        _run_result(sub, command, 0, "", "") for sub in payload.get("sub_projects") or []
    ]
    for sub in sub_projects:
        # Sub-project payloads carry no exit code; the aggregate counts do.
        sub.success = sub.failed == 0

    return BuildResult(
        command=command,
        success=code == 0,
        exit_code=code,
        path=payload.get("path") or "",
        target=payload.get("target") or "",
        results=results,
        total=int(payload.get("total") or len(results)),
        ok=int(payload.get("ok") or 0),
        failed=int(payload.get("failed") or 0),
        skipped=int(payload.get("skipped") or 0),
        rows=int(payload.get("rows") or 0),
        bytes=int(payload.get("bytes") or 0),
        ok_names=payload.get("ok_names") or "",
        sub_projects=sub_projects,
        stdout=stdout,
        stderr=stderr,
    )


def _compile_result(payload: Dict[str, Any], stdout: str) -> BuildCompileResult:
    return BuildCompileResult(
        order=list(payload.get("order") or []),
        nodes=_nodes(payload.get("nodes")),
        target=payload.get("target") or "",
        compiled=bool(payload.get("compiled")),
        stdout=stdout,
    )
