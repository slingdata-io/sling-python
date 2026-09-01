import json, os, re, subprocess
from typing import Any, Dict, List, Optional, Tuple, Union
from .bin import SLING_BIN
from .connection import TestResult


class SlingPlatformError(Exception):
    """Raised when a `sling platform` command fails unexpectedly."""


_EXEC_ID_RE = re.compile(r"exec_id=([A-Za-z0-9_]+)")


class Platform:
    """Wraps `sling platform` CLI subcommands.

    Authenticates via `SLING_PROJECT_TOKEN` (environment or constructor).
    List/get methods request JSON (`-o json`) and return Python objects.
    Destructive methods pass `--force` so they never prompt.

    Examples:
        p = Platform()                                 # uses SLING_PROJECT_TOKEN
        p = Platform(token="...", cwd="/path/to/proj")
        p.status()
        p.jobs.list(job_type="replication")
        p.jobs.trigger("job_abc", wait=True)
        p.files.get("replications/users.yaml")
        p.connections.test("POSTGRES")
    """

    token: Optional[str]
    cwd: Optional[str]

    def __init__(self, token: Optional[str] = None, cwd: Optional[str] = None):
        self.token = token
        self.cwd = cwd
        self.jobs = PlatformJobs(self)
        self.execs = PlatformExecs(self)
        self.files = PlatformFiles(self)
        self.connections = PlatformConnections(self)

    def __repr__(self) -> str:
        return f"Platform(cwd={self.cwd!r})"

    def status(self) -> str:
        """Project overview (needs a token). Returns the CLI table text."""
        return self._run_ok(["status"])

    def sync(self, force: bool = True, debug: bool = False) -> str:
        """Two-way file sync (local dir ↔ platform, by mtime).

        `force=True` (the default) skips the confirmation prompt — a Python
        API should not block on stdin. Pass `force=False` only when driving
        the CLI interactively.
        """
        args = ["sync"]
        if force:
            args.append("--force")
        if debug:
            args.append("--debug")
        return self._run_ok(args)

    def init(self) -> str:
        """Create or link a local sling project folder (writes `.sling.json`)."""
        return self._run_ok(["init"])

    # --- internals --------------------------------------------------------

    def _run(
        self,
        args: List[str],
        stdin: Optional[str] = None,
    ) -> Tuple[str, str, int]:
        """Run `sling platform <args>`. Returns (stdout, stderr, returncode)."""
        cmd = [SLING_BIN, "platform"] + args
        env = dict(os.environ)
        env.setdefault("SLING_PACKAGE", "python")
        if self.token:
            env["SLING_PROJECT_TOKEN"] = self.token
        try:
            proc = subprocess.run(
                cmd,
                input=stdin.encode("utf-8") if stdin is not None else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                cwd=self.cwd,
                check=False,
            )
        except FileNotFoundError as e:
            raise SlingPlatformError(f"sling binary not found at {cmd[0]}: {e}") from e
        stdout = proc.stdout.decode("utf-8", errors="replace")
        stderr = proc.stderr.decode("utf-8", errors="replace")
        if _binary_missing_platform(stderr):
            raise SlingPlatformError(
                "this sling binary does not support `sling platform` "
                "(need sling-cli 1.6+). Update the binary or set SLING_BINARY."
            )
        return stdout, stderr, proc.returncode

    def _run_ok(self, args: List[str], stdin: Optional[str] = None) -> str:
        stdout, stderr, code = self._run(args, stdin=stdin)
        _raise_if_failed(args, stdout, stderr, code)
        return stdout

    def _run_json(self, args: List[str], stdin: Optional[str] = None) -> Any:
        if "-o" not in args and "--output" not in args:
            args = args + ["-o", "json"]
        stdout, stderr, code = self._run(args, stdin=stdin)
        _raise_if_failed(args, stdout, stderr, code)
        return _loads_json(stdout, stderr, args)


class PlatformJobs:
    """`sling platform jobs` — list/get/save/trigger/activate/deactivate/delete."""

    def __init__(self, platform: Platform):
        self._p = platform

    def list(
        self,
        job_type: Optional[str] = None,
        name: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List jobs. Filters: `job_type` (replication|pipeline|query|monitor|build),
        `name`, `file_name`."""
        args = ["jobs", "list"]
        _opt(args, "--type", job_type)
        _opt(args, "--name", name)
        _opt(args, "--file-name", file_name)
        data = self._p._run_json(args)
        return data if isinstance(data, list) else []

    def status(
        self,
        job_id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Per-job status overview (latest exec, active, next run).
        `job_id` / `name` are substring filters."""
        args = ["jobs", "status"]
        _opt(args, "--id", job_id)
        _opt(args, "--name", name)
        data = self._p._run_json(args)
        return data if isinstance(data, list) else []

    def get(self, job_id: str) -> Dict[str, Any]:
        """Full job JSON. Use as a template before `save` (save is a full replace)."""
        if not job_id:
            raise ValueError("job_id is required")
        stdout = self._p._run_ok(["jobs", "get", job_id])
        data = _loads_json(stdout, "", ["jobs", "get", job_id])
        if not isinstance(data, dict):
            raise SlingPlatformError(f"unexpected jobs get response: {data!r}")
        return data

    def save(
        self,
        job: Union[Dict[str, Any], str, None] = None,
        file: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create or update a job. Omit `id` to create; include it to update.

        Provide exactly one of `job` (dict or JSON string) or `file` (path,
        or `'-'` for stdin). Save replaces the entire job — round-trip with
        `get` before editing.
        """
        if (job is None and not file) or (job is not None and file):
            raise ValueError("must provide exactly one of job= or file=")
        args = ["jobs", "save"]
        stdin = None
        if file:
            args += ["-f", file]
        else:
            args += ["-f", "-"]
            stdin = json.dumps(job) if isinstance(job, dict) else job
        stdout = self._p._run_ok(args, stdin=stdin)
        data = _loads_json(stdout, "", args)
        if not isinstance(data, dict):
            raise SlingPlatformError(f"unexpected jobs save response: {data!r}")
        return data

    def trigger(
        self,
        job_id: str,
        wait: bool = False,
        streams: Optional[Union[str, List[str]]] = None,
        full_refresh: bool = False,
    ) -> str:
        """Run a job now. Returns the new `exec_id`.

        `wait=True` polls until the run finishes (raises if it does not succeed).
        `streams` and `full_refresh` override the job's saved config for this
        run only.
        """
        if not job_id:
            raise ValueError("job_id is required")
        args = ["jobs", "trigger", job_id]
        if wait:
            args.append("--wait")
        if streams:
            if isinstance(streams, list):
                streams = ",".join(streams)
            args += ["--streams", streams]
        if full_refresh:
            args.append("--full-refresh")
        stdout, stderr, code = self._p._run(args)
        if code != 0:
            raise SlingPlatformError(
                f"`sling platform {' '.join(args)}` failed (exit {code}): "
                f"{stderr.strip() or stdout.strip() or '(no error message)'}"
            )
        match = _EXEC_ID_RE.search(stderr) or _EXEC_ID_RE.search(stdout)
        if not match:
            raise SlingPlatformError(
                f"could not parse exec_id from jobs trigger output: "
                f"stdout={stdout!r} stderr={stderr.strip()!r}"
            )
        return match.group(1)

    def activate(self, job_id: str) -> None:
        """Enable a job's schedules."""
        if not job_id:
            raise ValueError("job_id is required")
        self._p._run_ok(["jobs", "activate", job_id])

    def deactivate(self, job_id: str) -> None:
        """Disable a job's schedules."""
        if not job_id:
            raise ValueError("job_id is required")
        self._p._run_ok(["jobs", "deactivate", job_id])

    def delete(self, job_id: str) -> None:
        """Remove a job. Always passes `--force` (no confirmation prompt)."""
        if not job_id:
            raise ValueError("job_id is required")
        self._p._run_ok(["jobs", "delete", job_id, "--force"])


class PlatformExecs:
    """`sling platform execs` — list/status/log/cancel."""

    def __init__(self, platform: Platform):
        self._p = platform

    def list(
        self,
        job_id: Optional[str] = None,
        job_name: Optional[str] = None,
        status: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Run history. `since` accepts RFC3339, YYYY-MM-DD, or a duration
        (`24h`, `7d`). Default limit is the CLI's (10, server-capped at 100)."""
        if job_id and job_name:
            raise ValueError("provide only one of job_id= or job_name=")
        if limit is not None and (not isinstance(limit, int) or limit < 0):
            raise ValueError(f"limit must be a non-negative int or None, got {limit!r}")
        args = ["execs", "list"]
        _opt(args, "--job-id", job_id)
        _opt(args, "--job-name", job_name)
        _opt(args, "--status", status)
        _opt(args, "--since", since)
        _opt(args, "--until", until)
        if limit is not None:
            args += ["--limit", str(limit)]
        data = self._p._run_json(args)
        return data if isinstance(data, list) else []

    def status(self, exec_id: str) -> Dict[str, Any]:
        """Per-task / per-step state of one execution."""
        if not exec_id:
            raise ValueError("exec_id is required")
        stdout = self._p._run_ok(["execs", "status", exec_id])
        data = _loads_json(stdout, "", ["execs", "status", exec_id])
        if not isinstance(data, dict):
            raise SlingPlatformError(f"unexpected execs status response: {data!r}")
        return data

    def log(
        self,
        exec_id: str,
        task: Optional[str] = None,
        status: Optional[str] = None,
        job_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Full log as a list of task/step records (JSON).

        `task` filters to a stream/step/model name; `status` filters by
        task status (`error`, `success`, ...); `job_type` forces
        replication|pipeline|build (auto-detected if omitted).
        """
        if not exec_id:
            raise ValueError("exec_id is required")
        args = ["execs", "log", exec_id, "--no-color"]
        _opt(args, "--task", task)
        _opt(args, "--status", status)
        _opt(args, "--type", job_type)
        data = self._p._run_json(args)
        return data if isinstance(data, list) else []

    def cancel(self, exec_id: str) -> None:
        """Cancel a running execution."""
        if not exec_id:
            raise ValueError("exec_id is required")
        self._p._run_ok(["execs", "cancel", exec_id])


class PlatformFiles:
    """`sling platform files` — list/get/save/delete/rename."""

    def __init__(self, platform: Platform):
        self._p = platform

    def list(self) -> List[Dict[str, Any]]:
        """List YAML files (replications, pipelines, specs) on the platform."""
        data = self._p._run_json(["files", "list"])
        return data if isinstance(data, list) else []

    def get(self, name: str) -> str:
        """Return a file's body. `${VAR}` references are preserved."""
        if not name:
            raise ValueError("file name is required")
        return self._p._run_ok(["files", "get", name])

    def save(
        self,
        name: str,
        body: Optional[str] = None,
        file: Optional[str] = None,
        is_dir: bool = False,
    ) -> Dict[str, Any]:
        """Create or update a project file. Trailing `/` on `name` creates a directory.

        Provide `body` (string contents), `file` (local path, or `'-'` for stdin),
        or `is_dir=True`. `body` and `file` are mutually exclusive.
        """
        if not name:
            raise ValueError("file name is required")
        if file and body is not None:
            raise ValueError("cannot specify both file= and body=")
        if not is_dir and file is None and body is None:
            raise ValueError("must provide one of file=, body=, or is_dir=True")
        args = ["files", "save", name]
        stdin = None
        if is_dir:
            args.append("--dir")
        elif file:
            args += ["-f", file]
        else:
            args += ["-f", "-"]
            stdin = body
        data = self._p._run_json(args, stdin=stdin)
        return data if isinstance(data, dict) else {"name": name}

    def delete(self, name: str) -> None:
        """Delete a project file. Always passes `--force` (no confirmation prompt)."""
        if not name:
            raise ValueError("file name is required")
        self._p._run_ok(["files", "delete", name, "--force"])

    def rename(self, old_name: str, new_name: str) -> None:
        """Rename a project file."""
        if not old_name or not new_name:
            raise ValueError("both old_name and new_name are required")
        self._p._run_ok(["files", "rename", old_name, new_name])


class PlatformConnections:
    """`sling platform connections` — list/test project-scoped connections."""

    def __init__(self, platform: Platform):
        self._p = platform

    def list(self) -> List[Dict[str, Any]]:
        """Project-scoped connections (not the local `env.yaml`)."""
        data = self._p._run_json(["connections", "list"])
        return data if isinstance(data, list) else []

    def test(self, name: str) -> TestResult:
        """Test a project connection. Returns `TestResult` (does not raise on
        an invalid connection — only on token/binary errors)."""
        if not name:
            raise ValueError("connection name is required")
        stdout, stderr, code = self._p._run(["connections", "test", name])
        combined = (stderr.strip() or stdout.strip())
        if code == 0:
            return TestResult(success=True, error="")
        lower = combined.lower()
        # Auth / setup failures should still raise; a bad connection name or
        # unreachable endpoint is a TestResult(success=False).
        if "sling_project_token" in lower or "invalid project token" in lower:
            raise SlingPlatformError(
                f"`sling platform connections test {name}` failed (exit {code}): "
                f"{combined or '(no error message)'}"
            )
        return TestResult(success=False, error=combined)


def _opt(args: List[str], flag: str, value: Optional[str]) -> None:
    if value:
        args.extend([flag, str(value)])


_USAGE_RE = re.compile(r"Usage:\s+sling \[([^\]]+)\]")


def _binary_missing_platform(stderr: str) -> bool:
    """True when an older sling binary printed top-level help because it has
    no `platform` subcommand (pre-1.6 used `project`)."""
    m = _USAGE_RE.search(stderr)
    return bool(m and "platform" not in m.group(1).split("|"))


def _raise_if_failed(args: List[str], stdout: str, stderr: str, code: int) -> None:
    if code != 0:
        raise SlingPlatformError(
            f"`sling platform {' '.join(args)}` failed (exit {code}): "
            f"{stderr.strip() or stdout.strip() or '(no error message)'}"
        )


def _loads_json(stdout: str, stderr: str, args: List[str]) -> Any:
    text = stdout.strip()
    if not text:
        raise SlingPlatformError(
            f"empty JSON from `sling platform {' '.join(args)}`: "
            f"stderr={stderr.strip()!r}"
        )
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # On success the payload is the last JSON value on stdout (only one
        # is emitted today; slice defensively in case a future version adds
        # a log line).
        try:
            return json.loads(text.splitlines()[-1])
        except json.JSONDecodeError as e:
            raise SlingPlatformError(
                f"could not parse JSON from `sling platform {' '.join(args)}`: "
                f"stdout={stdout!r} stderr={stderr.strip()!r}"
            ) from e
