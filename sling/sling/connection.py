import os, json, subprocess
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union
from .bin import SLING_BIN


class SlingConnectionError(Exception):
    """Raised when a `sling conns` command fails unexpectedly."""


@dataclass
class TestResult:
    success: bool
    error: str  # empty string on success


@dataclass
class QueryResult:
    fields: List[str]
    rows: List[List[Any]]

    def to_list(self) -> List[Dict[str, Any]]:
        return [dict(zip(self.fields, r)) for r in self.rows]

    def to_dataframe(self):
        import pandas as pd
        return pd.DataFrame(self.rows, columns=self.fields)

    def to_dataset(self):
        # Returns pyarrow.Table — pyarrow's "dataset" type is a different,
        # lazy multi-file thing; for an in-memory result a Table is what callers want.
        import pyarrow as pa
        return pa.table({
            name: [r[i] for r in self.rows]
            for i, name in enumerate(self.fields)
        })


class Connection:
    """Wraps a named sling connection.

    Examples:
        conn = Connection('POSTGRES')
        conn.test()                                    # -> TestResult
        conn.exec("select 1 as a")                     # -> [{'a': 1}]
        conn.exec("select 1 as a", return_type='dataframe')  # -> pandas.DataFrame
        conn.exec("select 1 as a", return_type='dataset')    # -> pyarrow.Table

    Env-var connections work transparently:
        os.environ['MYSQL'] = 'mysql://user:pw@host/db'
        Connection('MYSQL').test()

    Note: exec() materializes the full result in memory. For large queries,
    use Sling(src_conn=..., src_stream=sql).stream() instead.
    """
    name: str

    def __init__(self, name: str):
        if not isinstance(name, str) or not name:
            raise ValueError(f"Connection name must be a non-empty string, got {name!r}")
        self.name = name

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"Connection({self.name!r})"

    def test(self) -> TestResult:
        stdout_b, stderr_b, code = _run_sling([SLING_BIN, "conns", "test", self.name], output="json")
        stdout = stdout_b.decode("utf-8", errors="replace")
        stderr = stderr_b.decode("utf-8", errors="replace")
        # `conns test` exits 1 on bad connection name but still emits JSON.
        # Try to parse first; only raise if parse fails.
        try:
            data = json.loads(stdout) if stdout else {}
        except json.JSONDecodeError:
            raise SlingConnectionError(
                f"could not parse JSON from `sling conns test {self.name}` "
                f"(exit {code}): stdout={stdout!r} stderr={stderr.strip()!r}"
            )
        if "success" not in data:
            raise SlingConnectionError(
                f"unexpected response from `sling conns test {self.name}` "
                f"(exit {code}): {data!r} stderr={stderr.strip()!r}"
            )
        return TestResult(success=bool(data["success"]), error=data.get("error") or "")

    def exec(
        self,
        sql: str,
        return_type: str = "list",
        limit: Optional[int] = None,
    ) -> Union[List[Dict[str, Any]], Any]:  # pd.DataFrame or pa.Table
        """Execute a SQL query.

        Args:
            sql: the SQL to run.
            return_type: 'list' | 'dataframe' | 'dataset' | 'arrow'. The 'arrow'
                path streams via Arrow IPC and keeps memory bounded for large
                queries; the others materialize the result.
            limit: maximum rows to return. None (default) lets the CLI apply
                its default cap of 100. Pass 0 for no limit. The CLI wraps the
                SQL with the dialect's LIMIT template so the database
                truncates server-side.
        """
        if return_type not in ("list", "dataframe", "dataset", "arrow"):
            raise ValueError(
                f"return_type must be one of 'list', 'dataframe', 'dataset', 'arrow', "
                f"got {return_type!r}"
            )
        if limit is not None and (not isinstance(limit, int) or limit < 0):
            raise ValueError(f"limit must be a non-negative int or None, got {limit!r}")

        cmd_extras: List[str] = []
        if limit is not None:
            cmd_extras += ["--limit", str(limit)]

        # 'arrow' uses the binary's SLING_OUTPUT=arrow stream IPC path —
        # rows stream from the source DB straight into Arrow batches on the
        # other side of the pipe, so memory stays bounded for large queries.
        if return_type == "arrow":
            stdout_b, stderr_b, code = _run_sling(
                [SLING_BIN, "conns", "exec", self.name, sql] + cmd_extras, output="arrow"
            )
            if code != 0:
                raise SlingConnectionError(
                    f"`sling conns exec {self.name}` failed (exit {code}): "
                    f"{stderr_b.decode('utf-8', errors='replace').strip() or '(no error message)'}"
                )
            import io
            import pyarrow.ipc as ipc
            return ipc.open_stream(io.BytesIO(stdout_b)).read_all()

        stdout_b, stderr_b, code = _run_sling(
            [SLING_BIN, "conns", "exec", self.name, sql] + cmd_extras, output="json"
        )
        stdout = stdout_b.decode("utf-8", errors="replace")
        stderr = stderr_b.decode("utf-8", errors="replace")
        if code != 0:
            raise SlingConnectionError(
                f"`sling conns exec {self.name}` failed (exit {code}): "
                f"{stderr.strip() or stdout.strip()}"
            )
        # On success, payload is the last JSON line on stdout (only one is emitted today,
        # but slice defensively in case future versions add log lines to stdout).
        payload = json.loads(stdout.strip().splitlines()[-1])
        result = QueryResult(
            fields=payload.get("fields") or [],
            rows=payload.get("rows") or [],
        )
        if return_type == "list":
            return result.to_list()
        if return_type == "dataframe":
            return result.to_dataframe()
        return result.to_dataset()


def _run_sling(cmd: List[str], output: str = "json") -> Tuple[bytes, bytes, int]:
    """Run a sling subcommand with SLING_OUTPUT=<output>.

    Returns raw stdout/stderr bytes so callers can decode (json/text) or
    parse binary (arrow IPC) as appropriate. stdout and stderr are captured
    separately so the binary's log output (always on stderr) never corrupts
    a structured payload on stdout. os.environ is merged so user-defined
    env-var connections are visible to the binary.
    """
    env = dict(os.environ)
    env["SLING_OUTPUT"] = output
    env.setdefault("SLING_PACKAGE", "python")
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            check=False,
        )
    except FileNotFoundError as e:
        raise SlingConnectionError(f"sling binary not found at {cmd[0]}: {e}") from e
    return proc.stdout, proc.stderr, proc.returncode
