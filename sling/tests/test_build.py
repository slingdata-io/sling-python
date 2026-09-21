"""Tests for the `sling build` operator (sling.Build).

Pure-Python tests mock the CLI. The live tests build a throwaway project
against an env-defined DuckDB connection, so they need a sling binary with the
`build` subcommands and `--json` on `run`.
"""

import json
import os
import subprocess
import pytest
from unittest.mock import MagicMock

from sling import Build, SlingBuildError
from sling.bin import SLING_BIN
import sling.build as build_mod

requires_binary = pytest.mark.skipif(
    not os.path.exists(SLING_BIN), reason="Sling binary not available"
)

_USAGE = "build - Build and execute SQL models\n\n  Usage:\n    build [run|list|test|compile]\n"
_HELP = {
    "build": _USAGE,
    "build run": "run - Materialize models\n  Flags:\n       --json           Emit machine-readable JSON.\n",
    "build test": "test - Run declarative data tests only\n  Flags:\n       --json\n",
    "build list": "list - List selected models\n  Flags:\n       --json\n",
    "build compile": "compile - Render SQL and DAG\n  Flags:\n       --json\n",
}


def _proc(stdout="", stderr="", code=0):
    proc = MagicMock()
    proc.stdout = stdout.encode() if isinstance(stdout, str) else stdout
    proc.stderr = stderr.encode() if isinstance(stderr, str) else stderr
    proc.returncode = code
    return proc


@pytest.fixture(autouse=True)
def clear_help_cache():
    build_mod._HELP_CACHE.clear()
    yield
    build_mod._HELP_CACHE.clear()


def _patch(monkeypatch, payload=None, code=0, stderr="", help_map=None, stdout=None):
    """Replaces subprocess.run: `--help` probes get help text, build commands
    get `payload` (or raw `stdout`). Returns the fake, whose `.calls` holds the
    build argv."""
    helps = dict(_HELP)
    helps.update(help_map or {})

    def fake(args, **kwargs):
        if args[-1] == "--help":
            return _proc(stdout=helps.get(" ".join(args[1:-1]), ""))
        fake.calls.append(list(args))
        fake.kwargs = kwargs
        out = stdout if stdout is not None else (
            json.dumps(payload) if payload is not None else ""
        )
        return _proc(stdout=out, stderr=stderr, code=code)

    fake.calls = []
    fake.kwargs = {}
    monkeypatch.setattr(build_mod.subprocess, "run", fake)
    return fake


# --- command construction (no binary) ---


class TestCommand:
    def test_run_flags_include_run_only_flags(self, monkeypatch):
        fake = _patch(monkeypatch, payload={})
        Build(
            path="models",
            target="MY_SF",
            select=["+stg_users", "fct_orders"],
            exclude="tmp_*",
            vars={"start_date": "2024-01-01"},
            threads=4,
            fail_fast=True,
            full_refresh=True,
            no_seeds=True,
            schema="dev_me",
            recursive=True,
            range_param="2024-01-01,2024-02-01,1mo",
        ).run()

        cmd = fake.calls[0]
        assert cmd[:4] == [SLING_BIN, "build", "run", "models"]
        assert cmd[cmd.index("--target") + 1] == "MY_SF"
        assert cmd[cmd.index("--select") + 1] == "+stg_users,fct_orders"
        assert cmd[cmd.index("--exclude") + 1] == "tmp_*"
        assert cmd[cmd.index("--vars") + 1] == '{"start_date": "2024-01-01"}'
        assert cmd[cmd.index("--threads") + 1] == "4"
        assert cmd[cmd.index("--schema") + 1] == "dev_me"
        assert cmd[cmd.index("--range") + 1] == "2024-01-01,2024-02-01,1mo"
        for flag in ("--fail-fast", "--full-refresh", "--no-seeds", "--recursive", "--json"):
            assert flag in cmd

    def test_list_and_compile_omit_run_only_flags(self, monkeypatch):
        """`--full-refresh`, `--range`, and `--no-seeds` only exist on `run`;
        passing them elsewhere makes the CLI reject the invocation."""
        for command, call in (("list", "list"), ("compile", "compile")):
            fake = _patch(monkeypatch, payload=[] if command == "list" else {})
            build = Build(
                path=".", full_refresh=True, range_param="2024-01-01,2024-02-01",
                no_seeds=True, threads=2, fail_fast=True,
            )
            getattr(build, call)()
            cmd = fake.calls[0]
            assert cmd[:3] == [SLING_BIN, "build", command]
            for flag in ("--full-refresh", "--range", "--no-seeds", "--threads", "--fail-fast"):
                assert flag not in cmd, f"{flag} must not be passed to build {command}"
            assert "--json" in cmd

    def test_env_is_merged_into_the_subprocess(self, monkeypatch):
        fake = _patch(monkeypatch, payload={"results": []})
        Build(env={"BUILD_TEST_DUCKDB": "{type: duckdb}"}, cwd="/tmp/proj").run()
        env = fake.kwargs["env"]
        assert env["BUILD_TEST_DUCKDB"] == "{type: duckdb}"
        assert env["SLING_PACKAGE"] == "python"
        assert fake.kwargs["cwd"] == "/tmp/proj"


# --- payload parsing (no binary) ---


class TestParsing:
    def test_run_result(self, monkeypatch):
        _patch(
            monkeypatch,
            payload={
                "path": "models",
                "target": "MY_SF",
                "results": [
                    {"name": "staging.stg_orders", "type": "model", "mode": "view",
                     "duration": 1.5, "status": "success", "rows": 10, "bytes": 0},
                    {"name": "marts.fct_orders", "type": "model", "mode": "full-refresh",
                     "duration": 0.25, "status": "error", "error": "boom", "rows": 0, "bytes": 0},
                    {"name": "seeds.customers", "type": "seed", "status": "skipped"},
                ],
                "total": 3, "ok": 1, "failed": 1, "skipped": 1,
                "rows": 10, "bytes": 0, "ok_names": "staging.stg_orders",
            },
        )
        result = Build(path="models").run()

        assert result.success is True
        assert (result.path, result.target) == ("models", "MY_SF")
        assert (result.total, result.ok, result.failed, result.skipped) == (3, 1, 1, 1)
        assert result.rows == 10
        assert result.ok_names == "staging.stg_orders"
        assert [n.name for n in result.failed_nodes] == ["marts.fct_orders"]
        assert result.failed_nodes[0].error == "boom"
        assert [n.name for n in result.ok_nodes] == ["staging.stg_orders"]
        assert result.results[0].duration == 1.5
        assert result.results[2].status == "skipped"

    def test_failed_run_raises_with_result(self, monkeypatch):
        payload = {
            "results": [{"name": "marts.fct_orders", "status": "error", "error": "missing table"}],
            "total": 1, "ok": 0, "failed": 1,
        }
        _patch(monkeypatch, payload=payload, code=1, stderr="build completed with 1 error(s)")

        with pytest.raises(SlingBuildError) as exc:
            Build(path="models").run()

        assert exc.value.result is not None
        assert exc.value.result.failed == 1
        assert exc.value.result.failed_nodes[0].error == "missing table"
        assert "exit 1" in str(exc.value)

    def test_failed_run_without_raise(self, monkeypatch):
        _patch(monkeypatch, payload={"results": [], "total": 0, "ok": 0, "failed": 1}, code=1)
        result = Build().run(raise_on_error=False)
        assert result.success is False
        assert result.exit_code == 1
        assert result.failed == 1

    def test_test_payload_normalizes_status_and_duration(self, monkeypatch):
        """`test --json` is a bare array with ok|fail|skip and Go durations."""
        _patch(
            monkeypatch,
            payload=[
                {"name": "stg_orders", "type": "model", "status": "ok", "duration": "1.5s"},
                {"name": "fct_orders", "type": "model", "status": "fail",
                 "error": "3 violating row(s)", "duration": "1m2.5s"},
                {"name": "dim_customers", "type": "model", "status": "skip"},
            ],
            code=1,
        )
        result = Build().test(raise_on_error=False)

        assert result.success is False
        assert (result.total, result.ok, result.failed, result.skipped) == (3, 1, 1, 1)
        assert [r.status for r in result.results] == ["success", "error", "skipped"]
        assert result.results[0].duration == 1.5
        assert result.results[1].duration == 62.5
        assert result.failed_nodes[0].error == "3 violating row(s)"

    def test_list_parses_nodes(self, monkeypatch):
        _patch(
            monkeypatch,
            payload=[
                {"name": "stg_orders", "type": "view", "file": "staging/stg_orders.sql"},
                {"name": "fct_orders", "type": "full-refresh", "file": "marts/fct_orders.sql"},
            ],
        )
        nodes = Build(path="models").list()
        assert [n.name for n in nodes] == ["stg_orders", "fct_orders"]
        assert nodes[1].file == "marts/fct_orders.sql"

    def test_compile_parses_order_and_sql(self, monkeypatch):
        _patch(
            monkeypatch,
            payload={
                "order": ["stg_orders", "fct_orders"],
                "nodes": [
                    {"name": "stg_orders", "type": "model", "mode": "view",
                     "file": "staging/stg_orders.sql", "sql": "select 1 as id"},
                    {"name": "fct_orders", "type": "model", "dependencies": ["stg_orders"],
                     "sql": "select * from staging.stg_orders"},
                ],
                "target": "MY_SF",
                "compiled": True,
            },
        )
        result = Build(path="models").compile()

        assert result.order == ["stg_orders", "fct_orders"]
        assert result.target == "MY_SF"
        assert result.compiled is True
        assert result.nodes[1].dependencies == ["stg_orders"]
        assert result.nodes[0].sql == "select 1 as id"

    def test_sub_projects_are_grouped(self, monkeypatch):
        """`-R` over independent builds reports one payload per project."""
        _patch(
            monkeypatch,
            payload={
                "path": "monorepo",
                "target": "",
                "sub_projects": [
                    {"path": "monorepo/a", "target": "SNOWFLAKE",
                     "results": [{"name": "staging.stg_orders", "status": "success"}],
                     "total": 1, "ok": 1, "failed": 0},
                    {"path": "monorepo/b", "target": "BIGQUERY",
                     "results": [{"name": "staging.stg_orders", "status": "error", "error": "nope"}],
                     "total": 1, "ok": 0, "failed": 1},
                ],
                "results": [],
                "total": 2, "ok": 1, "failed": 1,
            },
            code=1,
        )
        result = Build(path="monorepo", recursive=True).run(raise_on_error=False)

        assert len(result.sub_projects) == 2
        assert result.sub_projects[0].target == "SNOWFLAKE"
        assert result.sub_projects[0].success is True
        assert result.sub_projects[1].success is False
        assert result.failed == 1

    def test_garbage_json_raises(self, monkeypatch):
        _patch(monkeypatch, stdout="not json at all\n")
        with pytest.raises(SlingBuildError, match="could not parse JSON"):
            Build().run()

    def test_payload_after_log_lines(self, monkeypatch):
        """`SLING_LOGGING=JSON` puts log lines on stdout before the payload."""
        _patch(
            monkeypatch,
            stdout='{"lvl":"info","msg":"building"}\n{"results":[],"total":0,"ok":0}\n',
        )
        result = Build().run()
        assert result.total == 0

    def test_empty_stdout_on_failure_raises_cli_error(self, monkeypatch):
        """A project that fails before executing prints only to stderr."""
        _patch(monkeypatch, payload=None, code=1, stderr="could not load build project")
        with pytest.raises(SlingBuildError, match="could not load build project"):
            Build().run()

    def test_empty_stdout_on_failure_without_raise(self, monkeypatch):
        _patch(monkeypatch, payload=None, code=1, stderr="could not load build project")
        result = Build().run(raise_on_error=False)
        assert result.success is False
        assert result.results == []

    def test_empty_stdout_on_success_raises(self, monkeypatch):
        _patch(monkeypatch, payload=None)
        with pytest.raises(SlingBuildError, match="empty JSON"):
            Build().run()

    def test_missing_build_subcommands_raises(self, monkeypatch):
        _patch(monkeypatch, help_map={"build": "sling - The Data Engineering CLI\n"})
        with pytest.raises(SlingBuildError, match="no `build` subcommands"):
            Build().run()

    def test_missing_json_support_raises(self, monkeypatch):
        _patch(monkeypatch, help_map={"build run": "run - Materialize models\n  Flags:\n"})
        with pytest.raises(SlingBuildError, match="--json"):
            Build().run()


# --- live tests (real binary + DuckDB) ---


def _build_supported() -> bool:
    if not os.path.exists(SLING_BIN):
        return False

    def help_text(*args) -> str:
        proc = subprocess.run(
            [SLING_BIN, *args, "--help"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False,
        )
        return (proc.stdout + proc.stderr).decode("utf-8", errors="replace")

    try:
        return "build [run|list|test|compile]" in help_text("build") and \
            "--json" in help_text("build", "run")
    except Exception:
        return False


requires_build = pytest.mark.skipif(
    not _build_supported(), reason="sling binary has no `build run --json`"
)

VALID_MODEL = """/**
tests:
  - not_null: [id]
  - unique: [id]
**/
SELECT 1 AS id, 'a' AS name
UNION ALL
SELECT 2 AS id, 'b' AS name
"""
INVALID_MODEL = "SELECT * FROM table_that_does_not_exist\n"


def _project(path, models):
    """Writes a sling_build.yml project at `path` with the given models."""
    (path / "staging").mkdir(parents=True, exist_ok=True)
    (path / "marts").mkdir(parents=True, exist_ok=True)
    (path / "sling_build.yml").write_text("defaults:\n  mode: full-refresh\n")
    for rel_path, sql in models.items():
        (path / rel_path).write_text(sql)
    return path


@pytest.fixture
def project(tmp_path):
    """Two valid models (staging + a mart referencing it) plus a broken one."""
    path = _project(tmp_path / "project", {
        "staging/stg_orders.sql": VALID_MODEL,
        "marts/fct_orders.sql": 'SELECT * FROM {{ ref("stg_orders") }}\n',
        "staging/stg_bad.sql": INVALID_MODEL,
    })
    connection = {
        "BUILD_TEST_DUCKDB": json.dumps(
            {"type": "duckdb", "instance": str(tmp_path / "test.duckdb")}
        )
    }
    return path, connection


@requires_binary
@requires_build
class TestBuildLive:
    def test_run_success(self, project):
        path, env = project
        result = Build(
            path=str(path), target="BUILD_TEST_DUCKDB", env=env,
            select=["stg_orders", "fct_orders"],
        ).run()

        assert result.success is True
        assert result.exit_code == 0
        assert (result.total, result.ok, result.failed) == (2, 2, 0)
        assert result.ok_names
        assert result.rows == 4  # 2 rows per model
        assert result.failed_nodes == []
        # `run --json` stdout is a single JSON object: logs stay on stderr.
        assert json.loads(result.stdout)["ok"] == 2

    def test_run_failure_reports_the_node(self, project):
        path, env = project
        build = Build(path=str(path), target="BUILD_TEST_DUCKDB", env=env, select="stg_bad")

        with pytest.raises(SlingBuildError) as exc:
            build.run()

        result = exc.value.result
        assert result.exit_code != 0
        assert result.failed == 1
        assert result.failed_nodes[0].name.endswith("stg_bad")
        assert "does not exist" in result.failed_nodes[0].error
        assert json.loads(result.stdout)["failed"] == 1

    def test_list_selects(self, project):
        path, env = project
        nodes = Build(path=str(path), target="BUILD_TEST_DUCKDB", env=env).list()
        assert sorted(n.name for n in nodes) == ["fct_orders", "stg_bad", "stg_orders"]

    def test_compile_renders_ref(self, project):
        path, env = project
        result = Build(path=str(path), target="BUILD_TEST_DUCKDB", env=env).compile()

        assert result.compiled is True
        assert sorted(result.order) == ["fct_orders", "stg_bad", "stg_orders"]
        assert result.order.index("stg_orders") < result.order.index("fct_orders")
        sql = {n.name: n.sql for n in result.nodes}
        assert "staging.stg_orders" in sql["fct_orders"]

    def test_declarative_tests_pass(self, project):
        path, env = project
        build = Build(
            path=str(path), target="BUILD_TEST_DUCKDB", env=env,
            select=["stg_orders", "fct_orders"],
        )

        # `run` materializes and then runs each model's declarative tests
        run_result = build.run()
        assert run_result.success is True

        result = build.test()
        assert result.success is True
        assert result.failed == 0

    def test_declarative_test_failure_is_reported(self, project):
        path, env = project
        (path / "staging" / "stg_dupes.sql").write_text(
            "/**\ntests:\n  - unique: [id]\n**/\nSELECT 1 AS id\nUNION ALL\nSELECT 1 AS id\n"
        )
        build = Build(path=str(path), target="BUILD_TEST_DUCKDB", env=env, select="stg_dupes")
        # `run` also executes the declarative tests, so it fails here too
        build.run(raise_on_error=False)

        with pytest.raises(SlingBuildError) as exc:
            build.test()

        assert exc.value.result.failed == 1
        assert "violating row" in exc.value.result.failed_nodes[0].error

    def test_independent_sub_projects(self, tmp_path):
        for name in ("a", "b"):
            _project(
                tmp_path / name, {"staging/stg_%s.sql" % name: "SELECT 1 AS id\n"}
            )
        env = {
            "BUILD_TEST_DUCKDB": json.dumps(
                {"type": "duckdb", "instance": str(tmp_path / "subs.duckdb")}
            )
        }
        result = Build(
            path=str(tmp_path), target="BUILD_TEST_DUCKDB", env=env, recursive=True
        ).run()

        assert result.success is True
        assert len(result.sub_projects) == 2
        assert result.ok == 2
        assert all(sub.success for sub in result.sub_projects)
        assert sorted(n.name for sub in result.sub_projects for n in sub.results) == [
            "staging.stg_a", "staging.stg_b",
        ]
