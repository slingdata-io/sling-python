"""Tests for the golyglot SQL-parser FFI path.

The `sling build` command loads a shared library (polyglot-sql) through
purego to split multi-statement SQL models. That FFI registration panicked
on Windows, because purego rejects struct arguments there. No existing test
reached the parser, so CI stayed green while `sling build` and
`sling agent run` crashed on Windows.

`SplitModelSQL` has a fast path: SQL without a semicolon never calls the
parser. The models below therefore need more than one statement to load
the library.
"""

import json
import os
import subprocess

import pytest

from sling.bin import SLING_BIN

def _has_build_command() -> bool:
    """`sling build` arrived after 1.5.14, so older pinned binaries skip."""
    if not os.path.exists(SLING_BIN):
        return False
    # An older binary exits 0 but falls back to top-level help, so check the
    # text for the build command's own usage line.
    proc = subprocess.run(
        [SLING_BIN, "build", "--help"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
    return "build - " in output


requires_build = pytest.mark.skipif(
    not _has_build_command(), reason="Sling binary has no `build` command"
)

MULTI_STATEMENT_SQL = """create temporary table tmp_parser as select 1 as a;
select a from tmp_parser;
drop table tmp_parser;
"""

SINGLE_STATEMENT_SQL = "select 1 as a\n"


def _make_project(tmp_path, models: dict):
    """Write a minimal sling_build.yml project and return its path."""
    (tmp_path / "sling_build.yml").write_text("target: DUCKDB\n")
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    for name, sql in models.items():
        (models_dir / f"{name}.sql").write_text(sql)
    return tmp_path


def _run_build(project, *args):
    return subprocess.run(
        [SLING_BIN, "build", str(project), *args],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )


def _assert_no_panic(proc):
    """The parser failure mode is a Go panic, not a non-zero exit alone."""
    output = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
    assert "panic:" not in output, f"binary panicked:\n{output}"
    assert "purego" not in output, f"purego FFI failure:\n{output}"
    return output


@requires_build
class TestSQLParserFFI:
    def test_multi_statement_model_compiles(self, tmp_path):
        # Loads the polyglot-sql library and calls polyglot_parse over FFI.
        # This is the call that panicked on Windows.
        project = _make_project(tmp_path, {"multi": MULTI_STATEMENT_SQL})
        proc = _run_build(project, "--compile")
        output = _assert_no_panic(proc)
        assert proc.returncode == 0, output

    def test_multi_statement_model_splits_correctly(self, tmp_path):
        # Assert the parser actually split the statements, rather than
        # silently falling back. --json reports only the model query, so the
        # DDL landing outside `sql` is the proof the split happened.
        project = _make_project(tmp_path, {"multi": MULTI_STATEMENT_SQL})
        proc = _run_build(project, "--compile", "--json")
        output = _assert_no_panic(proc)
        assert proc.returncode == 0, output

        payload = json.loads(proc.stdout.decode())
        nodes = {n["name"]: n for n in payload["nodes"]}
        assert "multi" in nodes, f"model 'multi' not in output: {payload}"

        sql = nodes["multi"]["sql"].lower()
        assert "select" in sql and "tmp_parser" in sql
        assert "create" not in sql and "drop" not in sql

    def test_multi_statement_split_shows_pre_and_post(self, tmp_path):
        # The human-readable --compile output lists the split statements.
        project = _make_project(tmp_path, {"multi": MULTI_STATEMENT_SQL})
        proc = _run_build(project, "--compile")
        output = _assert_no_panic(proc).lower()
        assert proc.returncode == 0, output
        assert "pre_statements" in output and "create temporary table" in output
        assert "post_statements" in output and "drop table" in output

    def test_multi_statement_model_executes(self, tmp_path):
        # Full run, not just compile: pre/post statements execute against DuckDB.
        project = _make_project(tmp_path, {"multi": MULTI_STATEMENT_SQL})
        proc = _run_build(project)
        output = _assert_no_panic(proc)
        assert proc.returncode == 0, output
        assert "0 Failures" in output, output

    def test_single_statement_model_skips_parser(self, tmp_path):
        # Fast path: no semicolon, so the library never loads. Guards the
        # branch that kept this bug hidden.
        project = _make_project(tmp_path, {"single": SINGLE_STATEMENT_SQL})
        proc = _run_build(project, "--compile")
        output = _assert_no_panic(proc)
        assert proc.returncode == 0, output

    def test_parser_loads_when_any_model_is_multi_statement(self, tmp_path):
        # Models are split before selection, so a multi-statement model loads
        # the parser even when another model is selected.
        project = _make_project(
            tmp_path,
            {"single": SINGLE_STATEMENT_SQL, "multi": MULTI_STATEMENT_SQL},
        )
        proc = _run_build(project, "-s", "single", "--compile")
        output = _assert_no_panic(proc)
        assert proc.returncode == 0, output
