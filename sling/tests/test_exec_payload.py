"""exec() must surface an empty or garbled success payload as
SlingConnectionError, not a raw IndexError/JSONDecodeError/AttributeError."""

import json

import pytest

import sling.connection as conn_mod
from sling import Connection, SlingConnectionError


def _payload(fields=("a",), rows=((1,),)):
    return json.dumps({"fields": list(fields), "rows": [list(r) for r in rows]})


def test_exec_empty_stdout_raises_connection_error(monkeypatch):
    monkeypatch.setattr(conn_mod, "_run_sling", lambda *a, **k: (b"", b"", 0))
    with pytest.raises(SlingConnectionError):
        Connection("DUCKDB").exec("select 1 as a")


def test_exec_non_json_payload_raises_connection_error(monkeypatch):
    monkeypatch.setattr(
        conn_mod, "_run_sling", lambda *a, **k: (b"not json at all\n", b"", 0)
    )
    with pytest.raises(SlingConnectionError):
        Connection("DUCKDB").exec("select 1 as a")


def test_exec_non_object_payload_raises_connection_error(monkeypatch):
    monkeypatch.setattr(conn_mod, "_run_sling", lambda *a, **k: (b"[1, 2]\n", b"", 0))
    with pytest.raises(SlingConnectionError):
        Connection("DUCKDB").exec("select 1 as a")


def test_exec_valid_payload_still_parses(monkeypatch):
    monkeypatch.setattr(
        conn_mod, "_run_sling", lambda *a, **k: (_payload().encode(), b"", 0)
    )
    assert Connection("DUCKDB").exec("select 1 as a") == [{"a": 1}]
