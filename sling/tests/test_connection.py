import csv, io, os, subprocess, pytest
from sling.bin import SLING_BIN
from sling import Connection, SlingConnectionError, Sling, Replication

try: import pandas as pd; HAS_PANDAS = True
except ImportError: HAS_PANDAS = False
try: import pyarrow as pa; HAS_ARROW = True
except ImportError: HAS_ARROW = False

requires_binary = pytest.mark.skipif(
    not os.path.exists(SLING_BIN), reason="Sling binary not available"
)

# --- pure-Python (no binary needed) ---
class TestConnectionAPI:
    def test_str_and_repr(self):
        c = Connection("POSTGRES")
        assert str(c) == "POSTGRES"
        assert repr(c) == "Connection('POSTGRES')"
        assert c.name == "POSTGRES"

    def test_invalid_name(self):
        with pytest.raises(ValueError): Connection("")
        with pytest.raises(ValueError): Connection(None)

    def test_invalid_return_type(self):
        with pytest.raises(ValueError, match="return_type"):
            Connection("X").exec("select 1", return_type="numpy")

# --- Sling/Replication accept Connection instance ---
class TestIntegration:
    def test_sling_accepts_connection(self):
        s = Sling(src_conn=Connection("POSTGRES"), tgt_conn=Connection("DUCKDB"),
                  src_stream="public.foo", tgt_object="main.foo")
        assert s.src_conn == "POSTGRES"
        assert s.tgt_conn == "DUCKDB"
        cmd = s._build_command()
        assert cmd[cmd.index("--src-conn") + 1] == "POSTGRES"
        assert cmd[cmd.index("--tgt-conn") + 1] == "DUCKDB"

    def test_replication_accepts_connection(self):
        r = Replication(source=Connection("POSTGRES"), target=Connection("DUCKDB"),
                        streams={"public.foo": {"object": "main.foo"}})
        assert r.source == "POSTGRES" and isinstance(r.source, str)
        assert r.target == "DUCKDB"  and isinstance(r.target, str)

# --- binary required ---
@requires_binary
class TestConnectionTest:
    def test_postgres(self):
        r = Connection("POSTGRES").test()
        assert r.success is True and r.error == ""

    def test_duckdb(self):
        assert Connection("DUCKDB").test().success is True

    def test_nonexistent_returns_failure_no_raise(self):
        # `conns test` exits 1 on bad name but emits JSON — must NOT raise
        r = Connection("NONEXISTENT_CONN_XYZ").test()
        assert r.success is False
        assert r.error  # non-empty error message

@requires_binary
class TestConnectionExec:
    def test_list_default(self):
        rows = Connection("DUCKDB").exec("select 1 as a, 'foo' as b")
        assert rows == [{"a": 1, "b": "foo"}]

    def test_list_explicit(self):
        assert Connection("DUCKDB").exec("select 1 as a", return_type="list") == [{"a": 1}]

    def test_empty_result(self):
        assert Connection("DUCKDB").exec("select 1 as a where 1=0") == []

    @pytest.mark.skipif(not HAS_PANDAS, reason="pandas not installed")
    def test_dataframe(self):
        df = Connection("DUCKDB").exec("select 1 as a, 2 as b", return_type="dataframe")
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["a", "b"]
        assert df.iloc[0]["a"] == 1

    @pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
    def test_dataset(self):
        tbl = Connection("DUCKDB").exec("select 1 as a", return_type="dataset")
        assert isinstance(tbl, pa.Table)
        assert tbl.column_names == ["a"]
        assert tbl.num_rows == 1

    @pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
    def test_arrow_ipc_stream(self):
        # Streaming Arrow IPC path: rows pulled via StreamRowsContext on the
        # binary side, decoded via pyarrow.ipc.open_stream here.
        tbl = Connection("DUCKDB").exec(
            "select 1 as a, 'foo' as b", return_type="arrow"
        )
        assert isinstance(tbl, pa.Table)
        assert tbl.column_names == ["a", "b"]
        assert tbl.num_rows == 1
        assert tbl.column("a")[0].as_py() == 1
        assert tbl.column("b")[0].as_py() == "foo"

    @pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
    def test_arrow_streams_many_rows(self):
        # 25k rows exercises multi-batch flushing (10k/batch in ArrowWriter).
        # limit=0 disables the CLI's default 100-row cap.
        tbl = Connection("DUCKDB").exec(
            "select range as n from range(25000)", return_type="arrow", limit=0,
        )
        assert tbl.num_rows == 25000
        assert tbl.column("n")[0].as_py() == 0
        assert tbl.column("n")[-1].as_py() == 24999

    @pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
    def test_arrow_empty(self):
        tbl = Connection("DUCKDB").exec(
            "select 1 as a where 1=0", return_type="arrow"
        )
        assert tbl.num_rows == 0
        assert tbl.column_names == ["a"]

    @pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
    def test_arrow_failure_raises(self):
        with pytest.raises(SlingConnectionError):
            Connection("DUCKDB").exec(
                "select * from definitely_does_not_exist_xyz", return_type="arrow"
            )

    def test_failure_raises(self):
        with pytest.raises(SlingConnectionError):
            Connection("DUCKDB").exec("select * from definitely_does_not_exist_xyz")

    # --- limit ---

    def test_limit_default_caps_at_100(self):
        # No limit arg → CLI default (100) applies.
        rows = Connection("DUCKDB").exec("select range as n from range(500)")
        assert len(rows) == 100
        assert rows[0] == {"n": 0}
        assert rows[-1] == {"n": 99}

    def test_limit_explicit(self):
        rows = Connection("DUCKDB").exec(
            "select range as n from range(500)", limit=5
        )
        assert rows == [{"n": i} for i in range(5)]

    def test_limit_zero_means_unlimited(self):
        rows = Connection("DUCKDB").exec(
            "select range as n from range(250)", limit=0
        )
        assert len(rows) == 250

    @pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
    def test_limit_with_arrow(self):
        tbl = Connection("DUCKDB").exec(
            "select range as n from range(500)", return_type="arrow", limit=7,
        )
        assert tbl.num_rows == 7
        assert tbl.column("n")[0].as_py() == 0
        assert tbl.column("n")[-1].as_py() == 6

    def test_limit_validation(self):
        with pytest.raises(ValueError, match="limit"):
            Connection("DUCKDB").exec("select 1", limit=-1)
        with pytest.raises(ValueError, match="limit"):
            Connection("DUCKDB").exec("select 1", limit="100")  # type: ignore[arg-type]


# A row that exercises every common type DuckDB serializes differently across
# the three output paths (JSON, Arrow IPC, CSV). Used by the type tests below.
_TYPES_SQL = (
    "select "
    "'hello'::varchar as s, "
    "'big text'::text as t, "
    "42::integer as i, "
    "3.14::double as f, "
    "1234.56::decimal(10,2) as d, "
    "'2024-01-15'::date as dt, "
    "'2024-01-15 12:34:56'::timestamp as ts, "
    "true as b, "
    "'{\"k\":\"v\",\"n\":1}'::json as j"
)


@requires_binary
class TestConnectionTypes:
    """End-to-end type fidelity across the three output paths.

    The CLI serializes the same row differently per format — JSON stringifies
    decimals/booleans/timestamps, Arrow gives native Python types via pyarrow,
    CSV is all strings. These tests pin the contract for each path so a
    regression in the binary's type handling shows up here.
    """

    def test_list_types(self):
        # JSON path: numeric ints/floats stay numeric; decimal/bool/timestamp
        # come through as strings; JSON columns are returned as their raw
        # textual form (not re-parsed into a Python dict).
        row = Connection("DUCKDB").exec(_TYPES_SQL)[0]
        assert row["s"] == "hello"
        assert row["t"] == "big text"
        assert row["i"] == 42 and isinstance(row["i"], int)
        assert row["f"] == 3.14 and isinstance(row["f"], float)
        assert row["d"] == "1234.56"          # decimal -> string in JSON
        assert row["dt"] == "2024-01-15T00:00:00Z"
        assert row["ts"] == "2024-01-15T12:34:56Z"
        assert row["b"] == "true"             # bool -> string in JSON
        assert row["j"] == '{"k":"v","n":1}'  # JSON column -> raw text

    @pytest.mark.skipif(not HAS_ARROW, reason="pyarrow not installed")
    def test_arrow_types(self):
        # Arrow path: native types end-to-end. This is what users get when
        # they want real datetime/Decimal/bool objects without re-parsing.
        import datetime as dt
        from decimal import Decimal
        tbl = Connection("DUCKDB").exec(_TYPES_SQL, return_type="arrow")
        assert tbl.num_rows == 1
        schema = tbl.schema
        # Schema sanity — the binary should preserve precision/timezone info.
        assert str(schema.field("s").type) == "string"
        assert str(schema.field("t").type) == "string"
        assert str(schema.field("i").type) == "int32"
        assert str(schema.field("f").type) == "double"
        assert str(schema.field("d").type).startswith("decimal128")
        assert str(schema.field("dt").type) == "date32[day]"
        ts_type = str(schema.field("ts").type)
        assert ts_type.startswith("timestamp[") and "tz=UTC" in ts_type
        assert str(schema.field("b").type) == "bool"

        get = lambda c: tbl.column(c)[0].as_py()
        assert get("s") == "hello"
        assert get("t") == "big text"
        assert get("i") == 42
        assert get("f") == 3.14
        assert get("d") == Decimal("1234.56")
        assert get("dt") == dt.date(2024, 1, 15)
        ts_val = get("ts")
        assert isinstance(ts_val, dt.datetime)
        assert ts_val.year == 2024 and ts_val.month == 1 and ts_val.day == 15
        assert ts_val.hour == 12 and ts_val.minute == 34 and ts_val.second == 56
        assert ts_val.utcoffset() == dt.timedelta(0)  # UTC
        assert get("b") is True
        assert get("j") == '{"k":"v","n":1}'  # JSON arrives as utf8 string

    def test_csv_types(self):
        # CSV path: everything is a string post-parse; the binary's
        # CastToStringCSV decides the textual form. Pin the formats so
        # downstream consumers can rely on them.
        proc = _run_cli_csv(_TYPES_SQL)
        assert proc.returncode == 0
        rows = list(csv.reader(io.StringIO(proc.stdout.decode())))
        assert rows[0] == ["s", "t", "i", "f", "d", "dt", "ts", "b", "j"]
        assert rows[1] == [
            "hello",
            "big text",
            "42",
            "3.14",
            "1234.56",
            "2024-01-15",
            "2024-01-15 12:34:56 +00",
            "true",
            '{"k":"v","n":1}',
        ]


def _run_cli_csv(sql: str, limit: int = None) -> "subprocess.CompletedProcess":
    # `--output csv` is a CLI-only flag (not exposed through Connection.exec),
    # so shell out directly. stderr is discarded — logs go there but the CSV
    # stream stays clean on stdout.
    cmd = [SLING_BIN, "conns", "exec", "DUCKDB", sql, "--output", "csv"]
    if limit is not None:
        cmd += ["--limit", str(limit)]
    return subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )


@requires_binary
class TestCSVStreaming:
    def test_basic_header_and_row(self):
        proc = _run_cli_csv("select 1 as a, 'hi' as b, 3.14 as c")
        assert proc.returncode == 0
        rows = list(csv.reader(io.StringIO(proc.stdout.decode())))
        assert rows == [["a", "b", "c"], ["1", "hi", "3.14"]]

    def test_quotes_strings_with_commas(self):
        # Type-aware CSV (csv.NewWriter on the binary side) must RFC-4180-quote
        # values containing the delimiter.
        proc = _run_cli_csv("select 'foo, bar' as msg")
        assert proc.returncode == 0
        # Raw bytes contain the literal quoted form
        assert b'"foo, bar"' in proc.stdout
        # And it round-trips through csv.reader
        rows = list(csv.reader(io.StringIO(proc.stdout.decode())))
        assert rows == [["msg"], ["foo, bar"]]

    def test_streams_large_result(self):
        # 25k rows exercises the streaming path (StreamRowsContext). If this
        # ever materializes the whole result in memory the test still passes
        # but adds latency; the row count is the real correctness check.
        # limit=0 disables the CLI's default 100-row cap.
        proc = _run_cli_csv("select range as n from range(25000)", limit=0)
        assert proc.returncode == 0
        lines = proc.stdout.decode().splitlines()
        assert lines[0] == "n"
        assert len(lines) == 25001  # header + 25k rows
        assert lines[1] == "0"
        assert lines[-1] == "24999"

    def test_empty_result_emits_header_only(self):
        proc = _run_cli_csv("select 1 as a where 1=0")
        assert proc.returncode == 0
        assert proc.stdout.decode().splitlines() == ["a"]

    def test_failure_exits_nonzero_with_empty_stdout(self):
        proc = _run_cli_csv("select * from definitely_does_not_exist_xyz")
        assert proc.returncode != 0
        assert proc.stdout == b""
        assert proc.stderr  # error message lives on stderr


@requires_binary
class TestEnvVarConnection:
    def test_env_var_defined_connection(self, tmp_path):
        os.environ["DUCK_TEST_CONN"] = f"duckdb://{tmp_path / 'dt.db'}"
        try:
            assert Connection("DUCK_TEST_CONN").test().success is True
        finally:
            os.environ.pop("DUCK_TEST_CONN", None)
