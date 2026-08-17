"""
ADBC Postgres integration tests.

Exercises the ADBC (Arrow Database Connectivity) code path end to end against a
real Postgres, on macOS, Linux and Windows. ADBC loads the driver manager and
the Postgres driver as native shared libraries over FFI, so the failure modes
are platform-specific and invisible to the regular pgx-based Postgres tests.

Motivating regression: ADBC panicked on Windows during driver-manager symbol
registration ("purego: struct arguments are only supported on darwin and
linux"), making every ADBC connection unusable there while macOS and Linux
passed. See slingdata-io/sling-cli#783.

Requirements (all three must hold or the tests skip):
  - the sling binary
  - a reachable Postgres (the POSTGRES connection)
  - the ADBC Postgres driver, installed via `dbc install postgresql`

The ADBC connection is built here rather than read from env.yaml so the test is
self-contained and does not depend on a machine-specific driver path. `driver`
is deliberately omitted: sling resolves it per-platform from the standard dbc
install locations, which is the behaviour we want covered.
"""
import json
import os
import subprocess
import uuid

import pytest

from sling import Connection, Sling
from sling.bin import SLING_BIN

# Built from the plain POSTGRES connection, which CI already provides.
ADBC_CONN_NAME = "ADBC_POSTGRES_TEST"
PG_CONN_NAME = os.environ.get("ADBC_TEST_PG_CONN", "POSTGRES")


def _extract_url(val):
    """Pull a Postgres URL out of a connection definition.

    A connection may be a bare URL or a YAML/JSON mapping with a `url` key
    alongside other properties (`schema`, etc). CI supplies the mapping form.
    """
    if not isinstance(val, str):
        return None

    val = val.strip()
    if val.startswith(("postgres://", "postgresql://")):
        return os.path.expandvars(val)

    try:
        import yaml
    except ImportError:
        return None
    try:
        parsed = yaml.safe_load(val)
    except yaml.YAMLError:
        return None

    if isinstance(parsed, dict):
        url = parsed.get("url")
        if isinstance(url, str) and url.startswith(("postgres://", "postgresql://")):
            return os.path.expandvars(url)
    return None


def _pg_url():
    """Resolve the Postgres URL for the POSTGRES connection.

    Checked in order: an explicit override, the env var backing the connection
    (how CI supplies it), then ~/.sling/env.yaml (how a dev machine has it).
    """
    for key in ("ADBC_TEST_PG_URL", PG_CONN_NAME):
        url = _extract_url(os.environ.get(key))
        if url:
            return url

    env_yaml = os.path.join(
        os.environ.get("SLING_HOME_DIR") or os.path.join(
            os.path.expanduser("~"), ".sling"
        ),
        "env.yaml",
    )
    try:
        import yaml
    except ImportError:
        return None
    try:
        with open(env_yaml) as f:
            conns = (yaml.safe_load(f) or {}).get("connections") or {}
    except (OSError, yaml.YAMLError):
        return None

    conn = conns.get(PG_CONN_NAME) or conns.get(PG_CONN_NAME.lower())
    if not isinstance(conn, dict):
        return None
    # Only a plain URL is usable here; component-style configs would need the
    # CLI's own resolution, which this test intentionally does not reimplement.
    return _extract_url(conn.get("url"))


def _adbc_conn_spec():
    """JSON spec for an ADBC connection, or None if no Postgres URL is known.

    `driver` is deliberately omitted so sling resolves it per-platform from the
    standard dbc install locations.
    """
    url = _pg_url()
    if not url:
        return None
    return json.dumps({
        "type": "adbc",
        "driver_name": "postgresql",
        "adbc.postgresql.connection_string": url,
    })


def _adbc_env():
    """Environment with an ADBC connection defined, or None if unavailable."""
    spec = _adbc_conn_spec()
    if spec is None:
        return None
    env = os.environ.copy()
    env[ADBC_CONN_NAME] = spec
    return env


def _run_sling(args, env=None, timeout=180):
    return subprocess.run(
        [SLING_BIN] + args,
        capture_output=True, text=True, timeout=timeout,
        env=env if env is not None else os.environ.copy(),
    )


# A driver that was never installed is an environment gap -> skip. Anything else
# is a real failure -> run the tests so they report it. Skipping on every error
# would hide sling-cli#783, the regression this module exists to catch.
#
# Keep these narrow and specific. Broad substrings ("no such file",
# "adbc_driver_manager") also match a driver manager that IS present but fails to
# load — e.g. a GLIBCXX version mismatch — which must fail loudly, not skip.
_DRIVER_MISSING_MARKERS = (
    "dlopen_failed",
    "dlopen failed",
    "could not load driver",
    "image not found",
)


def _required():
    """Whether a missing prerequisite should fail instead of skip.

    CI sets ADBC_TESTS_REQUIRED so an environment gap surfaces as a failure —
    otherwise a broken ADBC stack would quietly report a green build.
    """
    return os.environ.get("ADBC_TESTS_REQUIRED", "").lower() in ("1", "true", "yes")


def _preflight():
    """(ok, skip_reason). skip_reason is None when the tests should run."""
    env = _adbc_env()
    if env is None:
        return False, "no Postgres URL"

    # Is plain Postgres reachable at all? If not, this is an environment gap
    # unrelated to ADBC.
    try:
        pg = _run_sling(["conns", "test", PG_CONN_NAME], env=env, timeout=120)
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, f"Postgres unreachable ({e.__class__.__name__})"
    if pg.returncode != 0:
        return False, f"Postgres ({PG_CONN_NAME}) not reachable"

    # Postgres is up, so an ADBC failure now is meaningful — unless the driver
    # simply is not installed on this machine.
    try:
        r = _run_sling(["conns", "test", ADBC_CONN_NAME], env=env, timeout=120)
    except (subprocess.TimeoutExpired, OSError) as e:
        return False, f"ADBC connect timed out ({e.__class__.__name__})"

    raw = r.stdout + r.stderr
    combined = raw.lower()
    if r.returncode != 0 or "error" in combined:
        if any(m in combined for m in _DRIVER_MISSING_MARKERS):
            # include the real output — a bare "not installed" hides why
            return False, (
                "ADBC Postgres driver not installed "
                f"(install with `dbc install postgresql`); sling said: {raw.strip()}"
            )
        # Driver is present but ADBC still failed — let the tests run and fail.
        return True, None
    return True, None


requires_adbc = pytest.mark.skipif(
    not os.path.exists(SLING_BIN),
    reason="sling binary not available",
)


@pytest.fixture(scope="module", autouse=True)
def adbc_env():
    """Skip the whole module unless the ADBC stack is usable.

    Exports the connection into os.environ for the duration of the module so
    Connection(), which inherits the ambient environment, can resolve it too.
    """
    def unavailable(reason):
        if _required():
            pytest.fail(f"{reason} (ADBC_TESTS_REQUIRED is set)")
        pytest.skip(reason)

    if not os.path.exists(SLING_BIN):
        unavailable("sling binary not available")
    spec = _adbc_conn_spec()
    if spec is None:
        unavailable(
            f"no Postgres URL: set ${PG_CONN_NAME} or $ADBC_TEST_PG_URL"
        )

    prior = os.environ.get(ADBC_CONN_NAME)
    os.environ[ADBC_CONN_NAME] = spec
    try:
        ok, reason = _preflight()
        if not ok:
            unavailable(reason)
        yield os.environ.copy()
    finally:
        if prior is None:
            os.environ.pop(ADBC_CONN_NAME, None)
        else:
            os.environ[ADBC_CONN_NAME] = prior


@pytest.fixture
def temp_table(adbc_env):
    """A uniquely named table, dropped afterwards."""
    name = f"public.adbc_test_{uuid.uuid4().hex[:12]}"
    yield name
    _run_sling(
        ["conns", "exec", PG_CONN_NAME, f"drop table if exists {name}"],
        env=adbc_env,
    )


@requires_adbc
class TestAdbcPostgresConnection:
    def test_connection_succeeds(self, adbc_env):
        """The ADBC driver manager loads and the driver connects.

        This is the assertion that fails on a platform where registering the
        ADBC symbols panics — the regression behind sling-cli#783.
        """
        r = _run_sling(["conns", "test", ADBC_CONN_NAME], env=adbc_env)
        assert r.returncode == 0, f"conns test failed: {r.stderr or r.stdout}"
        combined = r.stdout + r.stderr
        assert "panic" not in combined.lower(), f"panic during connect: {combined}"

    def test_no_purego_panic(self, adbc_env):
        """Guard the specific Windows failure mode, with a legible message."""
        r = _run_sling(["conns", "test", ADBC_CONN_NAME], env=adbc_env)
        combined = (r.stdout + r.stderr).lower()
        assert "purego" not in combined, (
            "purego rejected an ADBC binding on this platform — see "
            f"sling-cli#783: {r.stdout + r.stderr}"
        )

    def test_query_returns_rows(self, adbc_env):
        """A query round-trips through the Arrow result path."""
        r = _run_sling(
            ["conns", "exec", ADBC_CONN_NAME, "select 1 as one, 'abc' as word"],
            env=adbc_env,
        )
        assert r.returncode == 0, f"query failed: {r.stderr or r.stdout}"

    def test_discover_lists_objects(self, adbc_env):
        """Schema discovery works over ADBC (exercises metadata calls)."""
        r = _run_sling(["conns", "discover", ADBC_CONN_NAME], env=adbc_env)
        assert r.returncode == 0, f"discover failed: {r.stderr or r.stdout}"


@requires_adbc
class TestAdbcPostgresDataFlow:
    def test_write_then_read_back(self, adbc_env, temp_table):
        """Load rows in over ADBC, then read them back and verify contents."""
        rows = [
            {"id": 1, "name": "alice", "score": 10},
            {"id": 2, "name": "bob", "score": 20},
            {"id": 3, "name": "carol", "score": 30},
        ]

        Sling(
            input=rows,
            tgt_conn=ADBC_CONN_NAME,
            tgt_object=temp_table,
            mode="full-refresh",
            env=adbc_env,
        ).run(print_output=False)

        # Read back through the same ADBC connection.
        out = Connection(ADBC_CONN_NAME).exec(
            f"select id, name, score from {temp_table} order by id",
        )
        got = [
            {"id": int(r["id"]), "name": r["name"], "score": int(r["score"])}
            for r in out
        ]
        assert got == rows

    def test_read_to_csv(self, adbc_env, temp_table, tmp_path):
        """Stream an ADBC source out to a file target."""
        Sling(
            input=[{"id": i, "val": f"v{i}"} for i in range(1, 6)],
            tgt_conn=ADBC_CONN_NAME,
            tgt_object=temp_table,
            mode="full-refresh",
            env=adbc_env,
        ).run(print_output=False)

        out_file = tmp_path / "adbc_out.csv"
        Sling(
            src_conn=ADBC_CONN_NAME,
            src_stream=temp_table,
            tgt_object=f"file://{out_file}",
            env=adbc_env,
        ).run(print_output=False)

        assert out_file.exists(), "no output file written"
        lines = out_file.read_text().strip().splitlines()
        assert len(lines) == 6, f"expected header + 5 rows, got {len(lines)}"
        assert "id" in lines[0] and "val" in lines[0]
