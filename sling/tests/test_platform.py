import json, os, re, subprocess, pytest
from unittest.mock import MagicMock
from sling.bin import SLING_BIN
from sling import (
    Platform, SlingPlatformError,
    PlatformJobs, PlatformExecs, PlatformFiles, PlatformConnections,
)

requires_binary = pytest.mark.skipif(
    not os.path.exists(SLING_BIN), reason="Sling binary not available"
)
requires_token = pytest.mark.skipif(
    not os.environ.get("SLING_PROJECT_TOKEN"),
    reason="SLING_PROJECT_TOKEN not set",
)


def _proc(stdout="", stderr="", code=0):
    proc = MagicMock()
    proc.stdout = stdout.encode() if isinstance(stdout, str) else stdout
    proc.stderr = stderr.encode() if isinstance(stderr, str) else stderr
    proc.returncode = code
    return proc


def _cmd(mock):
    return mock.call_args[0][0]


@pytest.fixture
def run(mocker):
    return mocker.patch("sling.platform.subprocess.run", return_value=_proc(stdout="[]"))


# --- pure-Python (no binary needed) ---

class TestPlatformAPI:
    def test_repr_and_nested(self):
        p = Platform(cwd="/tmp/proj")
        assert repr(p) == "Platform(cwd='/tmp/proj')"
        assert isinstance(p.jobs, PlatformJobs)
        assert isinstance(p.execs, PlatformExecs)
        assert isinstance(p.files, PlatformFiles)
        assert isinstance(p.connections, PlatformConnections)

    def test_token_and_cwd_passed_to_subprocess(self, run):
        run.return_value = _proc(stdout="ok")
        p = Platform(token="tok_abc", cwd="/tmp/proj")
        p.status()
        kwargs = run.call_args.kwargs
        assert kwargs["cwd"] == "/tmp/proj"
        assert kwargs["env"]["SLING_PROJECT_TOKEN"] == "tok_abc"
        assert kwargs["env"]["SLING_PACKAGE"] == "python"
        assert _cmd(run) == [SLING_BIN, "platform", "status"]

    def test_no_token_does_not_override_env(self, run, monkeypatch):
        monkeypatch.setenv("SLING_PROJECT_TOKEN", "from-env")
        run.return_value = _proc(stdout="ok")
        Platform().status()
        env = run.call_args.kwargs["env"]
        assert env["SLING_PROJECT_TOKEN"] == "from-env"

    def test_nonzero_raises(self, run):
        run.return_value = _proc(stderr="invalid project token", code=1)
        with pytest.raises(SlingPlatformError, match="invalid project token"):
            Platform().status()

    def test_binary_missing_raises(self, mocker):
        mocker.patch(
            "sling.platform.subprocess.run",
            side_effect=FileNotFoundError("nope"),
        )
        with pytest.raises(SlingPlatformError, match="sling binary not found"):
            Platform().status()

    def test_old_binary_without_platform_raises(self, run):
        run.return_value = _proc(
            stderr="Usage:\n    sling [agent|conns|project|run|serve|update]\n",
            code=0,
        )
        with pytest.raises(SlingPlatformError, match="does not support"):
            Platform().status()


class TestPlatformSyncInit:
    def test_sync_defaults_force(self, run):
        run.return_value = _proc(stdout="")
        Platform().sync()
        assert _cmd(run) == [SLING_BIN, "platform", "sync", "--force"]

    def test_sync_no_force_and_debug(self, run):
        run.return_value = _proc(stdout="")
        Platform().sync(force=False, debug=True)
        assert _cmd(run) == [SLING_BIN, "platform", "sync", "--debug"]

    def test_init(self, run):
        run.return_value = _proc(stdout="wrote .sling.json")
        out = Platform().init()
        assert _cmd(run) == [SLING_BIN, "platform", "init"]
        assert "wrote" in out


class TestPlatformJobs:
    def test_list_filters(self, run):
        run.return_value = _proc(stdout='[{"id":"job_1","name":"Users"}]')
        rows = Platform().jobs.list(job_type="replication", name="Users", file_name="replications/users.yaml")
        assert rows == [{"id": "job_1", "name": "Users"}]
        cmd = _cmd(run)
        assert cmd[:4] == [SLING_BIN, "platform", "jobs", "list"]
        assert cmd[cmd.index("--type") + 1] == "replication"
        assert cmd[cmd.index("--name") + 1] == "Users"
        assert cmd[cmd.index("--file-name") + 1] == "replications/users.yaml"
        assert cmd[cmd.index("-o") + 1] == "json"

    def test_list_empty(self, run):
        run.return_value = _proc(stdout="[]")
        assert Platform().jobs.list() == []

    def test_status_filters(self, run):
        run.return_value = _proc(stdout='[{"job_id":"job_1"}]')
        rows = Platform().jobs.status(job_id="job_1", name="users")
        assert rows == [{"job_id": "job_1"}]
        cmd = _cmd(run)
        assert cmd[cmd.index("--id") + 1] == "job_1"
        assert cmd[cmd.index("--name") + 1] == "users"
        assert "-o" in cmd

    def test_get(self, run):
        job = {"id": "job_abc", "name": "Users", "type": "replication"}
        run.return_value = _proc(stdout=json.dumps(job, indent=2))
        got = Platform().jobs.get("job_abc")
        assert got == job
        assert _cmd(run) == [SLING_BIN, "platform", "jobs", "get", "job_abc"]

    def test_get_requires_id(self):
        with pytest.raises(ValueError, match="job_id"):
            Platform().jobs.get("")

    def test_save_dict_via_stdin(self, run):
        saved = {"id": "job_new", "name": "Users"}
        run.return_value = _proc(stdout=json.dumps(saved, indent=2))
        payload = {"name": "Users", "type": "replication", "file_name": "replications/users.yaml"}
        got = Platform().jobs.save(job=payload)
        assert got == saved
        cmd = _cmd(run)
        assert cmd[-2:] == ["-f", "-"]
        assert json.loads(run.call_args.kwargs["input"]) == payload

    def test_save_file(self, run):
        run.return_value = _proc(stdout='{"id":"job_1"}')
        Platform().jobs.save(file="job.json")
        assert _cmd(run)[-2:] == ["-f", "job.json"]
        assert run.call_args.kwargs["input"] is None

    def test_save_requires_exactly_one(self):
        with pytest.raises(ValueError, match="exactly one"):
            Platform().jobs.save()
        with pytest.raises(ValueError, match="exactly one"):
            Platform().jobs.save(job={}, file="x.json")

    def test_trigger_parses_exec_id(self, run):
        run.return_value = _proc(stderr="12:00:00 INF Started Job (exec_id=exec_xyz)\n")
        exec_id = Platform().jobs.trigger("job_abc")
        assert exec_id == "exec_xyz"
        assert _cmd(run) == [SLING_BIN, "platform", "jobs", "trigger", "job_abc"]

    def test_trigger_wait_streams_full_refresh(self, run):
        run.return_value = _proc(
            stderr="Started Job (exec_id=exec_1)\nExecution completed: status=Success rows=10 bytes=100 duration=1s\n"
        )
        exec_id = Platform().jobs.trigger(
            "job_abc", wait=True, streams=["s1", "s2"], full_refresh=True,
        )
        assert exec_id == "exec_1"
        cmd = _cmd(run)
        assert "--wait" in cmd
        assert "--full-refresh" in cmd
        assert cmd[cmd.index("--streams") + 1] == "s1,s2"

    def test_trigger_missing_exec_id_raises(self, run):
        run.return_value = _proc(stderr="something went wrong but exit 0")
        with pytest.raises(SlingPlatformError, match="could not parse exec_id"):
            Platform().jobs.trigger("job_abc")

    def test_activate_deactivate_delete(self, run):
        run.return_value = _proc(stderr="ok")
        p = Platform()
        p.jobs.activate("job_abc")
        assert _cmd(run) == [SLING_BIN, "platform", "jobs", "activate", "job_abc"]
        p.jobs.deactivate("job_abc")
        assert _cmd(run) == [SLING_BIN, "platform", "jobs", "deactivate", "job_abc"]
        p.jobs.delete("job_abc")
        assert _cmd(run) == [SLING_BIN, "platform", "jobs", "delete", "job_abc", "--force"]


class TestPlatformExecs:
    def test_list_filters(self, run):
        run.return_value = _proc(stdout='[{"id":"exec_1"}]')
        rows = Platform().execs.list(
            job_id="job_abc", status="error", since="7d", until="2026-01-01", limit=5,
        )
        assert rows == [{"id": "exec_1"}]
        cmd = _cmd(run)
        assert cmd[:4] == [SLING_BIN, "platform", "executions", "list"]
        assert cmd[cmd.index("--job-id") + 1] == "job_abc"
        assert cmd[cmd.index("--status") + 1] == "error"
        assert cmd[cmd.index("--since") + 1] == "7d"
        assert cmd[cmd.index("--until") + 1] == "2026-01-01"
        assert cmd[cmd.index("--limit") + 1] == "5"
        assert cmd[cmd.index("-o") + 1] == "json"

    def test_list_job_id_and_name_exclusive(self):
        with pytest.raises(ValueError, match="only one"):
            Platform().execs.list(job_id="job_1", job_name="users")

    def test_list_limit_validation(self):
        with pytest.raises(ValueError, match="limit"):
            Platform().execs.list(limit=-1)
        with pytest.raises(ValueError, match="limit"):
            Platform().execs.list(limit="10")  # type: ignore[arg-type]

    def test_status(self, run):
        rec = {"id": "exec_1", "status": "success"}
        run.return_value = _proc(stdout=json.dumps(rec, indent=2))
        assert Platform().execs.status("exec_1") == rec
        assert _cmd(run) == [SLING_BIN, "platform", "executions", "status", "exec_1"]

    def test_log(self, run):
        run.return_value = _proc(stdout='[{"stream_name":"users","output":"ok"}]')
        rows = Platform().execs.log("exec_1", task="users", status="error", job_type="replication")
        assert rows[0]["stream_name"] == "users"
        cmd = _cmd(run)
        assert cmd[:5] == [SLING_BIN, "platform", "executions", "log", "exec_1"]
        assert "--no-color" in cmd
        assert cmd[cmd.index("--task") + 1] == "users"
        assert cmd[cmd.index("--status") + 1] == "error"
        assert cmd[cmd.index("--type") + 1] == "replication"
        assert cmd[cmd.index("-o") + 1] == "json"

    def test_cancel(self, run):
        run.return_value = _proc(stderr="Cancelled")
        Platform().execs.cancel("exec_1")
        assert _cmd(run) == [SLING_BIN, "platform", "executions", "cancel", "exec_1"]


class TestPlatformFiles:
    def test_list(self, run):
        run.return_value = _proc(stdout='[{"name":"replications/users.yaml","size":12}]')
        rows = Platform().files.list()
        assert rows[0]["name"] == "replications/users.yaml"
        assert _cmd(run)[-2:] == ["-o", "json"]

    def test_get_preserves_body(self, run):
        body = "source: PG\ntarget: SF\nstreams:\n  public.users:\n"
        run.return_value = _proc(stdout=body)
        assert Platform().files.get("replications/users.yaml") == body
        assert _cmd(run) == [SLING_BIN, "platform", "files", "get", "replications/users.yaml"]

    def test_save_body_via_stdin(self, run):
        run.return_value = _proc(stdout='{"name":"replications/users.yaml","size":4}')
        got = Platform().files.save("replications/users.yaml", body="abcd")
        assert got["size"] == 4
        cmd = _cmd(run)
        assert cmd[cmd.index("-f") + 1] == "-"
        assert run.call_args.kwargs["input"] == b"abcd"
        assert cmd[cmd.index("-o") + 1] == "json"

    def test_save_file(self, run):
        run.return_value = _proc(stdout='{"name":"replications/users.yaml"}')
        Platform().files.save("replications/users.yaml", file="users.yaml")
        cmd = _cmd(run)
        assert cmd[cmd.index("-f") + 1] == "users.yaml"
        assert run.call_args.kwargs["input"] is None

    def test_save_dir(self, run):
        run.return_value = _proc(stdout='{"name":"monitors"}')
        Platform().files.save("monitors/", is_dir=True)
        assert "--dir" in _cmd(run)

    def test_save_validation(self):
        with pytest.raises(ValueError, match="file name"):
            Platform().files.save("")
        with pytest.raises(ValueError, match="both"):
            Platform().files.save("a.yaml", body="x", file="x.yaml")
        with pytest.raises(ValueError, match="must provide one"):
            Platform().files.save("a.yaml")

    def test_delete_and_rename(self, run):
        run.return_value = _proc(stderr="ok")
        p = Platform()
        p.files.delete("replications/users.yaml")
        assert _cmd(run) == [SLING_BIN, "platform", "files", "delete", "replications/users.yaml", "--force"]
        p.files.rename("old.yaml", "new.yaml")
        assert _cmd(run) == [SLING_BIN, "platform", "files", "rename", "old.yaml", "new.yaml"]


class TestPlatformConnections:
    def test_list(self, run):
        run.return_value = _proc(stdout='[{"name":"POSTGRES","type":"postgres"}]')
        rows = Platform().connections.list()
        assert rows[0]["name"] == "POSTGRES"
        assert _cmd(run)[-2:] == ["-o", "json"]

    def test_test_success(self, run):
        run.return_value = _proc(stderr="Connection POSTGRES: valid")
        r = Platform().connections.test("POSTGRES")
        assert r.success is True and r.error == ""
        assert _cmd(run) == [SLING_BIN, "platform", "connections", "test", "POSTGRES"]

    def test_test_invalid_returns_failure(self, run):
        run.return_value = _proc(stderr="Connection X: invalid — connection refused", code=1)
        r = Platform().connections.test("X")
        assert r.success is False
        assert "connection refused" in r.error

    def test_test_token_error_raises(self, run):
        run.return_value = _proc(stderr="did not provide the SLING_PROJECT_TOKEN environment variable", code=1)
        with pytest.raises(SlingPlatformError, match="SLING_PROJECT_TOKEN"):
            Platform().connections.test("POSTGRES")

    def test_test_requires_name(self):
        with pytest.raises(ValueError, match="connection name"):
            Platform().connections.test("")


class TestJsonParsing:
    def test_pretty_json(self, run):
        run.return_value = _proc(stdout='{\n  "id": "job_1"\n}\n')
        assert Platform().jobs.get("job_1") == {"id": "job_1"}

    def test_last_line_fallback(self, run):
        run.return_value = _proc(stdout='not-json\n{"id":"job_1"}\n')
        assert Platform().jobs.get("job_1") == {"id": "job_1"}

    def test_empty_json_raises(self, run):
        run.return_value = _proc(stdout="")
        with pytest.raises(SlingPlatformError, match="empty JSON"):
            Platform().jobs.list()

    def test_garbage_json_raises(self, run):
        run.return_value = _proc(stdout="not json at all")
        with pytest.raises(SlingPlatformError, match="could not parse JSON"):
            Platform().jobs.list()


def _platform_supported() -> bool:
    if not os.path.exists(SLING_BIN):
        return False
    try:
        proc = subprocess.run(
            [SLING_BIN, "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
            check=False,
        )
        text = (proc.stdout + proc.stderr).decode("utf-8", errors="replace")
        m = re.search(r"Usage:\s+sling \[([^\]]+)\]", text)
        return bool(m and "platform" in m.group(1).split("|"))
    except Exception:
        return False


requires_platform_cmd = pytest.mark.skipif(
    not _platform_supported(),
    reason="sling binary does not support `platform` (need 1.6+)",
)


# --- optional live tests (need token + a binary that has `platform`) ---

@requires_binary
@requires_token
@requires_platform_cmd
class TestPlatformLive:
    def test_status(self):
        out = Platform().status()
        assert out

    def test_jobs_list(self):
        rows = Platform().jobs.list()
        assert isinstance(rows, list)

    def test_files_list(self):
        rows = Platform().files.list()
        assert isinstance(rows, list)

    def test_connections_list(self):
        rows = Platform().connections.list()
        assert isinstance(rows, list)

    def test_execs_list(self):
        rows = Platform().execs.list(limit=1)
        assert isinstance(rows, list)
