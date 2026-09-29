"""Tests for the PATH fallback in sling/bin.py.

When the binary download fails (e.g. SLING_HOME_DIR is not accessible),
bin.py looks for `sling` in PATH. Under `uv run` or an active venv, the first
`sling` in PATH is this package's own console script. The wrapper then ran
itself, which failed the same way and ran itself again, without limit.
See https://github.com/slingdata-io/sling-cli/discussions/818.
"""

import os
import shutil
import signal
import stat
import subprocess
import sys
import sysconfig
import time

import pytest

from sling.bin import SLING_BIN, is_wrapper_launcher

TIMEOUT = 60

SCRIPTS_DIR = sysconfig.get_path('scripts')
LAUNCHER = shutil.which('sling', path=SCRIPTS_DIR)

pytestmark = pytest.mark.skipif(not LAUNCHER, reason=f"no sling console script in {SCRIPTS_DIR}")


def system_path():
  if os.name == 'nt':
    root = os.environ.get('SystemRoot', r'C:\Windows')
    return [os.path.join(root, 'System32'), root]
  return ['/usr/bin', '/bin']


def kill_tree(proc):
  if os.name == 'nt':
    subprocess.run(['taskkill', '/F', '/T', '/PID', str(proc.pid)], capture_output=True)
    return
  # repeat, since members of the group can start new children until killed
  for _ in range(10):
    try:
      os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):  # macOS gives EPERM when only zombies remain
      return
    time.sleep(0.2)


def run_launcher(home_dir, path_dirs):
  env = dict(os.environ)
  for key in ('SLING_BINARY', '_SLING_PYTHON_PATH_FALLBACK'):
    env.pop(key, None)
  env['SLING_HOME_DIR'] = home_dir
  env['PATH'] = os.pathsep.join(path_dirs + system_path())

  proc = subprocess.Popen(
    [LAUNCHER, '--version'],
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    start_new_session=(os.name != 'nt'),
  )
  try:
    out, _ = proc.communicate(timeout=TIMEOUT)
  except subprocess.TimeoutExpired:
    kill_tree(proc)
    proc.communicate()
    pytest.fail(f"wrapper did not exit within {TIMEOUT}s; it likely runs itself in a loop")
  return proc.returncode, out.decode(errors='replace')


@pytest.fixture
def home_under_file(tmp_path):
  """A SLING_HOME_DIR that cannot be created, on all platforms"""
  blocker = tmp_path / 'blocker'
  blocker.write_text('')
  return str(blocker / '.sling')


@pytest.fixture
def home_no_permission(tmp_path):
  if os.name == 'nt' or os.geteuid() == 0:
    pytest.skip("permission bits do not block root or Windows")
  parent = tmp_path / 'noaccess'
  parent.mkdir()
  parent.chmod(0)
  yield str(parent / '.sling')
  parent.chmod(stat.S_IRWXU)


def test_launcher_is_detected():
  assert is_wrapper_launcher(LAUNCHER)


@pytest.mark.parametrize('home_fixture', ['home_under_file', 'home_no_permission'])
def test_no_binary_in_path_fails_fast(request, home_fixture):
  home_dir = request.getfixturevalue(home_fixture)
  code, out = run_launcher(home_dir, [SCRIPTS_DIR])
  assert code != 0, out
  assert 'Could not locate or download sling binary' in out, out
  assert 'SLING_BINARY' in out, out


def test_binary_in_path_after_launcher(home_under_file):
  if not SLING_BIN or is_wrapper_launcher(SLING_BIN):
    pytest.skip("no sling Go binary available")
  code, out = run_launcher(home_under_file, [SCRIPTS_DIR, os.path.dirname(SLING_BIN)])
  assert code == 0, out
  assert 'Could not locate' not in out, out
