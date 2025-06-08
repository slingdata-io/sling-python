import os, sys, platform

#################################################################
# Logic to import the proper binary for the respective operating 
# systems and architecture. Since the binaries are built in Go, 
# they need to be added to the PyPi sling package via a `MANIFEST.in` file.
# And since there is approximately one binary per OS/ARCH,
# it is necessary to split them out into their own PyPi package
# to avoid exceeding the PyPi quotas. This also allows a faster 
# install via pip and saves bandwidth.

# For development
SLING_BASE = os.path.join(os.path.dirname(__file__), '..', '..', 'sling_base')
insert = lambda f: sys.path.insert(1, os.path.join(SLING_BASE, f))
insert('sling-windows-amd64')
insert('sling-linux-amd64')
insert('sling-linux-arm64')
insert('sling-mac-amd64')
insert('sling-mac-arm64')

# allows provision of a custom path for sling binary
SLING_BIN = os.getenv("SLING_BINARY")

if not SLING_BIN:
  if platform.system() == 'Linux':
    if platform.machine() == 'aarch64':
      exec('from sling_linux_arm64 import SLING_BIN')
    else:
      exec('from sling_linux_amd64 import SLING_BIN')
  elif platform.system() == 'Windows':
    if platform.machine() == 'ARM64':
      exec('from sling_windows_arm64 import SLING_BIN')
    else:
      exec('from sling_windows_amd64 import SLING_BIN')
  elif platform.system() == 'Darwin':
    if platform.machine() == 'arm64':
      exec('from sling_mac_arm64 import SLING_BIN')
    else:
      exec('from sling_mac_amd64 import SLING_BIN')

