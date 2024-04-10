
import os, platform, pathlib

# set binary
BIN_FOLDER = os.path.join(os.path.dirname(__file__), 'bin')

if platform.system() == 'Windows':
  if platform.machine() == 'ARM64':
    SLING_BIN = os.path.join(BIN_FOLDER,'sling-win-arm64.exe')
  else:
    SLING_BIN = os.path.join(BIN_FOLDER,'sling-win-amd64.exe')
else:
  SLING_BIN = ''

SLING_VERSION = '0.0.0dev'

version_path = pathlib.Path(os.path.join(BIN_FOLDER,'VERSION'))
if version_path.exists():
  with version_path.open() as file:
    SLING_VERSION = file.read().strip()