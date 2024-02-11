
import os, platform, pathlib

# set binary
BIN_FOLDER = os.path.join(os.path.dirname(__file__), 'bin')

if platform.system() == 'Darwin':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-mac')
else:
  SLING_BIN = ''

SLING_VERSION = 'dev'

version_path = pathlib.Path(os.path.join(BIN_FOLDER,'VERSION'))
if version_path.exists():
  with version_path.open() as file:
    SLING_VERSION = file.read().strip()