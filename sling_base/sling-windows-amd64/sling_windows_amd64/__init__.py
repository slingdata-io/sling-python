
import os, platform

# set binary
BIN_FOLDER = os.path.join(os.path.dirname(__file__), 'bin')

if platform.system() == 'Windows':
  if platform.machine() == 'aarch64':
    SLING_BIN = os.path.join(BIN_FOLDER,'sling-win-arm64.exe')
  else:
    SLING_BIN = os.path.join(BIN_FOLDER,'sling-win-amd64.exe')
else:
  SLING_BIN = ''
