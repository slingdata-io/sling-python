
import os, platform

# set binary
BIN_FOLDER = os.path.join(os.path.dirname(__file__), 'bin')

if platform.system() == 'Darwin':
  SLING_BIN = os.path.join(BIN_FOLDER,'sling-mac')
else:
  SLING_BIN = ''
