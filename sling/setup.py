import os, sys, platform
from setuptools import setup
from setuptools import find_packages
from sling import cli, SLING_BIN

version = cli('--version', return_output=True).strip().replace('Version: ', '')

if not version:
  raise Exception('version is blank')
elif version == 'dev':
  version='v0.0.dev'

binary_name = os.path.split(SLING_BIN)[-1]

install_requires = []
if platform.system() == 'Linux':
  if platform.machine() == 'aarch64':
    install_requires = ['sling-linux-arm64']
  else:
    install_requires = ['sling-linux-amd64']
elif platform.system() == 'Windows':
  if platform.machine() == 'aarch64':
    install_requires = ['sling-windows-arm64']
  else:
    install_requires = ['sling-windows-amd64']
elif platform.system() == 'Darwin':
  install_requires = ['sling-mac-universal']
else:
  raise Exception(f'platform "{platform.system()}" ({platform.system()}) not supported.')

setup(
  name='sling',
  version=version,
  description='Slings data from a source to a target',
  author='Fritz Larco',
  author_email='fritz@slingdata.io',
  url='https://github.com/slingdata-io/sling-python',
  download_url='https://github.com/slingdata-io/sling-python/archive/master.zip',
  keywords=['sling', 'etl', 'elt', 'extract', 'load'],

  # https://setuptools.pypa.io/en/latest/userguide/datafiles.html#subdirectory-for-data-files
  packages=find_packages(exclude=['tests']),
  # package_data={"sling": [os.path.join('bin', binary_name)]},
  long_description_content_type='text/markdown',
  long_description=open(os.path.join(os.path.dirname(__file__), '..', 'README.md')).read(),
  include_package_data=True, # uses MANIFEST.in
  install_requires=install_requires,
  extras_require={},
  entry_points={
    'console_scripts': ['sling=sling:cli',],
  },
  classifiers=[
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: MacOS', 'Operating System :: Unix',
    'Topic :: Utilities'
  ])
