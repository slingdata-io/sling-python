import os, platform, pathlib
from setuptools import setup
from setuptools import find_packages

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

SLING_VERSION = 'dev'

version_path = pathlib.Path(os.path.join(os.path.dirname(__file__), 'VERSION'))
if version_path.exists():
  with version_path.open() as file:
    SLING_VERSION = file.read()

setup(
  name='sling',
  version=SLING_VERSION,
  description='Slings data from a source to a target',
  author='Fritz Larco',
  author_email='fritz@slingdata.io',
  url='https://github.com/slingdata-io/sling-python',
  download_url='https://github.com/slingdata-io/sling-python/archive/master.zip',
  keywords=['sling', 'etl', 'elt', 'extract', 'load'],

  # https://setuptools.pypa.io/en/latest/userguide/datafiles.html#subdirectory-for-data-files
  packages=find_packages(exclude=['tests']),
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
