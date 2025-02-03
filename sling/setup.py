import os, platform, pathlib
from setuptools import setup
from setuptools import find_packages


SLING_VERSION = '0.0.0dev'

version_path = pathlib.Path(os.path.join(os.path.dirname(__file__), 'VERSION'))
if version_path.exists():
  with version_path.open() as file:
    SLING_VERSION = file.read().strip()

README = 'dev'
readme_path = pathlib.Path(os.path.join(os.path.dirname(__file__), 'README.md'))
if readme_path.exists():
  with readme_path.open() as file:
    README = file.read()

install_requires = []
if platform.system() == 'Linux':
  if platform.machine() == 'aarch64':
    install_requires = [f'sling-linux-arm64=={SLING_VERSION}']
  else:
    install_requires = [f'sling-linux-amd64=={SLING_VERSION}']
elif platform.system() == 'Windows':
  if platform.machine() == 'ARM64':
    install_requires = [f'sling-windows-arm64=={SLING_VERSION}']
  else:
    install_requires = [f'sling-windows-amd64=={SLING_VERSION}']
elif platform.system() == 'Darwin':
  if platform.machine() == 'arm64':
    install_requires = [f'sling-mac-arm64=={SLING_VERSION}']
  else:
    install_requires = [f'sling-mac-amd64=={SLING_VERSION}']
else:
  raise Exception(f'platform "{platform.system()}" ({platform.system()}) not supported.')

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
  long_description=README,
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
