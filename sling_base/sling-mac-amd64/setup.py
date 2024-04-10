import os, pathlib
from setuptools import setup
from setuptools import find_packages
from sling_mac_amd64 import SLING_VERSION

README = 'dev'
readme_path = pathlib.Path(os.path.join(os.path.dirname(__file__), 'README.md'))
if readme_path.exists():
  with readme_path.open() as file:
    README = file.read()

setup(
  name='sling-mac-amd64',
  version=SLING_VERSION,
  description='Sling Binary for Mac (AMD64)',
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
  install_requires=[],
  extras_require={},
  entry_points={},
  classifiers=[
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: MacOS', 'Operating System :: Unix',
    'Topic :: Utilities'
  ])
