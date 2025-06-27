import os, platform, pathlib
from setuptools import setup
from setuptools import find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop


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

# No platform-specific dependencies needed - binaries are downloaded from GitHub
install_requires = []

def download_sling_binary():
    """Download the sling binary during installation"""
    try:
        # Import our download logic
        import sys
        
        # Add current directory to path to import our modules
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sys.path.insert(0, current_dir)
        
        # Import and run the download function
        from sling.bin import download_binary
        
        # Map version format if needed (e.g., 0.0.0dev -> latest)
        version = SLING_VERSION
        if 'dev' in version or version == '0.0.0':
            version = 'latest'
        
        download_binary(version)
        
    except Exception as e:
        # Don't fail the installation if binary download fails
        # It will be attempted again on first use
        print(f"⚠️  Sling Binary download during installation failed: {e}")
        print(f"    Binary will be downloaded on first use instead.")

class PostInstallCommand(install):
    """Custom installation command that downloads binary after install"""
    def run(self):
        install.run(self)
        self.execute(download_sling_binary, [], msg="Downloading sling binary...")

class PostDevelopCommand(develop):
    """Custom development command that downloads binary after develop install"""  
    def run(self):
        develop.run(self)
        self.execute(download_sling_binary, [], msg="Downloading sling binary...")

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
  extras_require={
    'arrow': ['pyarrow'],
    'recommended': ['pyarrow'],
  },
  entry_points={
    'console_scripts': ['sling=sling:cli',],
  },
  cmdclass={
    'install': PostInstallCommand,
    'develop': PostDevelopCommand,
  },
  classifiers=[
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: MacOS', 'Operating System :: Unix',
    'Topic :: Utilities'
  ])
