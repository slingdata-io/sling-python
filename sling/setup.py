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
        # Import required modules directly here to avoid import issues
        import urllib.request
        import tarfile
        import zipfile
        import tempfile
        import stat
        import shutil
        from pathlib import Path
        
        # Get platform info
        system = platform.system()
        machine = platform.machine()
        
        if system == 'Linux':
            if machine == 'aarch64':
                archive_name, binary_name = 'sling_linux_arm64.tar.gz', 'sling'
            else:
                archive_name, binary_name = 'sling_linux_amd64.tar.gz', 'sling'
        elif system == 'Windows':
            if machine == 'ARM64':
                archive_name, binary_name = 'sling_windows_amd64.tar.gz', 'sling.exe'
            else:
                archive_name, binary_name = 'sling_windows_amd64.tar.gz', 'sling.exe'
        elif system == 'Darwin':
            if machine == 'arm64':
                archive_name, binary_name = 'sling_darwin_arm64.tar.gz', 'sling'
            else:
                archive_name, binary_name = 'sling_darwin_amd64.tar.gz', 'sling'
        else:
            raise RuntimeError(f"Unsupported platform: {system} {machine}")
        
        # Map version format
        version = SLING_VERSION
        if 'dev' in version or version == '0.0.0':
            version = 'latest'
        elif not version.startswith('v'):
            version = 'v' + version
        
        # Create cache directory
        cache_dir = Path.home() / '.sling' / 'bin' / 'sling' / version
        cache_dir.mkdir(parents=True, exist_ok=True)
        binary_path = cache_dir / binary_name
        
        # Skip if already exists
        if binary_path.exists():
            return
        
        # Download URL
        if version == 'latest':
            github_url = f"https://github.com/slingdata-io/sling-cli/releases/latest/download/{archive_name}"
        else:
            github_url = f"https://github.com/slingdata-io/sling-cli/releases/download/{version}/{archive_name}"
        
        print(f"⬇ Downloading sling binary ({version}) for {system}/{machine}...")
        
        # Download to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tar.gz') as tmp_file:
            # Handle SSL issues
            import ssl
            context = ssl.create_default_context()
            try:
                with urllib.request.urlopen(github_url, context=context) as response:
                    # Download with progress
                    total_size = int(response.headers.get('Content-Length', 0))
                    downloaded = 0
                    block_size = 8192
                    
                    while True:
                        data = response.read(block_size)
                        if not data:
                            break
                        tmp_file.write(data)
                        downloaded += len(data)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"\r  Progress: {percent:.1f}%", end='', flush=True)
                    print()  # New line after progress
            except ssl.SSLError:
                # Fallback for certificate issues
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                with urllib.request.urlopen(github_url, context=context) as response:
                    shutil.copyfileobj(response, tmp_file)
            tmp_file_path = tmp_file.name
        
        # Extract binary
        try:
            with tarfile.open(tmp_file_path, 'r:gz') as tar:
                for member in tar.getmembers():
                    if member.name == binary_name or member.name.endswith(f'/{binary_name}'):
                        member.name = binary_name
                        tar.extract(member, cache_dir)
                        break
                else:
                    raise RuntimeError(f"Binary {binary_name} not found in archive")
        finally:
            os.unlink(tmp_file_path)
        
        # Make executable
        binary_path.chmod(binary_path.stat().st_mode | stat.S_IEXEC)
        print(f"✓ Sling binary downloaded successfully to {binary_path}")
        
    except Exception as e:
        # Don't fail the installation if binary download fails
        print(f"⚠️  Sling binary download during installation failed: {e}")
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
  author='Sling Data',
  author_email='support@slingdata.io',
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
