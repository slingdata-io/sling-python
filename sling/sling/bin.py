import os, sys, platform, urllib.request, tarfile, zipfile, tempfile, stat, warnings
from pathlib import Path

#################################################################
# Logic to download and cache the proper binary for the respective 
# operating systems and architecture. Binaries are downloaded from
# GitHub releases on first use and cached locally.

def get_platform_info():
    """Get platform and architecture info for binary selection"""
    system = platform.system()
    machine = platform.machine()
    
    if system == 'Linux':
        if machine == 'aarch64':
            return 'linux', 'arm64', 'sling_linux_arm64.tar.gz', 'sling'
        else:
            return 'linux', 'amd64', 'sling_linux_amd64.tar.gz', 'sling'
    elif system == 'Windows':
        if machine == 'ARM64':
            # return 'windows', 'arm64', 'sling_windows_arm64.tar.gz', 'sling.exe' # there is no arm64 bin
            return 'windows', 'arm64', 'sling_windows_amd64.tar.gz', 'sling.exe'
        else:
            return 'windows', 'amd64', 'sling_windows_amd64.tar.gz', 'sling.exe'
    elif system == 'Darwin':
        if machine == 'arm64':
            return 'darwin', 'arm64', 'sling_darwin_arm64.tar.gz', 'sling'
        else:
            return 'darwin', 'amd64', 'sling_darwin_amd64.tar.gz', 'sling'
    else:
        raise RuntimeError(f"Unsupported platform: {system} {machine}")

def get_binary_cache_dir(version: str):
    """Get the directory where binaries are cached"""
    cache_dir = Path.home() / '.sling' / 'bin' /  'sling' / version
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir

def download_binary(version: str):
    """Download the sling binary for the current platform"""
    system, arch, archive_name, binary_name = get_platform_info()
    
    # Ensure version has 'v' prefix for non-latest versions
    if version != 'latest' and not version.startswith('v'):
        version = 'v' + version
    
    cache_dir = get_binary_cache_dir(version)
    binary_path = cache_dir / binary_name
    
    # If binary already exists, return it (may have been downloaded during pip install)
    if binary_path.exists():
        return str(binary_path)
    
    # Download from GitHub releases
    if version == 'latest':
        github_url = f"https://github.com/slingdata-io/sling-cli/releases/latest/download/{archive_name}"
    else:
        github_url = f"https://github.com/slingdata-io/sling-cli/releases/download/{version}/{archive_name}"
    
    try:
        print(f"Downloading sling binary ({version}) for {system}/{arch}...")
        
        # Download to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tar.gz') as tmp_file:
            # Handle SSL issues in some environments
            import ssl
            context = ssl.create_default_context()
            try:
                with urllib.request.urlopen(github_url, context=context) as response:
                    tmp_file.write(response.read())
            except ssl.SSLError:
                # Fallback for environments with certificate issues
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                with urllib.request.urlopen(github_url, context=context) as response:
                    tmp_file.write(response.read())
            tmp_file_path = tmp_file.name
        
        # Extract binary from archive
        try:
            if archive_name.endswith('.tar.gz'):
                with tarfile.open(tmp_file_path, 'r:gz') as tar:
                    # Find the binary in the archive
                    for member in tar.getmembers():
                        if member.name == binary_name or member.name.endswith(f'/{binary_name}'):
                            # Extract to cache directory
                            member.name = binary_name  # Rename to just the binary name
                            tar.extract(member, cache_dir)
                            break
                    else:
                        raise RuntimeError(f"Binary {binary_name} not found in archive")
            elif archive_name.endswith('.zip'):
                with zipfile.ZipFile(tmp_file_path, 'r') as zip_file:
                    # Find the binary in the archive
                    for name in zip_file.namelist():
                        if name == binary_name or name.endswith(f'/{binary_name}'):
                            # Extract to cache directory
                            with zip_file.open(name) as source:
                                with open(binary_path, 'wb') as target:
                                    target.write(source.read())
                            break
                    else:
                        raise RuntimeError(f"Binary {binary_name} not found in archive")
        finally:
            # Clean up temporary file
            os.unlink(tmp_file_path)
        
        # Make binary executable
        if binary_path.exists():
            binary_path.chmod(binary_path.stat().st_mode | stat.S_IEXEC)
            return str(binary_path)
        else:
            raise RuntimeError("Failed to extract binary from archive")
            
    except Exception as e:
        warnings.warn(f"Failed to download sling binary: {e}")
        raise RuntimeError(f"Could not download sling binary for {system}/{arch}: {e}")

def get_sling_version():
    """Get the sling version from package metadata or environment"""
    # First check environment variable
    version = os.getenv("SLING_VERSION")
    if version:
        return version
    
    # Try to get from package metadata
    try:
        from importlib.metadata import version as pkg_version
        version = pkg_version('sling')
        # Map dev versions to latest
        if 'dev' in version or version == '0.0.0':
            return 'latest'
        # Strip post suffix (e.g., 1.4.10.post2 -> 1.4.10)
        if '.post' in version:
            version = version.split('.post')[0]
        return version
    except Exception:
        pass
    
    # Try to read from VERSION file
    try:
        version_file = Path(__file__).parent.parent / 'VERSION'
        if version_file.exists():
            version = version_file.read_text().strip()
            # Map dev versions to latest
            if 'dev' in version or version == '0.0.0':
                return 'latest'
            # Strip post suffix
            if '.post' in version:
                version = version.split('.post')[0]
            return version
    except Exception:
        pass
    
    # Default to latest
    return 'latest'

# Get binary path - either from environment variable or download
SLING_BIN = os.getenv("SLING_BINARY")

if not SLING_BIN:
    try:
        version = get_sling_version()
        SLING_BIN = download_binary(version)
    except Exception as e:
        # Fallback: try to find binary in PATH
        import shutil
        SLING_BIN = shutil.which('sling')
        if not SLING_BIN:
            raise RuntimeError(f"Could not locate or download sling binary: {e}")

