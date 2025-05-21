import json
import os
from io import StringIO
import tempfile
import boto3
import paramiko
from dotenv import find_dotenv, load_dotenv
import time

from highstreets.core.logger import setup_logger

# Load environment variables
load_dotenv(find_dotenv())

logger = setup_logger(__name__)


class SFTPClientException(Exception):
    """Exception raised for SFTP client errors."""
    pass


class SFTPClient:
    """Client for interacting with SFTP servers to download data files."""

    def __init__(self, use_aws_secrets=False, secret_name=None):
        """
        Initialize the SFTP client with credentials.

        Args:
            use_aws_secrets (bool): Whether to use AWS Secrets Manager for credentials
            secret_name (str, optional): The name of the AWS Secrets Manager secret
        """
        self.logger = logger
        self.use_aws_secrets = use_aws_secrets
        self.private_key = None

        if use_aws_secrets and secret_name:
            self._load_credentials_from_aws(secret_name)
        else:
            self._load_credentials_from_env()

    def _load_credentials_from_aws(self, secret_name):
        """Load SFTP credentials from AWS Secrets Manager."""
        try:
            secrets_client = boto3.client('secretsmanager')
            secret_response = secrets_client.get_secret_value(SecretId=secret_name)
            secret = json.loads(secret_response['SecretString'])

            self.host = secret['host']
            self.port = int(secret['port'])
            self.username = secret['username']
            self.remote_dir = secret.get('directory', '.')

            # Handle either private key or password authentication
            if 'private_key' in secret:
                private_key_str = secret['private_key']
                self._load_private_key(private_key_str)
                self.auth_type = 'key'
            else:
                self.password = secret.get('password', '')
                self.auth_type = 'password'

            self.logger.info(f"Loaded SFTP credentials from"
                             f" AWS Secrets Manager: {secret_name}")
        except Exception as e:
            self.logger.error(f"Failed to load SFTP credentials from"
                              f"AWS Secrets Manager: {str(e)}")
            raise SFTPClientException(f"Failed to load SFTP credentials"
                                      f" from AWS Secrets Manager: {str(e)}")

    def _load_credentials_from_env(self):
        """Load SFTP credentials from environment variables."""
        try:
            self.host = os.getenv("SFTP_HOST")
            self.port = int(os.getenv("SFTP_PORT", 22))
            self.username = os.getenv("SFTP_USERNAME")
            self.remote_dir = os.getenv("SFTP_DIRECTORY", ".")

            # Handle either private key or password authentication
            private_key_path = os.getenv("SFTP_PRIVATE_KEY_PATH")
            private_key_str = os.getenv("SFTP_PRIVATE_KEY")

            if private_key_path or private_key_str:
                # Store the private key info but don't attempt to load it
                self.private_key_path = private_key_path if private_key_path else None
                self._private_key_str = private_key_str if private_key_str else None

                # Don't attempt to directly load the key - skip calling _load_private_key
                self.private_key = None  # Will be handled during connection
                self.auth_type = 'key'
            else:
                self.password = os.getenv("SFTP_PASSWORD", "")
                self.auth_type = 'password'

            if not self.host or not self.username or (
                self.auth_type == 'password' and not self.password) or (
                    self.auth_type == 'key' and not (
                        self.private_key_path or self._private_key_str)):
                raise SFTPClientException("Missing required SFTP credentials"
                                          "in environment variables")

            self.logger.info("Loaded SFTP credentials from environment variables")
        except Exception as e:
            self.logger.error(f"Failed to load SFTP credentials from"
                              f" environment variables: {str(e)}")
            raise SFTPClientException(f"Failed to load SFTP credentials from"
                                      f" environment variables: {str(e)}")

    def _load_private_key(self, private_key_str):
        """Load SSH private key from string."""
        try:
            # Store the raw key string for fallback method
            self._private_key_str = private_key_str

            # Get passphrase if specified
            passphrase = os.getenv("SFTP_KEY_PASSPHRASE", "")
            passphrase = passphrase if passphrase else None

            # Try to load the key directly - this works for most key formats
            key_file = StringIO(private_key_str)

            try:
                self.private_key = paramiko.RSAKey.from_private_key(key_file,
                                                                    password=passphrase)
                self.logger.info("Successfully loaded RSA private key")
                return
            except Exception:
                # Reset file pointer for next attempt
                key_file.seek(0)

            # If RSA fails, try DSS key
            try:
                self.private_key = paramiko.DSSKey.from_private_key(key_file,
                                                                    password=passphrase)
                self.logger.info("Successfully loaded DSS private key")
                return
            except Exception:
                # We'll use key_filename approach during connection
                # if direct loading fails
                self.logger.info("Using key_filename approach for authentication")
                self.private_key = None
        except Exception as e:
            self.logger.error(f"Failed to load private key: {str(e)}")
            raise SFTPClientException(f"Failed to load private key: {str(e)}")

    def download_files(self, s3_bucket=None, s3_prefix=None, file_filter=None,
                       local_dir=None):
        """
        Download files from SFTP server to either S3 or local directory.

        Args:
            s3_bucket (str, optional): S3 bucket name for storing files
            s3_prefix (str, optional): S3 key prefix
            file_filter (function, optional): Function to filter files by name
            local_dir (str, optional): Local directory to save files

        Returns:
            list: List of downloaded file paths
        """
        temp_key_file = None
        sftp = None
        transport = None

        try:
            # Connect to SFTP server
            sftp, transport, temp_key_file = self._connect_to_sftp()

            # List and filter files
            files = self._list_remote_files(sftp, file_filter)

            # Process files
            downloaded_files = self._process_files(sftp, files, s3_bucket, s3_prefix,
                                                   local_dir)

            return downloaded_files

        except Exception as e:
            self.logger.error(f"Error during SFTP operation: {str(e)}")
            raise SFTPClientException(f"Error during SFTP operation: {str(e)}")
        finally:
            if sftp:
                sftp.close()
            if transport:
                transport.close()
            if temp_key_file:
                try:
                    os.unlink(temp_key_file.name)
                except Exception:
                    pass

    def _connect_to_sftp(self):
        """Establish connection to SFTP server."""
        temp_key_file = None

        if self.auth_type == 'key':
            # Create SSH client
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Get key path
            key_path, temp_key_file = self._prepare_key_file()

            # Connect to server
            ssh = self._connect_with_key(ssh, key_path)

            # Get SFTP client
            transport = ssh.get_transport()
            sftp = paramiko.SFTPClient.from_transport(transport)
        else:
            # Password authentication
            transport = paramiko.Transport((self.host, self.port))
            transport.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)

        # Change to remote directory
        if self.remote_dir and self.remote_dir != ".":
            sftp.chdir(self.remote_dir)
            self.logger.info(f"Changed to directory: {self.remote_dir}")

        return sftp, transport, temp_key_file

    def _prepare_key_file(self):
        """Prepare key file for authentication."""
        temp_key_file = None

        if hasattr(self, '_private_key_str') and self._private_key_str:
            # Create temporary file with key content
            temp_key_file = tempfile.NamedTemporaryFile(mode='w+', delete=False)
            temp_key_file.write(self._private_key_str)
            temp_key_file.close()
            key_path = temp_key_file.name
        elif hasattr(self, 'private_key_path') and self.private_key_path:
            # Use the provided key path
            key_path = self.private_key_path
        else:
            raise SFTPClientException("No private key string or path provided")

        # Try to set permissions
        try:
            os.chmod(key_path, 0o600)
        except Exception:
            pass

        return key_path, temp_key_file

    def _connect_with_key(self, ssh, key_path):
        """Connect to SSH server using key authentication."""
        # Create connection parameters
        connect_kwargs = {
            'hostname': self.host,
            'port': self.port,
            'username': self.username,
            'look_for_keys': False,
            'allow_agent': False,
            'disabled_algorithms': dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"])
        }

        # Handle key type
        if key_path.lower().endswith('.ppk'):
            self.logger.info(f"Using PuTTY key format: {key_path}")
            connect_kwargs['key_filename'] = key_path
        else:
            try:
                key = paramiko.RSAKey.from_private_key_file(key_path)
                self.logger.info("Loaded RSA key explicitly")
                connect_kwargs['pkey'] = key
            except Exception:
                self.logger.info("Falling back to key_filename method")
                connect_kwargs['key_filename'] = key_path

        # Try to connect
        try:
            self.logger.info(f"Connecting to SFTP server"
                             f" {self.host}:{self.port} as {self.username}")
            ssh.connect(**connect_kwargs)
        except Exception as e:
            self.logger.warning(f"Initial connection failed: {e}")

            # Fall back to trying without algorithm disabling
            if "disabled_algorithms" in connect_kwargs:
                self.logger.info("Trying without algorithm restrictions")
                connect_kwargs.pop('disabled_algorithms')
                ssh.connect(**connect_kwargs)

        return ssh

    def _list_remote_files(self, sftp, file_filter=None):
        """List and filter files in remote directory."""
        files = sftp.listdir()
        self.logger.info(f"Found {len(files)} files in SFTP directory")

        if file_filter:
            filtered_files = [f for f in files if file_filter(f)]
            self.logger.info(f"After filtering, {len(filtered_files)}"
                             f" files will be processed")
            return filtered_files

        return files

    def _process_files(self, sftp, files, s3_bucket=None,
                       s3_prefix=None, local_dir=None):
        """Process each file from SFTP server using optimized transfer techniques."""
        downloaded_files = []

        # Set up S3 client if needed
        s3 = boto3.client('s3') if s3_bucket else None

        # Create local directory if needed
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir)
            self.logger.info(f"Created local directory: {local_dir}")

        # Process each file
        for file in files:
            self.logger.info(f"Processing file: {file}")
            start_time = time.time()
            file_size = sftp.stat(file).st_size
            file_ext = os.path.splitext(file)[1].lower()

            try:
                # Optimize based on file type and size
                if local_dir:
                    local_path = os.path.join(local_dir, file)

                    # For small files (<10MB), use direct get() which is more efficient
                    if file_size < 10 * 1024 * 1024:
                        self.logger.info(f"Using direct transfer for small file: {file}")
                        sftp.get(file, local_path)
                        downloaded_files.append(local_path)

                    # For larger files, use optimized buffer sizes based on file type
                    else:
                        # Determine optimal buffer size
                        if file_ext in ['.zip', '.xlsx']:
                            buffer_size = 262144  # 256KB for binary files
                        elif file_ext in ['.csv']:
                            buffer_size = 131072  # 128KB for CSV files
                        else:
                            buffer_size = 65536   # 64KB default

                        self.logger.info(f"Using buffered transfer with"
                                         f" {buffer_size/1024}KB chunks for {file}")

                        with sftp.open(file, 'rb') as remote_file, open(local_path, 'wb') as local_file:  # noqa
                            # Pre-allocate local file to full size for better performance
                            try:
                                os.posix_fallocate(local_file.fileno(), 0, file_size)
                            except (AttributeError, OSError):
                                # Not all systems support posix_fallocate, ignore
                                # if unavailable
                                pass

                            # Transfer in chunks
                            total_transferred = 0
                            buffer = remote_file.read(buffer_size)
                            while buffer:
                                local_file.write(buffer)
                                total_transferred += len(buffer)
                                buffer = remote_file.read(buffer_size)

                                # Optional: Log progress for very large files
                                if file_size > 100 * 1024 * 1024 and total_transferred % (10 * 1024 * 1024) < buffer_size:  # noqa
                                    percent = (total_transferred / file_size) * 100
                                    self.logger.info(f"Transfer progress: {percent:.1f}%"
                                                     f" ({total_transferred/(1024*1024):.1f}MB)")  # noqa

                        downloaded_files.append(local_path)

                # S3 uploads
                elif s3_bucket and s3_prefix:
                    s3_key = f"{s3_prefix.rstrip('/')}/{file}"

                    # For S3, we can use a memory file object for small files
                    # or streaming transfer for larger files
                    with sftp.open(file, 'rb') as remote_file:
                        s3.upload_fileobj(remote_file, s3_bucket, s3_key)

                    downloaded_files.append(f"s3://{s3_bucket}/{s3_key}")

                # No destination specified
                else:
                    self.logger.warning(f"No destination specified for {file}, skipping")

                # Log transfer statistics
                elapsed = time.time() - start_time
                transfer_rate = file_size / (elapsed * 1024 * 1024) if elapsed > 0 else 0
                self.logger.info(
                    f"Downloaded {file} ({file_size/1024/1024:.2f} MB) "
                    f"in {elapsed:.2f} seconds ({transfer_rate:.2f} MB/s)"
                )

            except Exception as e:
                self.logger.error(f"Error downloading {file}: {str(e)}")
                import traceback
                traceback.print_exc()

        return downloaded_files
