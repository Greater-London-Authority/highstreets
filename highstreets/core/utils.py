"""General purpose utility functions for the highstreets package."""

import os
import re
import fsspec
from typing import List, Optional


def list_files(path: str, pattern: Optional[str] = None) -> List[str]:
    """
    List files from either local or S3 path with optional pattern filtering.

    Args:
        path: Directory path (local or S3)
        pattern: Optional regex pattern to filter files

    Returns:
        List of file paths matching the criteria
    """
    if path.startswith('s3://'):
        fs = fsspec.filesystem('s3')
        files = fs.glob(f"{path}*")
    else:
        files = [os.path.join(path, f) for f in os.listdir(path)]

    # Filter by pattern if needed
    if pattern:
        regex = re.compile(pattern)
        files = [f for f in files if regex.match(os.path.basename(f))]

    return files


def path_exists(path: str) -> bool:
    """
    Check if a path exists (works for both local and S3 paths).

    Args:
        path: Path to check

    Returns:
        True if the path exists, False otherwise
    """
    if path.startswith('s3://'):
        fs = fsspec.filesystem('s3')
        return fs.exists(path)
    else:
        return os.path.exists(path)


def ensure_directory_exists(path: str) -> None:
    """
    Ensures that a directory exists, creating it if necessary.
    Only applicable for local paths.

    Args:
        path: Directory path to ensure exists
    """
    if not path.startswith('s3://'):
        os.makedirs(path, exist_ok=True)
