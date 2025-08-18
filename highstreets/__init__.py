"""Highstreets package: a Python package for ETL Pipeline for Highstreets"""
# -*- coding: utf-8 -*-

__author__ = "Anupam Bose"
__email__ = "anupam.bose@london.gov.uk"

try:
    from importlib.metadata import PackageNotFoundError, version  # type: ignore
except ImportError:  # pragma: no cover
    from importlib_metadata import PackageNotFoundError, version  # type: ignore

try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

import os
import subprocess
import sys
import warnings


def _install_glapy_if_needed():
    """Auto-install glapy if GITHUB_TOKEN is available and glapy is not installed"""
    try:
        import glapy  # noqa: F401
        return  # Already installed
    except ImportError:
        pass

    # Load .env if available
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    github_token = os.getenv('GITHUB_ACCESS_TOKEN_GLAPY')
    if not github_token:
        warnings.warn(
            "glapy dependency not found. To install it, set GITHUB_TOKEN "
            "environment variable and run: pip install git+https://token@github.com/...",
            UserWarning,
            stacklevel=2
        )
        return

    print("🔐 Auto-installing glapy using GITHUB_TOKEN...")
    glapy_url = (
        f"git+https://{github_token}@github.com/Greater-London-Authority/"
        f"glapy@feature/lds-update-data"
    )

    try:
        subprocess.run([
            sys.executable, "-m", "pip", "install", glapy_url
        ], check=True, capture_output=True)
        print("✅ glapy installed successfully!")
    except subprocess.CalledProcessError:
        warnings.warn(
            ("Failed to auto-install glapy."
             " Please install manually with your GITHUB_TOKEN."),
            UserWarning,
            stacklevel=2
        )


# Auto-install glapy on package import if available
_install_glapy_if_needed()
