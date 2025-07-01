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

# import os
# import subprocess
# import sys
# import warnings


# def _install_glapy_if_needed():
#     """
#     Automatically install glapy if GITHUB_TOKEN is available and glapy is not installed
#     """
#     try:
#         import glapy  # noqa: F401
#         return  # Already installed
#     except ImportError:
#         pass

#     github_token = os.getenv('GITHUB_TOKEN')
#     if not github_token:
#         warnings.warn(
#             "glapy dependency not found. To install it, set GITHUB_TOKEN "
#             "environment variable and run: poetry run install-glapy",
#             UserWarning
#         )
#         return

#     print("Installing glapy using GITHUB_TOKEN...")
#     glapy_url = (
#         f"git+https://{github_token}@github.com/"
#         f"Greater-London-Authority/glapy@feature/lds-update-data"
#     )

#     try:
#         subprocess.run([
#             sys.executable, "-m", "pip", "install", glapy_url
#         ], check=True, capture_output=True)
#         print("glapy installed successfully!")
#     except subprocess.CalledProcessError:
#         warnings.warn(
#             "Failed to auto-install glapy. Please run: poetry run install-glapy",
#             UserWarning
#         )


# # Auto-install glapy on package import if available
# _install_glapy_if_needed()
