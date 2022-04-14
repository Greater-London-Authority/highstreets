"""Highstreets package: a Python package for
   analysis and modeling of London Highstreet profiles.
   Aims to provide Jupyter notebooks for exploring different models
   and visualisations."""
# -*- coding: utf-8 -*-

__author__ = "Conor Dempsey"
__email__ = "conor.dempsey@london.gov.uk"

try:
    from importlib.metadata import PackageNotFoundError, version  # type: ignore
except ImportError:  # pragma: no cover
    from importlib_metadata import PackageNotFoundError, version  # type: ignore


try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
