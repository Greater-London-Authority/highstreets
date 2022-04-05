"""highstreets package"""
# -*- coding: utf-8 -*-

__author__ = 'Conor Dempsey'
__email__ = 'conor.dempsey@london.gov.uk'

try:
    from importlib.metadata import version, PackageNotFoundError  # type: ignore
except ImportError:  # pragma: no cover
    from importlib_metadata import version, PackageNotFoundError  # type: ignore


try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
