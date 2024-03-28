# -*- coding: UTF-8 -*-
from sys import modules


NAME = "egod-search"  # synchronize with `pyproject.toml`
"""
Package name.
"""
PACKAGE_NAME = NAME.replace("-", "_")
"""
Package directory name.
"""
VERSION = "1.0.0"
"""
Package version.
"""

if "unittest" in modules:
    from .crawl import *
    from .database import *
    from .index import *
    from .test__util import *
