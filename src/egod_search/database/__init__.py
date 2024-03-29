# -*- coding: UTF-8 -*-
from sys import modules

if "unittest" in modules:
    from .test_output import *
    from .test_scheme import *
