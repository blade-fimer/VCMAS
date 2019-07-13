#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:51 PM
# @Author  : Hao Yuan
# @E-mail  : paul_yuan@sphinx.work

import os
import sys


def add_bin_root_to_sys_path():
    path = os.path.join(os.path.dirname(__file__), '..')
    path = os.path.abspath(path)
    if path not in sys.path:
        sys.path.append(path)
