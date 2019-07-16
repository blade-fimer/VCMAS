#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:51 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

import os
import sys


def add_relative_path_to_sys(p):
    path = os.path.join(os.path.dirname(__file__), p)
    path = os.path.abspath(path)
    if path not in sys.path:
        sys.path.append(path)
