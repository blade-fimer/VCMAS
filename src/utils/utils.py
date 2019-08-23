#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:51 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

import os
import sys
import logging
import subprocess


def add_relative_path_to_sys(p):
    path = os.path.join(os.path.dirname(__file__), p)
    path = os.path.abspath(path)
    if path not in sys.path:
        sys.path.append(path)

def exe(cmd_line, error_on_exit=True, excep_on_exit=True):
    try:
        logging.debug('Execute: ' + cmd_line[:150])
        ret = subprocess.call(cmd_line, shell=True)
        if ret != 0 and error_on_exit:
            raise Exception('!!! The return code is not 0, but: ' + str(ret))
        return ret
    except Exception as e:
        logging.exception('Got exception while execute: ' + cmd_line)
        if excep_on_exit:
            raise e