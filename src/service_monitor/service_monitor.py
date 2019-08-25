#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/8/25 8:17 AM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

import os
import sys
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path)
import time
import logging

from utils.utils import *


def restart_collectors():
    exe("ps -ef | grep collector.py | grep python3 | head -1 | awk '{print $2}' | xargs kill -9", False, False)
    exe("python3 /root/projects/VCMAS/src/collector/collector.py > /dev/null 2>&1 &", False, False)


if __name__ == "__main__":
    prepare_run_env()
    LOG_FILE = "{0}/monitor.log".format(LOG_DIR)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %Y/%m/%d %H:%M:%S',
                        filename=LOG_FILE,
                        filemode='a')
    while True:
        if os.path.isfile(MON_FILE):
            restart_collectors()
            os.remove(MON_FILE)
        time.sleep(3)