#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/5/28 11:02 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

from gevent import monkey
monkey.patch_all()
import gevent

import os
import sys
import logging
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path)

from clct_huobi import HuobiCollector
from clct_okex import OkexCollector
from clct_upbit import UpbitCollector

from utils.utils import *


def main():
    collectors = [HuobiCollector(),
                  OkexCollector(),
                  UpbitCollector(),
                  ]
    gevent.joinall([gevent.spawn(collector.run) for collector in collectors])

if __name__ == "__main__":
    prepare_run_env()
    LOG_FILE = "{0}/collector.log".format(LOG_DIR)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %Y/%m/%d %H:%M:%S',
                        filename=LOG_FILE,
                        filemode='a')

    logging.info("Collector starting")
    main()