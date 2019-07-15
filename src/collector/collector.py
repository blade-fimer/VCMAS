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

LOG_FILE = "collector.log"
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %Y/%m/%d %H:%M:%S',
                    filename=LOG_FILE,
                    filemode='a')


def main():
    collectors = [HuobiCollector(), OkexCollector()]
    gevent.joinall([gevent.spawn(collector.run) for collector in collectors])

if __name__ == "__main__":
    logging.info("Collector starting")
    main()