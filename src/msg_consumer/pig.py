#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:34 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com


import os
import sys
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path)

import json
import logging

from kombu import Connection, Consumer, Queue, Exchange
from kombu.mixins import ConsumerMixin
from kombu.pools import connections

from decimal import *

from utils.msg_queue import MessagingMixin
from utils.utils import *

'''
BTC trade fee is "0.00015", others are "0.0005"
So the possible fee is 0.0002 + 0.0015 + 0.0005 = 0.0022,
or 0.0003 + 0.001 + 0.0005 = 0.0018,

or for BTC, it's:
0.0002 + 0.0015 + 0.00015 = 0.00185
or 0.0003 + 0.001 + 0.00015 = 0.00145
'''
FEE = {
    "huobi": {
        "maker": "0.0002",
        "taker": "0.0003",
        "trade": "0.0005",
    },
    "okex": {
        "maker": "0.001",
        "taker": "0.0015",
        "trade": "0.0005",
    },
}

threashold = 0.002

class Pig(ConsumerMixin):
    def __init__(self):
        self.connection = connections[Connection('redis://127.0.0.1:6379')].acquire()
        self._queues = []
        self._latest_key = None
        self._latest_vals = {}
        self._exchanges = ["ethusdt", "btcusdt"]
        # TODO: get the members from external conf file or arguments
        for key in self._exchanges:
            self._queues.append(Queue(key,
                                      exchange=Exchange(name=key,
                                                        type='direct',
                                                        durable=False,
                                                        # auto_delete=False,
                                                        delivery_mode="transient"),
                                      routing_key=key,
                                      durable=False,
                                      # auto_delete=True,
                                      max_length=10000))
            self._latest_vals[key] = ['', '', '']

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self._queues, callbacks=[self.chew], accept=['json']),
        ]

    def chew(self, body, message):
        '''
        Dealing with the messages

        :param body(sample):
        {
        "ch": "market.ethusdt.depth.step0",
        "host": 0,
        'ts': 1564288299,
        'bids': [['208.52', '2.080817'], ['208.51', '0.927'], ['208.5', '9.237666'], ['208.49', '0.993702'], ['208.48', '2.391']],
        'asks': [['208.56', '0.030535'], ['208.57', '0.136'], ['208.58', '1'], ['208.59', '6.723985'], ['208.6', '0.31285']]}

        :param message:
        https://kombu.readthedocs.io/en/stable/reference/kombu.html#kombu.Queue.exclusive
        <Message object at 0x7f72d158fc18 with details
        {'state': 'RECEIVED', 'content_type': 'application/json', 'delivery_tag': '9e9edf92-90f2-4f9c-84bc-9099243362ca',
        'body_length': 4, 'properties': {}, 'delivery_info': {'exchange': 'ethusdt', 'routing_key': 'ethusdt'}}

        :return:
        '''
        message.ack()
        if not body:
            return
        key = message.properties.get('delivery_info', {}).get('routing_key')
        host = body["host"]
        if key:
            # self._latest_key = key
            self._latest_vals[key][host] = body

            '''
            Example:
                key - "ethusdt"
                host - 0 (huobi)
            '''
            self._op_hunt(key, host)

    def _op_hunt(self, key, host):
        '''
        Short for opportunity hunt
        :return:
        '''
        new_ts = self._latest_vals[key][host]["ts"]
        new_bid = self._latest_vals[key][host]["bids"]
        new_ask = self._latest_vals[key][host]["asks"]
        nb1_price = Decimal(str(new_bid[0][0]))
        na1_price = Decimal(str(new_ask[0][0]))
        for index, val in enumerate(self._latest_vals[key]):
            if index != host and val != '':
                try:
                    ts = val["ts"]
                    if index != MessagingMixin.HOST_ID["upbit"] and \
                                    host != MessagingMixin.HOST_ID["upbit"] and \
                                    abs(new_ts - ts) > 2:
                        if not os.path.isfile(MON_FILE):
                            fp = open(MON_FILE, 'w+')
                            fp.close()
                        return

                        # exe("ps -ef | grep collector.py | grep python3 | head -1 | awk '{print $2}' | xargs kill -9")
                        # exe("python3 /root/projects/VCMAS/src/collector/collector.py > /dev/null 2>&1 &")
                        # for key in self._exchanges:
                        #     self._latest_vals[key] = ['', '']
                    bid = val["bids"]
                    ask = val["asks"]
                    # bids[0][0] is the price, bids[0][1] is the amount
                    b1_price = Decimal(str(bid[0][0]))
                    a1_price = Decimal(str(ask[0][0]))

                    '''
                    bid - buy
                    ask - sell
                    Only if the buy price is higher than sell price, there is chance, not the other way.
                    '''
                    nb1_a1_sub_per = (nb1_price - a1_price) / nb1_price
                    b1_na1_sub_per = (b1_price - na1_price) / b1_price
                    if nb1_a1_sub_per > threashold or b1_na1_sub_per > threashold:
                        logging.debug("{0}\n"
                                      "Channel: {1}\n"
                                      "New Host: {2}\n"
                                      "Old Host: {3}".format(" Detail ".center(40, '#'),
                                                             key,
                                                             MessagingMixin.ID_HOST[host],
                                                             MessagingMixin.ID_HOST[index],
                                                             ))
                        if nb1_a1_sub_per > threashold:
                            logging.debug("\n\tNew bid price: {0}\n\t"
                                          "Old ask price: {1}\n\t"
                                         "Sub is {2}\n\t"
                                          "Percentage is {3}".format(nb1_price,
                                                                     a1_price,
                                                                     (nb1_price - a1_price),
                                                                     nb1_a1_sub_per))
                        if b1_na1_sub_per > threashold:
                            logging.debug("\n\tOld bid price: {0}\n\t"
                                          "New ask price: {1}\n\t"
                                         "Sub is {2}\n\t"
                                          "Percentage is {3}".format(b1_price,
                                                                     na1_price,
                                                                     (b1_price - na1_price),
                                                                     b1_na1_sub_per))
                        logging.info(json.dumps(self._latest_vals[key]))
                except Exception as e:
                    logging.error("{}".format(e))
                    pass


if __name__ == "__main__":
    prepare_run_env()
    LOG_FILE = "{0}/opportunity.log".format(LOG_DIR)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %Y/%m/%d %H:%M:%S',
                        filename=LOG_FILE,
                        filemode='a')
    p = Pig()
    p.run()