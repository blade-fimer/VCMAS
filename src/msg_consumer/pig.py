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
        self._exchanges = ['huobi', 'okex']
        # TODO: get the members from external conf file or arguments
        for key in self._exchanges:
            self._queues.append(Queue(key,
                                      exchange=Exchange(name=key,
                                                        type='direct',
                                                        durable=False,
                                                        auto_delete=True,
                                                        delivery_mode="transient"),
                                      routing_key=key,
                                      durable=False,
                                      auto_delete=True,
                                      max_length=10000))
            self._latest_vals[key] = None

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self._queues, callbacks=[self.chew], accept=['json']),
        ]

    def chew(self, body, message):
        '''
        Dealing with the messages

        :param body(sample):
        {'ts': 1564288299,
        'bids': [['208.52', '2.080817'], ['208.51', '0.927'], ['208.5', '9.237666'], ['208.49', '0.993702'], ['208.48', '2.391']],
        'asks': [['208.56', '0.030535'], ['208.57', '0.136'], ['208.58', '1'], ['208.59', '6.723985'], ['208.6', '0.31285']]}

        :param message:
        https://kombu.readthedocs.io/en/stable/reference/kombu.html#kombu.Queue.exclusive
        <Message object at 0x7f72d158fc18 with details
        {'state': 'RECEIVED', 'content_type': 'application/json', 'delivery_tag': '9e9edf92-90f2-4f9c-84bc-9099243362ca',
        'body_length': 4, 'properties': {}, 'delivery_info': {'exchange': 'okex', 'routing_key': 'okex'}}

        :return:
        '''
        message.ack()
        if not body:
            return
        key = message.properties.get('delivery_info', {}).get('routing_key')
        if key:
            self._latest_key = key
            self._latest_vals[key] = body
            self._op_hunt()

    def _op_hunt(self):
        '''
        Short for opportunity hunt
        :return:
        '''
        new_bid = self._latest_vals[self._latest_key]["bids"]
        new_ask = self._latest_vals[self._latest_key]["asks"]
        nb1_price = Decimal(str(new_bid[0][0]))
        na1_price = Decimal(str(new_ask[0][0]))
        for key in self._latest_vals.keys():
            if key != self._latest_key:
                try:
                    bid = self._latest_vals[key]["bids"]
                    ask = self._latest_vals[key]["asks"]
                    # bids[0][0] is the price, bids[0][1] is the amount
                    b1_price = Decimal(str(bid[0][0]))
                    a1_price = Decimal(str(ask[0][0]))
                    if (nb1_price - a1_price) / nb1_price > threashold or \
                                    (b1_price - na1_price) / b1_price > threashold:
                        logging.debug(" Detail ".center(40, '#'))
                        logging.debug("{0}(new) bid price: {1}, {2}(old) ask price: {3}, "
                                     "sub is {4}, percentage is {5}".format(self._latest_key,
                                                                            nb1_price,
                                                                            key,
                                                                            a1_price,
                                                                            (nb1_price - a1_price),
                                                                            (nb1_price - a1_price)/nb1_price))
                        logging.debug("{0}(old) bid price: {1}, {2}(new) ask price: {3}, "
                                     "sub is {4}, percentage is {5}".format(key,
                                                                            b1_price,
                                                                            self._latest_key,
                                                                            na1_price,
                                                                            (b1_price - na1_price),
                                                                            (b1_price - na1_price)/b1_price))
                        logging.info(json.dumps(self._latest_vals))
                except Exception as e:
                    pass

LOG_FILE = "opportunity.log"
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %Y/%m/%d %H:%M:%S',
                    filename=LOG_FILE,
                    filemode='a')


if __name__ == "__main__":
    p = Pig()
    p.run()