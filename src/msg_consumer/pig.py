#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:34 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com


import os
import sys
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path)

from kombu import Connection, Consumer, Queue, Exchange
from kombu.mixins import ConsumerMixin
from kombu.pools import connections


class Pig(ConsumerMixin):
    def __init__(self):
        self.connection = connections[Connection('redis://127.0.0.1:6379')].acquire()
        self._queues = []
        # TODO: get the members from external conf file or arguments
        for key in ['huobi', 'okex']:
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

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self._queues, callbacks=[self.chew], accept=['json']),
        ]

    def chew(self, body, message):
        # Dealing with the messages
        print(body)
        print(message)
        pass


if __name__ == "__main__":
    p = Pig()
    p.run()