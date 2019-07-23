#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 3:34 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com


from kombu import Consumer, Queue
from kombu.mixins import ConsumerMixin

from msg_queue import MessagingMixin


class Pig(ConsumerMixin):
    def __init__(self):
        self._queue = Queue('', exchange=MessagingMixin.exchanges)

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self._queue, callbacks=[self.chew], accept=['json']),
        ]

    def chew(self, body, message):
        # Dealing with the messages
        print(body)
        print(message)
        pass