#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:39 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

from gevent import monkey
monkey.patch_all()
import gevent
import redis
from kombu import Connection, Exchange, Producer, Queue
from kombu.pools import connections

class MessagingMixin(object):

    def __init__(self, key_name=''):
        self._key = key_name
        self._conn = connections[Connection('redis://127.0.0.1:6379')].acquire()
        self._exchange = Exchange(name=key_name,
                                  type='direct',
                                  durable=False,
                                  auto_delete=True,
                                  delivery_mode="transient")
        self._queue = Queue(key_name,
                      exchange=self._exchange,
                      routing_key=key_name,
                      durable=False,
                      auto_delete=True,
                      max_length=10000)
        self._p = Producer(self._conn)

    def _send(self, msg):
        self._p.publish(
            msg,
            exchange=self._exchange,
            routing_key=self._key,
            serializer='json',
            # compression='zlib',
        )
        return

    def get_ex(self):
        return MessagingMixin.exchanges[self._key]