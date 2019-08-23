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
from kombu.common import maybe_declare

class MessagingMixin(object):
    ID_HOST = ['huobi', 'okex']
    HOST_ID = {
        'huobi': 0,
        'okex': 1,
    }

    def __init__(self, keys=[]):
        self._keys = keys
        self._conn = connections[Connection('redis://127.0.0.1:6379')].acquire()
        self._exchanges = []
        self._queue = []
        for key_name in keys:
            ex = Exchange(name=key_name,
                          type='direct',
                          durable=False,
                          # auto_delete=False,
                          delivery_mode="transient")

            self._exchanges.append(ex)

            qu = Queue(key_name,
                       exchange=key_name,
                       routing_key=key_name,
                       durable=False,
                       # auto_delete=True,
                       max_length=10000)
            self._queue.append(qu)

        self._p = Producer(self._conn,
                           auto_declare=False)

    def _send(self, msg):
        key_name = msg["ch"]

        self._p.publish(
            msg,
            exchange=key_name,
            routing_key=key_name,
            serializer='json',
            # compression='zlib',
        )
        return
