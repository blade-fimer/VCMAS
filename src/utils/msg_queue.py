#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:39 PM
# @Author  : Hao Yuan
# @E-mail  : yuanhao12@gmail.com

from gevent import monkey
monkey.patch_all()
import gevent
import redis
from kombu import Connection, Exchange, Producer
from kombu.pools import connections

class MessagingMixin(object):
    exchanges = []

    def __init__(self, key_name=''):
        # self._rpool = redis.ConnectionPool(host="127.0.0.1", port="6379")
        # self._queue = redis.Redis(connection_pool=self._rpool)
        # self._pipeline = self._queue.pipeline()
        self._key = key_name
        self._conn = connections[Connection('redis://127.0.0.1:6379')].acquire()
        MessagingMixin.exchanges.append(Exchange(name=key_name, type='direct'))
        self._p = Producer(self._conn)

    def _send(self, msg):
        # self._pipeline.rpush(key, value).expire(key, MsgQueue.EXPIRE_TIME).execute()
        self._p.publish(
            msg,
            exchange=MessagingMixin.exchanges[self._key],
            routing_key=self._key,
            serializer='json',
            # compression='zlib',
        )
        return

    def get_ex(self):
        return MessagingMixin.exchanges[self._key]