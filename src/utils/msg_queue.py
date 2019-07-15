#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/13 2:39 PM
# @Author  : Hao Yuan
# @E-mail  : paul_yuan@sphinx.work

import redis


class MsgQueue(object):
    EXPIRE_TIME = 600

    def __init__(self):
        # Thread.__init__(self)

        # TODO: All object share one connection pool
        self._rpool = redis.ConnectionPool(host="127.0.0.1", port="6379")
        self._queue = redis.Redis(connection_pool=self._rpool)
        self._pipeline = self._queue.pipeline()

    def add(self, key, value):
        self._pipeline.rpush(key, value).expire(key, MsgQueue.EXPIRE_TIME).execute()

    def get(self, key):
        self._queue.delete(key)

