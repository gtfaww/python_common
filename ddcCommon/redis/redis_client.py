# -*- coding: utf-8 -*-
# @author: zhangxiaoning
# Created on  7 18, 2014
#
import logging

from redis.sentinel import (Sentinel)

from settings import REDIS_SETTINGS


def redis_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            logging.info(e.message)
            auto_status = False
            logging.info('set auto_status=False')
            return None

    return wrapper


class RedisClient(object):
    pwd_switch = REDIS_SETTINGS['passwd_switch']
    if pwd_switch:
        pwd = REDIS_SETTINGS['password']
    else:
        pwd = ''
    sentinel = Sentinel(
        [(REDIS_SETTINGS['ip1'], REDIS_SETTINGS['port1']), (REDIS_SETTINGS['ip2'], REDIS_SETTINGS['port2']),
         (REDIS_SETTINGS['ip3'], REDIS_SETTINGS['port3']), (REDIS_SETTINGS['ip4'], REDIS_SETTINGS['port4'])
         ], socket_timeout=0.1, password=pwd)
    master = sentinel.master_for('master1', socket_timeout=1)
    slave = sentinel.slave_for('master1', socket_timeout=1)

    @staticmethod
    @redis_decorator
    def exists(name):
        result = RedisClient.slave.exists(name)
        return result

    @staticmethod
    @redis_decorator
    def set(name, value, timeout=0):
        if (timeout == 0):
            result = RedisClient.master.set(name, value)
        else:
            result = RedisClient.master.set(name, value, timeout)
        return result

    @staticmethod
    @redis_decorator
    def get(name):
        result = RedisClient.slave.get(name)
        return result

    @staticmethod
    @redis_decorator
    def incr(name):
        result = RedisClient.master.incr(name)
        return result

    @staticmethod
    @redis_decorator
    def delete(*names):
        result = RedisClient.master.delete(*names)
        return result

    @staticmethod
    @redis_decorator
    def randomkey():
        result = RedisClient.slave.randomkey()
        return result

    @staticmethod
    @redis_decorator
    def type(key):
        result = RedisClient.slave.type(key)
        return result

    @staticmethod
    @redis_decorator
    def setnx(name, value):
        result = RedisClient.master.setnx(name, value)
        return result

    @staticmethod
    @redis_decorator
    def hset(name, key, value):
        result = RedisClient.master.hset(name, key, value)
        return result

    @staticmethod
    @redis_decorator
    def hget(name, key):
        result = RedisClient.slave.hget(name, key)
        return result

    @staticmethod
    @redis_decorator
    def hkeys(name):
        result = RedisClient.slave.hkeys(name)
        return result

    @staticmethod
    @redis_decorator
    def hmset(name, mapping):
        result = RedisClient.master.hmset(name, mapping)
        return result

    @staticmethod
    @redis_decorator
    def hmget(name, keys):
        result = RedisClient.slave.hmget(name, keys)
        return result

    @staticmethod
    @redis_decorator
    def hgetall(name):
        result = RedisClient.slave.hgetall(name)
        return result

    @staticmethod
    @redis_decorator
    def lpush(name, value):
        result = RedisClient.master.lpush(name, value)
        return result

    @staticmethod
    @redis_decorator
    def lpop(name):
        result = RedisClient.master.lpop(name)
        return result

    @staticmethod
    @redis_decorator
    def rpush(name, value):
        result = RedisClient.master.rpush(name, value)
        return result

    @staticmethod
    @redis_decorator
    def rpop(name):
        result = RedisClient.master.rpop(name)
        return result

    @staticmethod
    @redis_decorator
    def llen(name):
        result = RedisClient.slave.llen(name)
        return result

    @staticmethod
    @redis_decorator
    def sadd(name, *values):
        result = RedisClient.master.sadd(name, *values)
        return result

    @staticmethod
    @redis_decorator
    def smembers(name):
        result = RedisClient.slave.smembers(name)
        return result

    @staticmethod
    @redis_decorator
    def sdiff(keys, *args):
        result = RedisClient.slave.sdiff(keys, *args)
        return result

    @staticmethod
    @redis_decorator
    def sinter(keys, *args):
        result = RedisClient.slave.sinter(keys, *args)
        return result

    @staticmethod
    @redis_decorator
    def sunion(keys, *args):
        result = RedisClient.slave.sunion(keys, *args)
        return result

    @staticmethod
    @redis_decorator
    def setbit(name, offset, value):
        value = value and 1 or 0
        result = RedisClient.master.setbit(name, offset, value)
        return result

    @staticmethod
    @redis_decorator
    def getbit(name, offset):
        result = RedisClient.slave.sunion(name, offset)
        return result

    @staticmethod
    @redis_decorator
    def keys(pattern):
        result = RedisClient.slave.keys(pattern)
        return result

    @staticmethod
    @redis_decorator
    def lrange(name, start, end):
        result = RedisClient.slave.lrange(name, start, end)
        return result

    @staticmethod
    @redis_decorator
    def lrem(name, value):
        result = RedisClient.master.lrem(name, 1, value)
        return result

