# -*- coding: utf-8 -*-
import logging
import re
import time
import traceback

import tormysql
from tormysql import DictCursor
from tornado import gen
from tornado.gen import coroutine

from settings import MYSQL_SETTINGS
from util import message_format


__author__ = 'guotengfei'

class AsyncMysqlClient(object):

    def __init__(self,):
        super(AsyncMysqlClient, self).__init__()
        self._db_name = MYSQL_SETTINGS['db']
        self._host = MYSQL_SETTINGS['host']
        self._port = MYSQL_SETTINGS['port']
        self._user_name = MYSQL_SETTINGS['user']
        self._password = MYSQL_SETTINGS['passwd']
        self._max_conn = MYSQL_SETTINGS['max_conn']
        self._conn_time_out = MYSQL_SETTINGS['conn_time_out']
        self._db = None
        self._client = None

        self.pool = tormysql.ConnectionPool(
            max_connections=self._max_conn,  # max open connections
            idle_seconds=7200,  # connection idle timeout time, 0 is not timeout
            wait_connection_timeout=self._conn_time_out,  # wait connection timeout
            host=self._host,
            port=self._port,
            user=self._user_name,
            passwd=self._password,
            db=self._db_name,
            charset="utf8"
        )

    @coroutine
    def query_one(self, sql, args=None):
        data = None
        try:
            conn, cursor = yield self.get_conn_cursor()
            yield cursor.execute(sql, args)
            data = cursor.fetchone()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(message_format("Query One error: %s" % e.message))
        else:
            yield conn.commit()
        cursor.close()
        self.pool.release_connection(conn)
        raise gen.Return(data)

    @coroutine
    def query_all(self, sql, args=None):
        try:
            conn, cursor = yield self.get_conn_cursor()
            yield cursor.execute(sql, args)
            datas = cursor.fetchall()
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(message_format("Query All error: %s" % e.message))
        else:
            yield conn.commit()
        cursor.close()
        self.pool.release_connection(conn)
        raise gen.Return(datas)

    @coroutine
    def execute(self, sql, args=None):
        try:
            conn, cursor = yield self.get_conn_cursor()
            ret = yield cursor.execute(sql, args)
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(message_format("Execute One error: %s" % e.message))
            yield conn.rollback()
        else:
            yield conn.commit()
        cursor.close()
        self.pool.release_connection(conn)
        raise gen.Return(ret)

    @coroutine
    def execute_many(self, sql, args=None):
        try:
            conn, cursor = yield self.get_conn_cursor()
            ret = yield cursor.executemany(sql, args)
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(message_format("Execute Many error: %s" % e.message))
            yield conn.rollback()
        else:
            yield conn.commit()
        cursor.close()
        self.pool.release_connection(conn)
        raise gen.Return(ret)

    @coroutine
    def execute_sqls(self, sqls):
        try:
            conn, cursor = yield self.get_conn_cursor()
            for sql in sqls:
                ret = yield cursor.execute(sql)
        except Exception as e:
            logging.error(traceback.print_exc())
            logging.error(message_format("Execute Many error: %s" % e.message))
            yield conn.rollback()
            rets = False
        else:
            yield conn.commit()
            rets = True
        cursor.close()
        self.pool.release_connection(conn)
        raise gen.Return(rets)

    @coroutine
    def get_conn_cursor(self):
        conn = yield self.pool.Connection()
        cursor = conn.cursor(cursor_cls=DictCursor)
        raise gen.Return((conn, cursor))


async_mysql_client = AsyncMysqlClient()