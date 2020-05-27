# -*- coding: utf-8 -*-
import json
import logging
import traceback

import tormysql
from tormysql import DictCursor
from tornado import gen
from tornado.gen import coroutine

__author__ = 'guotengfei'

from ddcCommon.util.util import message_format

LOGGER = logging.getLogger(__name__)


class AsyncMysqlClient(object):

    def __init__(self, **settings):
        self._db_name = settings['database']
        self._host = settings['ip']
        self._port = settings['port']
        self._user_name = settings['user']
        self._password = settings['password']
        self._max_conn = settings['conn_max']
        self._conn_time_out = settings['conn_time_out']
        self._charset = settings['charset']
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
            charset=self._charset
        )

    @coroutine
    def query_one(self, sql, args=None):
        data = None
        try:
            conn, cursor = yield self.get_conn_cursor()
            yield cursor.execute(sql, args)
            data = cursor.fetchone()
        except Exception as e:
            LOGGER.error(traceback.print_exc())
            LOGGER.critical(message_format("Query One error: " + sql + ' error_msg: ' + json.dumps(e.args)))
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
            LOGGER.error(traceback.print_exc())
            LOGGER.critical(message_format("Query All error: " + sql + ' error_msg: ' + json.dumps(e.args)))
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
            LOGGER.error(traceback.print_exc())
            LOGGER.critical(message_format("Execute One error: " + sql + ' error_msg: ' + json.dumps(e.args)))
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
            LOGGER.error(traceback.print_exc())
            LOGGER.critical(message_format("Execute Many error: " + sql + ' error_msg: ' + json.dumps(e.args)))
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
            LOGGER.error(traceback.print_exc())
            LOGGER.critical(message_format("Execute Many error: " + sql + ' error_msg: ' + json.dumps(e.args)))
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
