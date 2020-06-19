# -*- coding: utf-8 -*-
import traceback

__author__ = 'guotengfei'

from motor import MotorClient
import logging

from pymongo.errors import ConnectionFailure, OperationFailure

from ddcCommon.util.util import message_format

LOGGER = logging.getLogger(__name__)


class MongodbClient(object):
    """ mongodb client"""

    def __init__(self):
        self._db_name = None
        self._user_name = None
        self._password = None
        self._db = None
        self._host = None
        self._port = None
        self._client = None
        self._max_pool_size = None
        self._min_pool_size = None
        self._heartbeat = None
        self._socket_timeout = None
        self._conn_timeout = None
        self._app_name = None
        self._connect = None
        self._server_select_timeout = None

    def init_status(self, **settings):
        # 读取配置信息
        self._db_name = settings['dbname']
        self._host = settings['ip']
        self._port = settings['port']
        self._user_name = settings['user']
        self._password = settings['password']
        self._max_pool_size = settings['max_pool_size']
        self._min_pool_size = settings['min_pool_size']
        self._heartbeat = settings['heartbeat']
        self._socket_timeout = settings['socket_timeout']
        self._conn_timeout = settings['conn_timeout']
        self._server_select_timeout = settings['server_select_timeout']
        self._app_name = settings['app_name']
        self._connect = bool(settings['connect'])
        try:
            self._init_connection()
        except Exception as e:
            LOGGER.critical(message_format("mongo init error: " + e.message))

    def _init_connection(self):
        # 配置数据库的连接信息，这里并不会去连接数据库，只有在第一次数据库操作时进行连接
        if self._client is None:
            try:
                # uri = "mongodb://%s:%s@%s" % (
                #     quote_plus(self._user_name), quote_plus(self._password), self._host)

                self._client = MotorClient(host=self._host,
                                           port=self._port,
                                           username=self._user_name,
                                           password=self._password,
                                           authSource=self._db_name,
                                           maxPoolSize=self._max_pool_size,
                                           minPoolSize=self._min_pool_size,
                                           socketTimeoutMS=self._socket_timeout,
                                           connectTimeoutMS=self._conn_timeout,
                                           heartbeatFrequencyMS=self._heartbeat,
                                           serverSelectionTimeoutMS=self._server_select_timeout,
                                           connect=self._connect,
                                           appname=self._app_name)
                self._db = self._client[self._db_name]
            except ConnectionFailure as e:
                LOGGER.error(traceback.print_exc())
                LOGGER.error(message_format("mongo init error: " + e.message))
            except OperationFailure as e:
                LOGGER.error(traceback.print_exc())
                LOGGER.error(message_format("mongo init error: " + e.message))
            except Exception as e:
                LOGGER.error(traceback.print_exc())
                LOGGER.error(message_format("mongo init error: " + e.message))

            try:
                # The ismaster command is cheap and does not require auth.
                self._client.admin.command('ismaster')
            except ConnectionFailure as e:
                LOGGER.error(traceback.print_exc())
                LOGGER.error(message_format("mongo init error: " + e.message))
            except OperationFailure as e:
                LOGGER.error(traceback.print_exc())
                LOGGER.error(message_format("mongo init error: " + e.message))
            except Exception as e:
                LOGGER.error(traceback.print_exc())
                LOGGER.error(message_format("mongo init error: " + e.message))
            else:
                LOGGER.info("mongo init success")

    def get_connection(self):
        return self._db
