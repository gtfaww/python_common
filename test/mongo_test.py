# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import os
import sys

from ddcCommon.MongoDB.mongo_client import MongodbClient
from settings import MONGO_SETTING

__author__ = 'guotengfei'

import logging

LOGGER = logging.getLogger(__name__)

PATH = sys.path[0]
FILE_PATH = None

if os.path.isdir(PATH):
    FILE_PATH = PATH
elif os.path.isfile(PATH):
    FILE_PATH = os.path.dirname(PATH)

if __name__ == '__main__':
    async_mongo_client = MongodbClient()
    async_mongo_client.init_status(**MONGO_SETTING)

    db = async_mongo_client.get_connection()
    ret = db.DevicePositionSource.find_one({'-id':'5e3bc09c5f7c1abb54338b9a'})
    print (ret)
