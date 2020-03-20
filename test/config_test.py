# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import os
import sys

from ddcCommon.util.util import get_parameter_config
from settings import CONFIG_SETTINGS, CONFIG_KEY_VALUES, REDIS_SETTINGS, MYSQL_SETTINGS, BATTERY_PRODUCER

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
    file_path = FILE_PATH + '\config_setting.ini'
    result = get_parameter_config(file_path, CONFIG_SETTINGS, CONFIG_KEY_VALUES)

    print result
    print REDIS_SETTINGS
    print MYSQL_SETTINGS
    print BATTERY_PRODUCER
