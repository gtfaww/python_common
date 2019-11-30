#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 28

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':

    sc = SparkContext('local', appName="stock")
    ssc = StreamingContext(sc, 10)

    kafka_parm = {
        'bootstrap.servers': 'qg-cdh-server-04.vcom.local:9092,qg-cdh-server-05.vcom.local:9092,qg-cdh-server-06.vcom.local:9092',
        'group.id': 'ddc_test_group',
        'auto.offset.reset': 'earliest'
    }

    stream = KafkaUtils.createStream()
    stream.pprint(5)
    ssc.start()
