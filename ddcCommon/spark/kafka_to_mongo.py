#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging

import pymongo
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 28

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)


def save_location_data(pos_data):
    """
    添加一条消息
    """
    client = pymongo.MongoClient('mongodb://192.168.166.104:27017/mongodb', username='vcom', password='vcomvcom')

    db = client['mongodb']

    def doinsert(item):
        item = json.loads(item)
        ret = db.test.insert(item)
        LOGGER.info("数据保存成功")

    for item in pos_data:
        doinsert(item)


def func(rdd):
    repartitionedRDD = rdd.repartition(3)
    repartitionedRDD.foreachPartition(save_location_data)


if __name__ == '__main__':
    conf = SparkConf().setAppName("kafa").set('spark.io.compression.codec', 'snappy')
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    ssc.checkpoint('../data/checkpoint')

    kafka_parm = {
        'auto.offset.reset': 'earliest'
    }
    zk = ('qg-cdh-server-01.vcom.local:2181,qg-cdh-server-02.vcom.local:2181,qg-cdh-server-03.vcom.local:2181/kafka')
    topic = dict(ddc_test_topic=4)

    stream = KafkaUtils.createStream(ssc, zkQuorum=zk, groupId='ddc_test_group', topics=topic)
    # stream = KafkaUtils.createDirectStream(ssc, zkQuorum=zk, groupId='ddc_test_group', topics=topic)

    # msg = stream.map(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
    stream = stream.map(lambda x: x[1])
    stream.pprint(1)
    stream.foreachRDD(func)

    ssc.start()
    ssc.awaitTermination()
