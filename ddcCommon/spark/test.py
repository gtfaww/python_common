#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
from pyspark.sql.types import Row

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 26

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    conf = SparkConf().setAppName('vcom')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    lines = sc.textFile('../data/users.txt')
    user = lines.map(lambda l: l.split(",")).map(lambda p: Row(id=p[0], name=p[1]))

    sql.createDataFrame(user).registerTempTable("user")
    df = sql.sql("select * from user")
    print(df.collect())
    sql.sql("select id,name form user").write.save("../data/result.txt")
