#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.types import Row

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 26

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('spark sql').getOrCreate()

    df = spark.read.json('../data/people.json')
    df.show()
    df.printSchema()

    df.select(df.name, df.age).show()

    df.filter(df.age > 20).show()
    df.groupBy(df.age).count().show()

    df.sort(df.age.desc()).show()

    df.sort(df.age, df.name).show()

    df.select(df.name.alias("username"), df.age).show()

    df.write.save("../data/result", mode='overwrite')
