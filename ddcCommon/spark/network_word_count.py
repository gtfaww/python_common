#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from operator import add

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 26

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    sc = SparkContext('local', appName="PythonWordCount")

    ssc = StreamingContext(sc, 10)

    lines = sc.textFile('../data/words.txt')
    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)

    output = counts.collect()
    sort = sorted(output, key=lambda b: b[1], reverse=True)
    for (word, count) in sort:
        print("%s: %i" % (word, count))

    top = counts.top(3, key=lambda b: b[1])
    for (word, count) in top:
        print("%s: %i" % (word, count))

    sc.stop()
