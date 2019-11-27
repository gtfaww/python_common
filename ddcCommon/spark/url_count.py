#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from operator import add

from pyspark import SparkContext

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 27

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    sc = SparkContext('local', appName="UrlCount")
    lines = sc.textFile('../data/access.log1')
    count = lines.map(lambda l: l.split(" ")) \
        .map(lambda b: b[6]) \
        .map(lambda b: (b, 1)) \
        .reduceByKey(add)

    top = count.top(30, key=lambda b: b[1])
    for (word, count) in top:
        print("%s: %i" % (word, count))
    # count.foreach(print())

    sc.stop()
