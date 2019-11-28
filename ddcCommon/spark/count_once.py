#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from operator import add

from pyspark import SparkContext

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 28

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    sc = SparkContext('local', appName="inverted")
    lines = sc.parallelize([1, 2, 2, 3, 3, 1, 5, 7, 11, 5, 7, 11, 100])
    line = lines.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x:x[1]).take(1)

    for a in line:
        print(a)
