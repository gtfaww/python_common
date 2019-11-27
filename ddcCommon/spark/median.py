#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from pyspark import SparkContext

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 27

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    sc = SparkContext('local', appName="median")
    data = sc.parallelize([25, 1, 2, 3, 4, 5, 6, 7, 8, 100, 125,111])

    a_data = data.sortBy(lambda x: x).zipWithIndex().map(lambda b: (b[1], b[0]))
    print(a_data.collect())
    count = a_data.count()

    if count % 2 == 0:
        l = count / 2 - 1
        r = l + 1
        median = (a_data.lookup(int(l))[0] + a_data.lookup(int(r))[0]) / 2
    else:
        median = a_data.lookup(int(count / 2))[0]

    print(median)
