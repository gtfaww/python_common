#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from pyspark import SparkContext

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 28

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':
    sc = SparkContext('local', appName="inverted")
    lines = sc.textFile('../data/inverted.txt')
    line = lines.map(lambda x: x.split('|')) \
        .flatMap(lambda l: [(x, l[0]) for x in l[1].split(",")]) \
        .groupByKey()
    for a, b in line.collect():
        print(a, b.data)

    # map(lambda x: (x[1],x[0]))
