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
    sc = SparkContext('local', appName="skew")
    table1 = sc.textFile('../data/table1.txt')
    table2 = sc.textFile('../data/table2.txt')

    table1 = table1.map(lambda x: x.split(","))

    sample = table1.map(lambda x: (x[0], x[1])) \
        .sample(False, 0.3, 9).map(lambda x: (x[0], 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).take(1)

    skew = table1.filter(lambda x: x[0] == '1').map(lambda x: (x[0], x[1]))
    new_table1 = table1.filter(lambda x: x[0] != '1').map(lambda x: (x[0], x[1]))

    table2 = table2.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1]))

    j = skew.join(table2)
    u = j.union(new_table1.join(table2))
    for a in u.collect():
        print(a)
