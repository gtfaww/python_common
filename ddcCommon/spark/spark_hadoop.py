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

    sc.newAPIHadoopRDD()
