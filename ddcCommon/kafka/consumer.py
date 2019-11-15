#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from confluent_kafka.cimpl import Consumer

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 5

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)

if __name__ == '__main__':

    c = Consumer({
        'bootstrap.servers': 'qg-cdh-server-04.vcom.local:9092,qg-cdh-server-05.vcom.local:9092,qg-cdh-server-06.vcom.local:9092',
        'group.id': 'ddc_test_group',
        'auto.offset.reset': 'earliest'
    })

    c.subscribe(['ddc_test_topic1'])
    print('consumer start')
    count = 0
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        count += 1
        print('Received message: {}'.format(count))
        # print('Received message: {}'.format(msg.value().decode('utf-8')))

        # c.close()
