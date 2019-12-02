#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import traceback

from confluent_kafka.cimpl import Producer

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 5

"""
kafka  Producer
"""

LOGGER = logging.getLogger(__name__)


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == '__main__':
    try:
        p = Producer({
            'bootstrap.servers': 'qg-cdh-server-04.vcom.local:9092,qg-cdh-server-05.vcom.local:9092,qg-cdh-server-06.vcom.local:9092'})

        # data = requests.get('http://hq.sinajs.cn/list=sh601006,sh601008')
        # print(data.text)
        # for dt in data.text.split(";"):
        #     d = dt.split("=")[1]
        #     ret = p.produce('ddc_test_topic', d.encode('utf-8'), callback=delivery_report)
        #     #     print(ret)

        for i in range(10000):
            data = '{"crossingLat": 34.82712, "crossingLng": 113.55631, "start_direction": 102, "start_height": 92, "startLat": 34.77748, "commandData": "QkQZAMRcUyqU/7EPAAoAoDcAAAsAADUkWACtRe8AahpcUylyADUQ9ACubpIAXAgiASL+3AOj8QU7hJ4K+zv/hJ4K/Tz+hR8K+z7/hR4K+0H9hR8K/EL9hR0K/UD/hKEK+j79hSEK/0H9hR8K+UH/hR8K+kL+hKAK/j8BhR8K+UL/g6IK+jj/gqcK+isBgqIK+SgFgL8e/xkAgMEK+wYAgj0e8AMAwr4K5P7/gr0K5AADwjwK5P/+gjwK5AEBwsAK4f7/gzwK4gD/T/M=", "end_direction": 177, "start_speed": 16, "endLat": 34.774559999999994, "end_height": 77, "endLng": 114.32501, "locationType": 1, "deviceId": "a3beff27-d42f-42ef-a789-ebe3b44baff6", "device_id": "a3beff27-d42f-42ef-a789-ebe3b44baff6", "startTime": "2019-01-31 15:59:30", "crossingHeight": 106, "imei": "", "end_speed": 10, "endTime": "2019-01-31 16:04:20", "commandId": "", "startLng": 114.3157, "temperature": 15, "data_version": "1.1", "speedingFlag": 0, "beatFlag": 0, "timestamp": "2019-01-31 17:04:20", "point_id": "2019IQP7kRsreA", "police_id": "2019IYLfYZdSjN", "police_parent_id": "2019IYLQgxivLK", "create_by_id": "2019IXJwjBMBQe"}'
            # Trigger any available delivery report callbacks from previous produce() calls
            p.poll(0)
            #
            #     # Asynchronously produce a message, the delivery report callback
            #     # will be triggered from poll() above, or flush() below, when the message has
            #     # been successfully delivered or failed permanently.
            ret = p.produce('ddc_test_topic', data.encode('utf-8'), callback=delivery_report)
            print(ret)

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        p.flush()
    except Exception as e:
        print(traceback.format_exc())
