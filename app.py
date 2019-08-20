# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import logging

from ddcCommon.rabbitMQ.producer import Producer
from settings import PRODUCER
from test.consumer_test import consumer_test

__author__ = 'guotengfei'

from tornado.ioloop import IOLoop

LOGGER = logging.getLogger(__name__)


def main():
    loop = IOLoop.instance()
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    global example
    example = Producer(**PRODUCER)
    example.connect()
    # example.publish_message('test', 'locationKey')
    consumer_test
    loop.add_timeout(deadline=(loop.time() + .1), callback=init_component)

    loop.start()


def init_component():
    pass
    # example.publish_message('test', 'locationKey')


if __name__ == "__main__":
    main()
