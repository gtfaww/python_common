# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import logging
import traceback

from tornado.gen import sleep, coroutine

from ddcCommon.rabbitMQ.producer import Producer
from settings import PRODUCER, CONSUMER

__author__ = 'guotengfei'

from tornado.ioloop import IOLoop

from test.consumer_test import consumer_factory, ConsumerTest

LOGGER = logging.getLogger(__name__)


def main():
    loop = IOLoop.instance()
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    # global example
    # example = Producer(**PRODUCER)
    # example.connect()
    # example.publish_message('testdffaaaaaaaaaaaaaaaaaa', 'locationKey')
    # consumer_factory
    loop.add_timeout(deadline=(loop.time() + 10.1), callback=init_component)

    # loop.add_timeout(deadline=(loop.time() + 1.1), callback=init_component)

    loop.start()

@coroutine
def init_component():
    pass
    consumer_factory.init(ConsumerTest, **CONSUMER)
    print(11111111111)
    # try:
    #     for i in range(10000):
    #         yield sleep(0.01)
    #         example.publish_message(
    #             'testdffaaaaaaaaaaaaaaaaaatestdffaaaaaaaaaaaaaaaaaatestdffaaaaaaaaaaaaaaaaaatestdffaaaaaaaaaaaaaaaaaatestdffaaaaaaaaaaaaaaaaaatestdffaaaaaaaaaaaaaaaaaatestdffaaaaaaaaaaaaaaaaaatestdffaaaaaaaaaaaaaaaaaa',
    #             str(i))
    # except Exception as e:
    #     LOGGER.error('mq error')
    #     LOGGER.error(traceback.format_exc())

if __name__ == "__main__":
    main()
