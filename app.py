# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import logging

from rabbitMQ import consumer
from rabbitMQ.producer import Producer
from settings import NBIOT_PRODUCER

__author__ = 'guotengfei'

from tornado.ioloop import IOLoop

LOGGER = logging.getLogger(__name__)



def main():
    loop = IOLoop.instance()
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    global example
    example = Producer(
        'amqp://vcom:vcomvcom@192.168.166.72:5672/%2Fvhost?connection_attempts=3&heartbeat=50', **NBIOT_PRODUCER
    )
    example.connect()
    # example.publish_message('test', 'locationKey')
    consumer
    loop.add_timeout(deadline=(loop.time() + .1), callback=init_component)

    loop.start()


def init_component():
    example.publish_message('test', 'locationKey')

if __name__ == "__main__":
    main()
