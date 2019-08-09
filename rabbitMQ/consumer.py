# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
from rabbitMQ.connection import MQConnection

__author__ = 'guotengfei'

import logging

import pika

LOGGER = logging.getLogger(__name__)


class Consumer(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(self, amqp_url, callback, *arg, **settings):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """

        self._url = amqp_url
        self._settings = settings
        self._callback = callback

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        MQConnection(self._url, 'consumer', **self._settings).connect()
