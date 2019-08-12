# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
from rabbitMQ.connection import MQConnection

__author__ = 'guotengfei'

import logging

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

    def __init__(self, callback, *arg, **settings):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._url = settings.get('amqp_url')
        self._settings = settings
        self._callback = callback
        self._connection = None
        self._channel = None

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        self._connection = MQConnection(type='consumer', callback=self._callback, **self._settings)
        self._connection.connect()

    def get_channel(self):
        """ init channel"""
        if not self._channel:
            self._channel = self._connection.get_channel()
        return self._channel
