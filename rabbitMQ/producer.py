# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
from tornado.gen import sleep

from rabbitMQ.connection import MQConnection

__author__ = 'guotengfei'

import logging

import pika

LOGGER = logging.getLogger(__name__)


class Producer(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.
    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.
    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.
    """

    PUBLISH_INTERVAL = 1

    def __init__(self, *arg, **settings):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.
        :param str amqp_url: The URL for connecting to RabbitMQ
        """
        self._connection = None
        self._channel = None
        self._message_number = 0
        self._stopping = False
        self._url = settings.get('amqp_url')
        self.EXCHANGE = settings.get('exchange')
        self.ROUTING_KEY = settings.get('routing_key')
        self._settings = settings

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        LOGGER.info('Connecting to %s', self._url)
        self._connection = MQConnection(**self._settings)
        self._connection.connect()

    def publish_message(self, message, routing_key):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.
        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.
        """
        try:
            self.get_channel()
            if self._channel is None or not self._channel.is_open:
                LOGGER.error('channel is None, retry 3 secend later')
                sleep(3)
                self.get_channel()

            # hdrs = {u'مفتاح': u' قيمة', u'键': u'值', u'キー': u'値'}
            # properties = pika.BasicProperties(
            #     app_id='example-publisher',
            #     content_type='application/json',
            #     headers=hdrs)

            self._channel.basic_publish(self.EXCHANGE, routing_key,
                                        message)
            LOGGER.info('Published message # %s, key: %s', message, routing_key)
            return True
        except Exception, e:
            LOGGER.error("mq Published message fail:%s", e.message)
            return False

    def get_channel(self):
        """ init channel"""
        self._channel = self._connection.get_channel()
