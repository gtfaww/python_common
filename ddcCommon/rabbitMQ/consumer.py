# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
from ddcCommon.rabbitMQ.connection import MQConnection

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
        self._channel = self._connection.get_channel()
        return self._channel

    def acknowledge_message(self, delivery_tag, multiple=False, ):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.
        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self.get_channel()
        self._channel.basic_ack(delivery_tag, multiple)

    def nack_message(self, delivery_tag=None, multiple=False, requeue=True):
        """This method allows a client to reject one or more incoming messages.
        It can be used to interrupt and cancel large incoming messages, or
        return untreatable messages to their original queue.

        :param integer delivery_tag: int/long The server-assigned delivery tag
        :param bool multiple: If set to True, the delivery tag is treated as
                              "up to and including", so that multiple messages
                              can be acknowledged with a single method. If set
                              to False, the delivery tag refers to a single
                              message. If the multiple field is 1, and the
                              delivery tag is zero, this indicates
                              acknowledgement of all outstanding messages.
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.

        """
        LOGGER.info('Nack message %s', delivery_tag)
        self.get_channel()
        self._channel.basic_nack(delivery_tag, multiple=multiple, requeue=requeue)

    def reject_message(self, delivery_tag, requeue=True):
        """Reject an incoming message. This method allows a client to reject a
        message. It can be used to interrupt and cancel large incoming messages,
        or return untreatable messages to their original queue.

        :param integer delivery_tag: int/long The server-assigned delivery tag
        :param bool requeue: If requeue is true, the server will attempt to
                             requeue the message. If requeue is false or the
                             requeue attempt fails the messages are discarded or
                             dead-lettered.
        :raises: TypeError

        """
        LOGGER.info('Reject message %s', delivery_tag)
        self.get_channel()
        self._channel.basic_reject(delivery_tag, requeue)
