# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
from rabbitMQ.connection import MQConnection

__author__ = 'guotengfei'
import json

from tornado.gen import sleep

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

    def __init__(self, *arg, **settings):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.
        :param str amqp_url: The URL for connecting to RabbitMQ
        """
        self._connection = None
        self._channel = None
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._deliveries = {}
        self._stopping = False
        self._url = settings.get('amqp_url')
        self._delivery_mode = settings.get('delivery_mode', 1)
        self._mandatory = settings.get('mandatory', True)
        self.EXCHANGE = settings.get('exchange')
        self.PROPERTIES = pika.BasicProperties(delivery_mode=self._delivery_mode)
        self.PUBLISH_INTERVAL = 1
        self._settings = settings

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.
        :rtype: pika.SelectConnection
        """
        LOGGER.info('Connecting to %s', self._url)
        self._connection = MQConnection(callback=self.on_delivery_confirmation, **self._settings)
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

            self._channel.basic_publish(self.EXCHANGE, routing_key,
                                        message, properties=self.PROPERTIES,
                                        mandatory=self._mandatory)
            if not self._channel.callbacks.pending(self._channel.channel_number, '_on_return'):
                self._channel.add_on_return_callback(self.return_callback)
            self._message_number += 1
            self._deliveries.setdefault(self._message_number,
                                        {'routing_key': routing_key, 'message': message})
            LOGGER.info('Published message # %s, key: %s', message, routing_key)
            return True
        except Exception, e:
            LOGGER.error("mq Published message fail:%s", e.message)
            return False

    def get_channel(self):
        """ init channel"""
        self._channel = self._connection.get_channel()
        return self._channel

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.
        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame
        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        delivery_tag = method_frame.method.delivery_tag
        LOGGER.info('Received %s for delivery tag: %i', confirmation_type,
                    delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
            msg = self._deliveries.get(delivery_tag)
            msg = json.loads(msg)
            LOGGER.info('resend msg: %s  for %0.1f seconds ', msg['message'], self.PUBLISH_INTERVAL)
            self.schedule_next_message(msg['message'], msg['routing_key'])

        self._deliveries.pop(delivery_tag)
        LOGGER.info(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked', self._message_number,
            len(self._deliveries), self._acked, self._nacked)

    def return_callback(self, channel, method, properties, body):
        """The function to call, having the signature
            callback(channel, method, properties, body)
            where
            channel: pika.Channel
            method: pika.spec.Basic.Return
            properties: pika.spec.BasicProperties
            body: bytes

        """
        LOGGER.error('Return message %s ', body)

    def schedule_next_message(self, *arg, **args):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.
        """
        LOGGER.info('Scheduling next message for %0.1f seconds',
                    self.PUBLISH_INTERVAL)
        self._connection.get_connection().ioloop.call_later(self.PUBLISH_INTERVAL,
                                                            self.publish_message(*arg, **args))
