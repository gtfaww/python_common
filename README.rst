# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

from rabbitMQ.consumer import Consumer
from settings import CONSUMER

__author__ = 'guotengfei'

import logging

LOGGER = logging.getLogger(__name__)


class ConsumerTest(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    def __init__(self):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._channel = None
        self._consumer = Consumer(self.on_message, **CONSUMER)
        self._consumer.connect()

    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body

        """
        self._channel = channel
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        self._consumer.acknowledge_message(basic_deliver.delivery_tag)


consumer_test = ConsumerTest()


producer example
PRODUCER = dict(amqp_url="amqp://vcom:vcomvcom@192.168.166.72:5672/%2Fvhost?connection_attempts=3&heartbeat=50",
                queue='locationQueue',
                routing_key='locationKey',
                exchange='nbIotExchange',
                exchange_type='direct',
                durable=False,
                passive=False,
                delivery_mode=1,
                mandatory=True
                )

example = Producer(**PRODUCER)
example.connect()
example.publish_message('test', 'locationKey')

###consumer example
CONSUMER = dict(amqp_url="amqp://vcom:vcomvcom@192.168.166.72:5672/%2Fvhost?connection_attempts=3&heartbeat=50",
                queue='locationQueue',
                routing_key='locationKey',
                exchange='nbIotExchange',
                exchange_type='direct',
                durable=False,
                passive=False,
                delivery_mode=1,
                mandatory=True,
                prefetch_count=128
                )