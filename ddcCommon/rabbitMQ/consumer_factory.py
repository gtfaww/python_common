# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205
import traceback

__author__ = 'guotengfei'

import logging

LOGGER = logging.getLogger(__name__)


class ConsumerFactory(object):
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
        pass

    def init(self, cls, **CONSUMER):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        try:
            urls = CONSUMER.get('amqp_url')
            if isinstance(urls, list):
                for url in urls:
                    cls(url, **CONSUMER)
            else:
                cls(urls, **CONSUMER)
        except Exception as e:
            LOGGER.error(traceback.format_exc())
