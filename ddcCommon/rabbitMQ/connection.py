# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

__author__ = 'guotengfei'

import functools
import logging
import traceback

import pika
from pika.adapters.tornado_connection import TornadoConnection

LOGGER = logging.getLogger(__name__)


# 异常捕获
def exception_catch(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            LOGGER.error("conn init Error: %s", repr(e))
            LOGGER.error(traceback.format_exc())
            conn = args[0]
            conn.close_channel()

    return wrapper


class MQConnection(object):
    """
    MQ连接管理类
    """

    def __init__(self, url, type='producer', callback=None, *arg, **settings):
        """Create a new instance of the MQConnection class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with
        :param str type: connection type,for excmple,'consumer','producer'
        :param str callback: if type is 'consumer',callback is not None

        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = url
        self._type = type
        self._was_consuming = False
        self._was_publishing = False
        self._reconnect_delay = 0
        self._callback = callback
        self.EXCHANGE = settings.get('exchange')
        self.QUEUE = settings.get('queue')
        self.ROUTING_KEY = settings.get('routing_key')
        self.EXCHANGE_TYPE = settings.get('exchange_type')
        self.AE_EXCHANGE = settings.get('ae_exchange')
        self.AE_QUEUE = settings.get('ae_queue')
        self.AE_EXCHANGE_TYPE = settings.get('ae_exchange_type')
        self.DL_EXCHANGE = settings.get('dl_exchange')
        self.DL_QUEUE = settings.get('dl_queue')
        self.DL_EXCHANGE_TYPE = settings.get('dl_exchange_type')
        self._passive = settings.get('passive', True)
        self._durable = settings.get('durable', True)
        self._prefetch_count = settings.get('prefetch_count', 128)

    @exception_catch
    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        self._connection = TornadoConnection(pika.URLParameters(self._url),
                                             on_open_callback=self.on_connection_open,
                                             on_open_error_callback=self.on_connection_open_error)
        return self._connection

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        reconnect_delay = self._get_reconnect_delay()
        LOGGER.error('Connection open failed, reopening in %d seconds: %s', reconnect_delay, err)
        self._connection.ioloop.call_later(reconnect_delay, self.reconnect)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._closing:
            pass
            # self._connection.ioloop.stop()
        else:
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.warning('Connection closed, reopening in %d seconds: %s',
                           reconnect_delay, reason)
            self._connection.ioloop.call_later(reconnect_delay, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        self._was_consuming = False
        self._was_publishing = False
        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        if self._connection.is_open:
            self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)
        if self.AE_EXCHANGE:
            self.setup_ae_exchange(self.AE_EXCHANGE)
        if self.DL_EXCHANGE:
            self.setup_dl_exchange(self.DL_EXCHANGE)

    @exception_catch
    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        args = {}
        if self.AE_EXCHANGE:
            args['alternate-exchange'] = self.AE_EXCHANGE

        self._channel.exchange_declare(
            passive=self._passive,
            durable=self._durable,
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            arguments=args,
            callback=cb)

    @exception_catch
    def setup_ae_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        ae_cb = functools.partial(
            self.on_ae_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            passive=self._passive,
            durable=False,
            exchange=exchange_name,
            exchange_type=self.AE_EXCHANGE_TYPE,
            arguments={},
            callback=ae_cb)

    @exception_catch
    def setup_dl_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        cb = functools.partial(
            self.on_dl_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            passive=self._passive,
            durable=False,
            exchange=exchange_name,
            exchange_type=self.DL_EXCHANGE_TYPE,
            arguments={},
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE)

    def on_ae_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_ae_queue(self.AE_QUEUE)

    def on_dl_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        LOGGER.info('Exchange declared: %s', userdata)
        self.setup_dl_queue(self.DL_QUEUE)

    @exception_catch
    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        LOGGER.info('Declaring queue %s', queue_name)
        args = {}
        if self.DL_EXCHANGE:
            args['x-dead-letter-exchange'] = self.DL_EXCHANGE

        self._channel.queue_declare(
            durable=self._durable,
            passive=self._passive,
            queue=queue_name,
            arguments=args,
            callback=self.on_queue_declareok)

    @exception_catch
    def setup_ae_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(
            durable=False,
            passive=self._passive,
            queue=queue_name,
            callback=self.on_ae_queue_declareok)

    @exception_catch
    def setup_dl_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(
            durable=False,
            passive=self._passive,
            queue=queue_name,
            callback=self.on_dl_queue_declareok)

    @exception_catch
    def on_queue_declareok(self, _unused_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, self.QUEUE,
                    self.ROUTING_KEY)
        self._channel.queue_bind(
            self.QUEUE,
            self.EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=self.on_bindok)

    @exception_catch
    def on_ae_queue_declareok(self, _unused_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        LOGGER.info('Binding %s to %s with %s', self.AE_EXCHANGE, self.AE_QUEUE,
                    self.ROUTING_KEY)
        self._channel.queue_bind(
            self.AE_QUEUE,
            self.AE_EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=self.on_bindok)

    @exception_catch
    def on_dl_queue_declareok(self, _unused_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        LOGGER.info('Binding %s to %s with %s', self.DL_EXCHANGE, self.DL_QUEUE,
                    self.ROUTING_KEY)
        self._channel.queue_bind(
            self.DL_QUEUE,
            self.DL_EXCHANGE,
            routing_key=self.ROUTING_KEY,
            callback=self.on_bindok)

    def on_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        LOGGER.info('Queue bound')
        if self._type == 'consumer':
            if not self._was_consuming:
                self.start_consuming()
        else:
            if not self._was_publishing:
                self.start_publishing()

    @exception_catch
    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    @exception_catch
    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info('start consuming')
        self._was_consuming = True
        self.add_on_cancel_callback()
        self._channel.basic_qos(prefetch_count=self._prefetch_count)
        self._consumer_tag = self._channel.basic_consume(self.QUEUE, self._callback)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.
        """
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.
        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ
        """
        LOGGER.info('start publishing')
        self._was_publishing = True
        self.enable_delivery_confirmations()
        # self.schedule_next_message()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.
        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.
        """
        LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self._callback)

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    @exception_catch
    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def get_channel(self):
        """return _channel.
        """
        return self._channel

    def get_connection(self):
        """return _connection.
        """
        return self._connection

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self._closing = True
        LOGGER.info('Stopped')

    def _get_reconnect_delay(self):
        if self._was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
