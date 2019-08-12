#
# -*- coding: utf-8 -*-


__author__ = 'guotengfei'
__date__ = '2016/12/21'

# RabbitMQ 配置信息
PRODUCER = dict(amqp_url="amqp://vcom:vcomvcom@192.168.166.72:5672/%2Fvhost?connection_attempts=3&heartbeat=50",
                      queue='locationQueue',
                      routing_key='locationKey',
                      exchange='nbIotExchange',
                      exchange_type='direct',
                      durable=False,
                      passive=False
                      )


# RabbitMQ 配置信息
CONSUMER = dict(amqp_url="amqp://vcom:vcomvcom@192.168.166.72:5672/%2Fvhost?connection_attempts=3&heartbeat=50",
                      queue='locationQueue',
                      routing_key='locationKey',
                      exchange='nbIotExchange',
                      exchange_type='direct',
                      durable=False,
                      passive=False
                      )