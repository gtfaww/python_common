#
# -*- coding: utf-8 -*-
import os


__author__ = 'guotengfei'
__date__ = '2016/12/21'


# mongodb配置
MQ_PRODUCE_SETTINGS = dict(
    alarm_key="alarmKey",
    alarm_produce_num=1,
    durable_key="durableKey",
    durable_produce_num=1,
    location_key="locationKey",
    location_produce_num=1,
    constatnt_location_key="constantlocationKey",
    constatnt_location_produce_num=1,
    vcom_key="vcomKey",
    vcom_produce_num=1,
    file_key="fileKey",
    file_produce_num=1
)

# notifiaction RabbitMQ 配置信息
NBIOT_PRODUCER = dict(host="192.168.166.72",
                      port=5672,
                      vhost="/vhost",
                      user="vcom",
                      password="vcomvcom",
                      heartbeat=50,
                      queue='locationQueue',
                      routing_key='locationKey',
                      exchange='nbIotExchange',
                      exchange_type='direct',
                      durable=False
)

NBIOT_CONSTANT_PRODUCER = dict(host="192.168.108.97",
                      port=5672,
                      vhost="/vhost",
                      user="vcom",
                      password="vcomvcom",
                      heartbeat=50,
                      queue='constantlocationQueue',
                      routing_key='constantlocationKey',
                      exchange='constantlocationExchange',
                      exchange_type='direct',
                      durable=False
)

DURABLE_PRODUCER = dict(host="192.168.108.97",
                        port=5672,
                        vhost="/vhost",
                        user="vcom",
                        password="vcomvcom",
                        heartbeat=50,
                        queue='durableQueue',
                        routing_key='durableKey',
                        exchange='durableExchange',
                        exchange_type='direct',
                        durable=False
)

ALARM_PRODUCER = dict(host="192.168.108.97",
                      port=5672,
                      vhost="/vhost",
                      user="vcom",
                      password="vcomvcom",
                      heartbeat=50,
                      queue='alarmQueue',
                      routing_key='alarmKey',
                      exchange='alarmExchange',
                      exchange_type='direct',
                      durable=False
)

VCOM_CUSTOMER_PRODUCER = dict(host="192.168.108.97",
                      port=5672,
                      vhost="/vhost",
                      user="vcom",
                      password="vcomvcom",
                      heartbeat=50,
                      queue='vcomQueue',
                      routing_key='vcomKey',
                      exchange='vcomExchange',
                      exchange_type='direct',
                      durable=False
)

FILE_PRODUCER = dict(host="192.168.108.97",
                      port=5672,
                      vhost="/vhost",
                      user="vcom",
                      password="vcomvcom",
                      heartbeat=50,
                      queue='fileQueue',
                      routing_key='fileKey',
                      exchange='fileExchange',
                      exchange_type='direct',
                      durable=False
)


CMCC_TOKEN = dict(token='abcdefghijkmlnopqrstuvwxyz')

# 中国移动NB北向API配置
CMCC_NB_API_SETTINGS = dict(HttpsIP='http://api.heclouds.com',
                            device_manage_url='/devices',
                            send_command='/nbiot?obj_id=3311&obj_inst_id=0&mode=2'
                       )

# 中国移动NB 应用配置信息
CMCC_NB_APP_SETTINGS = [
                dict(
                appName='FJ_DDC',
                APIKey='NscAqZVq9yhPu3kRaDRnu5qSOeU='
                )
]
# 中国移动NB 上行应答回复
CMCC_NB_RESPONSE_SETTINGS = dict(
    warningRsp='42442800030100002CFF',# 告警类型数据的回复
    locationRsp='42442800030900003417',# 位置信息上报类型数据的回复
    versionRsp='4244280003050000300B',# 版本信息上报类型数据的回复
    configRsp='4244280003060000310E',# 配置信息上报类型数据的回复
    elecRsp='42442800030200002D02',# 电量告警上报类型数据的回复
)
TIME_FOR_THAILAND = True

TIME_ZONE = "Asia/Bangkok"

URL_SETTINGS = [
        dict(
            customer_no="hngliot",
            messageurl="http://rq.crownedforest.com/Accept/postMessage.ashx",
            routeurl = "http://rq.crownedforest.com/Accept/postRoute.ashx"

        ),
        dict(
            customer_no ="49",
            url="http://192.168.164.91:7001/v1.0/users"
        )
]


# redis 配置信息
REDIS_SETTINGS = dict(switch=True,  # 是否使用redis
                      ip1='192.168.166.101',
                      port1=26379,
                      ip2="192.168.166.102",
                      port2=26379,
                      ip3="192.168.164.101",
                      port3=26379,
                      ip4="192.168.164.102",
                      port4=26379,
                      master='master1',
                      passwd='VcomP@ssw0rd'
                      )
# PING配置
PING_SETTINGS = dict(
    mq_test=True,
    mysql_test=True,
    redis_test=True,
)