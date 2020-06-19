#
# -*- coding: utf-8 -*-


__author__ = 'guotengfei'
__date__ = '2016/12/21'

# RabbitMQ 配置信息
PRODUCER = dict(produce_amqp_url="amqp://vcom:vcomvcom@192.168.150.24:5671/%2Fvhost?connection_attempts=3&heartbeat=50",
                queue='testQueue',
                routing_key='testKey',
                exchange='testExchange',
                exchange_type='direct',
                ae_exchange='aeExchange',
                ae_exchange_type='fanout',
                ae_queue='aeQueue',
                dl_exchange='dlExchange',
                dl_exchange_type='direct',
                dl_queue='dlQueue',
                durable=True,
                passive=False,
                max_conn=3,  # 最大conn数量
                delivery_mode=1,  # 2消息持久化  1不持久化
                mandatory=False  # 没有队列消费数据时返回消息
                )

# RabbitMQ 配置信息
CONSUMER = dict(
    consumer_amqp_url=["amqp://vcom:vcomvcom@192.168.150.24:5671/%2Fvhost?connection_attempts=3&heartbeat=50",
                       ],
    queue='testQueue',
    routing_key='testKey',
    exchange='testExchange',
    exchange_type='direct',
    ae_exchange='aeExchange',
    ae_exchange_type='fanout',
    ae_queue='aeQueue',
    dl_exchange='dlExchange',
    dl_exchange_type='direct',
    dl_queue='dlQueue',
    durable=False,
    passive=False,
    delivery_mode=2,  # 2消息持久化  1不持久化
    mandatory=False,  # 没有队列消费数据时返回消息
    prefetch_count=128  # 预取消息数量
)

# mongodb 配置信息
MONGO_SETTING = dict(
    ip='192.168.119.75',
    user='uxin',
    password='YTk0_ODRiNTA0ZDU2',
    port=27018,
    dbname='uxin',
    max_pool_size=100,
    min_pool_size=5,
    heartbeat=10000,
    socket_timeout=10000,  # 单位 毫秒
    conn_timeout=20000,
    app_name='python_common',
    server_select_timeout=10000,
    connect=1  # 是否立即建立连接
)

# 读取参数配置信息
CONFIG_SETTINGS = dict(
    url="http://192.168.166.102:10001/config/getConfigByServiceID",
    app='USBAccessServer',
    Authorization='Basic dmNvbV90ZXN0OnZjb212Y29t',
    user="ddc",  # 用户名
    password="dmNvbXZjb20=")  # 密码

# redis 配置信息
REDIS_SETTINGS = dict()
# mysql 配置信息
MYSQL_SETTINGS = dict()

# 接入服务队列消费者
ORIGIN_CONSUMER = dict()

# 判断队列
NBIOT_PRODUCER = dict()
# 持久化队列
DURABLE_PRODUCER = dict()
# 告警队列
ALARM_PRODUCER = dict()
# 电池监控队列
BATTERY_PRODUCER = dict()

# 解析服务私有配置
ACCESSS_SERVER_CONFIG = dict()

# 配置中心参数配置
CONFIG_KEY_VALUES = {
    "mysql_config": MYSQL_SETTINGS,
    "redis_config": REDIS_SETTINGS,
    "rabbitmq_config_access": ORIGIN_CONSUMER,
    "rabbitmq_config_alarm": ALARM_PRODUCER,
    "rabbitmq_config_durable": DURABLE_PRODUCER,
    "rabbitmq_config_location": NBIOT_PRODUCER,
    "rabbitmq_config_battery": BATTERY_PRODUCER,
    "usbaccessserver_config": ACCESSS_SERVER_CONFIG,
}
