# -*- coding: utf-8 -*-

__author__ = 'guotengfei'
__date__ = '2020/3/20'

import json
import logging
import traceback
from ConfigParser import RawConfigParser

import requests

LOGGER = logging.getLogger(__name__)


def get_parameter_config(local_file_path, config_settings, config_key_values):
    """
    从配置中心获取参数配置 如果失败重试3次；如果重试3次仍不成功，从备份配置文件中读取，如不成功，则失败
    :param local_file_path: 本地文件路径
    :param config_settings: 配置中心配置
    :param config_key_values: 获取那些配置
    :rtype: object
    :return:
    """
    result = False
    for i in range(3):
        result = get_parameter_config_setting(local_file_path, config_settings, config_key_values)
        # 如果取到数据 跳出循环
        if result:
            break
    if not result:
        # 读取备份配置文件
        config = RawConfigParser()
        config.optionxform = str
        config.read(local_file_path)
        sections = config.sections()

        # 配置文件中是否含有配置

        if len(sections) > 0:
            for key, value in config_key_values.items():
                item = config.items(key)
                for (k, v) in item:
                    if v.isdigit():
                        v = int(v)
                    value.setdefault(k, v)
            # 含有配置 则表示成功
            result = True
        else:
            # 不含有配置 则表示失败
            result = False
    return result


def get_parameter_config_setting(local_file_path, config_settings, config_key_values):
    """
    从配置纵向获取参数配置放入缓存并写入备份文件
    :param local_file_path: 本地文件路径
    :param config_settings: 配置中心配置
    :param config_key_values: 获取那些配置
    :return:
    """
    url = config_settings["url"]
    config_settings.setdefault("action", "getconfig")
    json_data = json.dumps(config_settings)

    headers = dict()
    headers['authorization'] = config_settings[
        'Authorization'] if 'Authorization' in config_settings else 'Basic dmNvbV90ZXN0OnZjb212Y29t'

    res = syn_send_http_req(url, parm=json_data, headers=headers, method="POST")
    if res:
        res = json.loads(res.content)
        if res['code'] != 1:
            result = False
        else:
            data = res['data']
            parameter_config_setting(local_file_path, data, config_key_values)
            result = True
    else:
        result = False
    return result


def parameter_config_setting(local_file_path, data, config_key_values):
    """
    把参数配置放入缓存并写入备份文件
    :return:
    """
    raw_config = RawConfigParser()
    raw_config.optionxform = str
    raw_config.read(local_file_path)

    get_config(data, raw_config, config_key_values)

    # 保存到配置文件
    try:
        cf = open(local_file_path, 'w')
        raw_config.write(cf)
        cf.close()
    except Exception, e:
        LOGGER.critical(traceback.print_exc())
    else:
        LOGGER.info("Config write success")


def get_config(data, raw_config, key_values):
    """
    从配置中心获取数据
    :param data:
    :param raw_config:
    :param key_values:
    :return:
    """
    for key, value in key_values.items():
        if key in data:
            config = data[key]
            value.update(config)

            if not raw_config.has_section(key):
                raw_config.add_section(key)

            for (k, v) in config.items():
                raw_config.set(key, k, v)


def syn_send_http_req(req_url, parm, headers, method="POST"):
    """
    发送同步请求
    :param req_url:请求url
    :param parm:请求参数
    :param headers:请求头部
    :param method:请求方式 get post
    :return:
    """
    try:
        response = requests.request(method=method, url=req_url, headers=headers, data=parm, timeout=10.0)
    except Exception:
        LOGGER.critical(traceback.format_exc())
        LOGGER.critical("Acquire Parameter Config failed!", '')
        response = None
    if response:
        pass
    else:
        response = None
    return response
