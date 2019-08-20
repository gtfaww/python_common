# -*- encoding: utf-8 -*-
import time
from datetime import datetime

from pyzabbix import ZabbixAPI, ZabbixMetric, ZabbixSender

__author__ = 'guotengfei'


def main():
    # Create ZabbixAPI class instance
    zapi = ZabbixAPI(url='http://192.168.164.4/zabbix/', user='admin', password='admin')

    # Get all monitored hosts
    result1 = zapi.host.get(monitored_hosts=1, output='extend')

    # Get all disabled hosts
    result2 = zapi.do_request('host.get',
                              {
                                  'filter': {'status': 1},
                                  'output': 'extend'
                              })

    # Filter results
    hostnames1 = [host['host'] for host in result1]
    hostnames2 = [host['host'] for host in result2['result']]

    # Logout from Zabbix
    zapi.user.logout()

def send():
    packet = [
        ZabbixMetric('192.168.164.4', 'trap', 'error: write mq error')
    ]

    result = ZabbixSender('192.168.164.4',10051).send(packet)
    print(result)

if __name__ == '__main__':
    # main()
    send()