#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import traceback

from impala.dbapi import connect

__author__ = 'guotengfei'
__time__ = 2019 / 11 / 15

"""
Module comment
"""

LOGGER = logging.getLogger(__name__)


class ImpalaClient(object):

    def __init__(self, ):
        self._conn = connect(host='qg-cdh-server-04.vcom.local', database='vcom_ddc', port=21050)

    def query_one(self, sql, args=None):
        data = None
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, args)
            data = cursor.fetchone()
        except Exception as e:
            logging.error(traceback.print_exc())
        cursor.close()
        return data

    def query_all(self, sql, args=None):
        data = None
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, args)
            data = cursor.fetchall()
        except Exception as e:
            logging.error(traceback.print_exc())
        cursor.close()
        return data

    def execute(self, sql, args=None, configuration=None):
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(sql, args, configuration)
                return cursor.status()
        except Exception as e:
            logging.error(traceback.print_exc())

    def execute_many(self, sql, args=None):
        try:
            with self._conn.cursor() as cursor:
                ret = cursor.executemany(sql, args)
        except Exception as e:
            logging.error(traceback.print_exc())
        cursor.close()
        return ret


if __name__ == '__main__':
    impala_client = ImpalaClient()
    parm = {"id": "5b2cb57ee138236139e4f8ce", "startLat": "34.82494", "commandId": "", "crossingLat": "0",
            "end_speed": 3, "start_direction": 268, "start_speed": 5, "endLat": "34.82453", "end_height": 180,
            "data_version": "1.0", "startLng": "113.55789", "crossingLng": "0", "start_height": 122,
            "end_direction": 238, "deviceId": "1bff2d66-828e-489e-a511-0df3d7486048",
            "startTime": "2018-06-22 14:50:38", "imei": "",
            "commandData": "QkQJAERbIoHS/7wAAAsyjdIARx0AAAAAAAAAAAAAagNbIoC+ADUjfgCtRo0AegWGWyKBLAA1I1UArUZMALQDdwAK//L/7vcFcvUc",
            "locationType": "1", "endLng": "113.55724", "crossingHeight": 106, "endTime": "2018-06-22 13:52:28"}
    str = "id,crossingLat,crossingLng,start_direction,start_height,startLat,commandData,end_direction,start_speed," \
          "endLat,end_height,endLng,locationType,deviceId,startTime,crossingHeight,imei,end_speed,endTime,commandId," \
          "startLng,data_version,beatFlag,speedingFlag,total_mile,avg_speed"
    parm['crossingLat'] = float(parm['crossingLat'])
    parm['crossingLng'] = float(parm['crossingLng'])
    parm['startLat'] = float(parm['startLat'])
    parm['startLng'] = float(parm['startLng'])
    parm['endLat'] = float(parm['endLat'])
    parm['endLng'] = float(parm['endLng'])
    parm['locationType'] = int(parm['locationType'])

    parm_list = []
    for s in str.split(","):
        if s == "commandId":
            parm_list.append(parm.get(s, ""))
        else:
            parm_list.append(parm.get(s, 0))
    print(tuple(parm_list))
    sql = "INSERT INTO device_position_source PARTITION (year='2019',month='11') VALUES {}".format(tuple(parm_list))
    ret = impala_client.execute(sql)
    print(ret)

    # sql = "INSERT INTO device_position_source PARTITION (year='2019',month='11') VALUES ()"
    # ret = impala_client.execute(sql,configuration=parm)
    # # ret = impala_client.execute_many(sql)
    # print(ret)
    # batch_parm = []
    # batch_parm.append(tuple(parm_list))
    # batch_parm.append(tuple(parm_list))
    # sql = "INSERT INTO device_position_source PARTITION (year='2019',month='11') VALUES %s"
    # ret = impala_client.execute_many(sql, batch_parm)
    # print(ret)
