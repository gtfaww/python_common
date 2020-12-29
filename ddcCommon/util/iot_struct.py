# -*- coding: utf-8 -*-
"""
IOT struct
"""

# @Author：guotengfei
# @Version：1.0
# @Time：2020/8/19
import base64
import binascii
import functools
import logging
import struct
import traceback
from struct import unpack_from

from util import datetime_as_timezone

from ddcCommon.util.util import get_byte_height_4, get_byte_low_4

LOGGER = logging.getLogger(__name__)


class DDCException(Exception):
    def __init__(self, code, message, request_id=None):
        self.code = code
        self.message = message
        self.request_id = request_id


class ParseErrorException(DDCException):
    pass


class ConfigErrorException(DDCException):
    pass


def exception_catch(cls):
    """
    cls 是自定义异常，
    """

    def exception_catch_wrap(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                LOGGER.error(e.message)
                LOGGER.error(traceback.format_exc())
                raise cls(500, 'parse error')

        return wrapper

    return exception_catch_wrap


class IOTStruct():
    """
    iot 数据处理
    """

    def __init__(self):
        pass

    def pack(self, data, frm):
        """
        封装数据
        :param data: 二进制data, base64 decode后的
        :param frm: json 配置
        :return:
        """
        if not isinstance(frm, list):
            raise ConfigErrorException(500, 'config error')

        header0 = 66
        header1 = 68

        a_frm_char = ">BB"
        values = []

        values.append(header0)
        values.append(header1)

        for col in frm:
            frm_char, length = self.check_length(col['length'])
            a_frm_char += frm_char
            type = col.get('type', None)
            if type == "bit":
                value = self.pack_bit(col, data, length)
                values.append(value)
            elif type == "loop":
                num = data.get(col['name'])
                values.append(num)

                for first in col['first_data']:
                    first_data = data['data'][0]
                    self.first_data = first_data
                    a_frm_char = self.pack_value(a_frm_char, first, first_data, values)

                for i in range(1, num):
                    for other in col['other_data']:
                        other_data = data['data'][i]
                        a_frm_char = self.pack_value(a_frm_char, other, other_data, values)
            else:
                values.append(data.get(col['name']))

        ret = struct.pack(a_frm_char, *values)
        ret += self.gen_check(ret[2:])
        ret = binascii.b2a_hex(ret)

        return ret

    def pack_value(self, a_frm_char, other, other_data, values):
        """
        拼装value
        :param a_frm_char:
        :param other:
        :param other_data:
        :param values:
        :return:
        """
        frm_char, length = self.check_length(other['length'])
        a_frm_char += frm_char
        type = other.get('type', None)
        if type == "bit":
            value = self.pack_bit(other, other_data, length)
        elif type == "subtract":
            value = other_data.get(other['name']) - self.first_data.get(other['name'])
        else:
            value = other_data.get(other['name'])
        values.append(value)
        return a_frm_char

    def pack_bit(self, col, data, length):
        """
        包装 bit
        :param col:
        :param data:
        :param length:
        :return:
        """
        sub_data = col.get('sub_data', None)
        value = ""
        for sub_d in sub_data:
            v = data[sub_d['name']]
            value += str(v)
        value = value[::-1]
        value = value.rjust(8 * length, '0')
        return int(value, base=2)

    def get_version_and_msg_type(self, data):
        """
        return protocal_version, cmd
        """
        msg = unpack_from(">B", data, offset=2)[0]
        protocal_version = get_byte_height_4(msg)
        cmd = get_byte_low_4(msg)
        return protocal_version, cmd

    def check_sn(self, data):
        """
        检查校验码
        return protocal_version, cmd
        """
        check_sum = data[-2:]
        check = self.gen_check(data[2:-2])
        return check == check_sum

    @exception_catch(ParseErrorException)
    def gen_check(self, data):
        """
        校验检查
        @param data:
        @return:
        """
        check_last_second_byte = 0
        check_last_byte = 0
        for i in range(len(data)):
            tmp_data = unpack_from('>B', data, offset=i)[0]
            check_last_second_byte = check_last_second_byte + tmp_data
            check_last_byte = check_last_second_byte + check_last_byte

        check_last_second_byte &= 0XFF
        check_last_byte &= 0XFF

        return struct.pack('>BB', check_last_second_byte, check_last_byte)

    @exception_catch(ParseErrorException)
    def unpack(self, data, frm, start_byte=0):
        """
        解析数据
        :param data: 二进制data, base64 decode后的
        :param frm: json 配置
        :return:
        """
        ret_json = {}
        start_byte = start_byte

        if not isinstance(frm, list):
            raise ConfigErrorException(500, 'config error')

        for col in frm:
            if start_byte >= (len(data) - 2):  # 处理只有一个位置点情况
                break
            frm_char, length = self.check_length(col['length'])
            value = self.parse(data, frm_char, length, start_byte)
            value = self.transfer_value(col, ret_json, value, length)

            start_byte += length
            ret_json.setdefault(col['name'], value)
        return ret_json

    @exception_catch(ParseErrorException)
    def transfer_value(self, col, ret_json, value, length):
        """
        按照类型转换value
        :param col:
        :param ret_json:
        :param value:
        :return:
        """
        if col['type'] == "timestamp":
            time_zone = col.get("time_zone", "utc")
            value = datetime_as_timezone(value, to_time_zone=time_zone)
        elif col['type'] == "bit":
            byte_l = bin(value)[2:]  # 去掉开头ob
            byte_l = byte_l.rjust(8 * length, '0')[::-1]  # 反转顺序为 低到高
            sub_data = col['sub_data']
            start = 0
            for sub in sub_data:
                end = start + sub['len']
                bit_value = byte_l[start:end][::-1]
                ret_json.setdefault(sub['name'], int(bit_value, 2))
                start += sub['len']
        elif col['type'] == "voltage":
            value = float(value) / 10
        elif col['type'] == "Coordinate":
            precision = col['precision']
            precision = float(10 ** precision)
            value /= precision
        return value

    @exception_catch(ParseErrorException)
    def parse(self, data, frm_char, length, start_byte):
        """
        按类型解析数据
        :param data:
        :param frm_char:
        :param length:
        :param start_byte:
        :return:
        """
        if frm_char == "pass":
            value = ""
        elif frm_char == "ch":
            end_byte = start_byte + length
            value = data[start_byte:end_byte].decode('UTF-8', 'ignore').strip(b'\x00'.decode())
        elif frm_char == "bcd":
            start = start_byte * 2
            end = start + length * 2 - 1
            value = binascii.b2a_hex(data)[start:end]
            if isinstance(value, bytes):
                value = value.decode('utf-8', 'ignore')
        elif frm_char == "ip":
            frm_char = ">B"
            ip1 = unpack_from(frm_char, data, offset=start_byte)[0]
            ip2 = unpack_from(frm_char, data, offset=start_byte + 1)[0]
            ip3 = unpack_from(frm_char, data, offset=start_byte + 2)[0]
            ip4 = unpack_from(frm_char, data, offset=start_byte + 3)[0]
            value = str(ip1) + '.' + str(ip2) + '.' + str(ip3) + '.' + str(ip4)
        else:
            frm_char = ">" + frm_char
            value = unpack_from(frm_char, data, offset=start_byte)[0]
        return value

    @exception_catch(ParseErrorException)
    def check_length(self, length):
        """
            根据length返回 解析类型和长度
        :param length:
        :return:
        """
        if length.startswith("U"):
            if int(length[1]) == 1:
                ret = "B", 1
            elif int(length[1]) == 2:
                ret = "H", 2
            elif int(length[1]) == 4:
                ret = "L", 4
            elif int(length[1]) == 8:
                ret = "Q", 8
            else:
                ret = "pass", int(length[1])
        elif length.startswith("I"):
            if int(length[1]) == 1:
                ret = "b", 1
            elif int(length[1]) == 2:
                ret = "h", 2
            elif int(length[1]) == 4:
                ret = "l", 4
            elif int(length[1]) == 8:
                ret = "q", 8
            else:
                ret = "pass", int(length[1])
        elif length.startswith("CH["):
            suffix = length.split("CH[")[1]
            ret = "ch", int(suffix.split("]")[0])
        elif length.startswith("BCD["):
            suffix = length.split("BCD[")[1]
            ret = "bcd", int(suffix.split("]")[0])
        elif length.startswith("ip"):
            ret = "ip", 4
        else:
            ret = "pass", int(length[1])
        return ret


iot_struct = IOTStruct()

if __name__ == '__main__':
    data = 'QkQCAClfx8i8/6wAAAgA0NEAAA8AAUYBEwE1gpRQAQFfx8i7ADUjTgCtRl0AB9eT'
    frm = [{"name": "head", "length": "U2", "type": "constant"},
           {"name": "msg_type", "length": "U1", "type": "bit", "sub_data": [
               {"name": "cmd", "len": 4}, {"name": "protocal_version", "len": 4}
           ]},
           {"name": "length", "length": "U2", "type": "constant"},
           {"name": "timestamp", "length": "U4", "type": "timestamp", "time_zone": "utc"},
           {"name": "retain1", "length": "I2", "type": "pass"},
           {"name": "retain2", "length": "U1", "type": "pass"},
           {"name": "retain3", "length": "U1", "type": "pass"},
           {"name": "retain4", "length": "U4", "type": "pass"},
           {"name": "retain5", "length": "U2", "type": "pass"},
           {"name": "retain6", "length": "I1", "type": "pass"},
           {"name": "terminal_version", "length": "U1", "type": "constant"},
           {"name": "elec", "length": "U1", "type": "bit",
            "sub_data": [
                {"name": "elec_alarm", "len": 1},
                {"name": "elec_vol", "len": 7}
            ]},
           {"name": "imsi", "length": "BCD[8]", "type": "constant"},
           {"name": "point_num", "length": "U1", "type": "constant"},
           {"name": "location_type", "length": "U1", "type": "bit",
            "sub_data": [
                {"name": "gps_location", "len": 1},
                {"name": "wifi_location", "len": 1}
            ]},
           {"name": "first_timestamp", "length": "U4", "type": "constant", "time_zone": "utc"},
           {"name": "lat", "length": "I4", "type": "Coordinate", "precision": 5},
           {"name": "lng", "length": "I4", "type": "Coordinate", "precision": 5},
           {"name": "speed", "length": "U1", "type": "constant"},
           {"name": "star_num", "length": "U1", "type": "constant"}
           ]

    iot_struct = IOTStruct()
    data = base64.b64decode(bytes(data, encoding="UTF8"))
    print(iot_struct.get_version_and_msg_type(data))
    ret_json = iot_struct.unpack(data, frm)
    print(ret_json)

    ret = iot_struct.check_sn(data)
    print(ret)
    # old_json = '{"terminal_low_power_alarm": "0", "duandian_alarm": "0", "wifi_lng": "113.5634475", "lat": 34.82468, "weiyi_alarm": "0", "lng": 113.55712, "speed": 0, "star_num": 18, "wendu_alarm": "0", "wifi_lat": "34.8233811", "location": "10000011", "location_type7": "1", "common_alarm": "00010000", "imsi": "460113015461466", "location_type8": "1", "timestamp": "2020-11-30 15:07:06", "terminal_version": 0, "terminal_temperature": 0, "protocal_version": 0, "zhendong_alarm": "0", "first_time_stamp": 1579258285, "location_type": 3, "terminal_vol": 100, "elec": "11001000", "cmd": 1, "wifi_num": 5, "length": 77, "points": [{"mac": "a8:a7:95:07:dc:7f", "intensity": "52"}, {"mac": "b0:95:8e:bb:28:3a", "intensity": "52"}, {"mac": "00:03:0f:10:67:3a", "intensity": "64"}, {"mac": "00:03:0f:10:09:27", "intensity": "66"}, {"mac": "b4:86:55:7f:e0:31", "intensity": "76"}], "chaichu_alarm": "1"}'
    # compare(old_json, json.dumps(ret_json))

    frm = [{"name": "msg_type", "length": "U1", "type": "constant"},
           {"name": "length", "length": "U2", "type": "constant"},
           {"name": "mid", "length": "U2", "type": "constant"},
           {"name": "num", "length": "U1", "type": "loop",
            "first_data": [
                {"name": "latitude", "length": "U4", "type": "constant"},
                {"name": "longitude", "length": "U4", "type": "constant"},
                {"name": "radius", "length": "U2", "type": "constant"},
                {"name": "fence_config", "length": "U1", "type": "bit",
                 "sub_data": [
                     {"name": "out_fence_alarm", "len": 1},
                     {"name": "enter_fence_alarm", "len": 1}
                 ]}],
            "other_data": [
                {"name": "latitude", "length": "I2", "type": "subtract"},
                {"name": "longitude", "length": "I2", "type": "subtract"},
                {"name": "radius", "length": "U2", "type": "constant"},
                {"name": "fence_config", "length": "U1", "type": "bit",
                 "sub_data": [
                     {"name": "out_fence_alarm", "len": 1},
                     {"name": "enter_fence_alarm", "len": 1}
                 ]}]
            }
           ]

    data = dict(
        msg_type=41,
        length=21,
        mid=14,
        num=2,
        data=[{"latitude": 3699999, "longitude": 11388889, "radius": 2, "out_fence_alarm": 1, "enter_fence_alarm": 1},
              {"latitude": 3699999, "longitude": 11388889, "radius": 2, "out_fence_alarm": 1, "enter_fence_alarm": 1}]
    )

    old = '4244290015000e020038751f00adc7d900020300000000000203715e'


    ret = iot_struct.pack(data, frm)
    print(old)

    print(ret)
