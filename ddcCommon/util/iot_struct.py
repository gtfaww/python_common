"""
IOT struct
"""

# @Author：guotengfei
# @Version：1.0
# @Time：2020/8/19
import base64
import binascii
from struct import unpack_from

from ddcCommon.util.util import convert_timestamp_to_str, get_bit_val, get_byte_height_4, get_byte_low_4, get_byte_low_7


class IOTStruct():
    """
    iot 数据处理
    """

    def __init__(self):
        pass

    def pack(self):
        pass

    def unpack(self, data, frm):
        """
        解析数据
        :param data:
        :param frm:
        :return:
        """
        ret_json = {}
        start_byte = 0
        data = base64.b64decode(bytes(data, encoding='UTF-8'))

        for col in frm:
            if start_byte >= (len(data) - 2):  # 处理只有一个位置点情况
                break
            frm_char, length = self.check_length(col['length'])
            value = self.parse(data, frm_char, length, start_byte)
            value = self.transfer_value(col, ret_json, value)

            start_byte += length
            ret_json.setdefault(col['name'], value)
        return ret_json

    def transfer_value(self, col, ret_json, value):
        """
        按照类型转换value
        :param col:
        :param ret_json:
        :param value:
        :return:
        """
        if col['type'] == "timestamp":
            value = convert_timestamp_to_str(value)
        elif col['type'] == "bit":
            sub_data = col['sub_data']
            for sub, index in zip(sub_data, range(len(sub_data))):
                bit_value = get_bit_val(value, index)
                ret_json.setdefault(sub['name'], bit_value)
        elif col['type'] == "bit3":
            sub_data = col['sub_data']
            ret_json.setdefault(sub_data[0]['name'], get_byte_low_4(value))
            ret_json.setdefault(sub_data[1]['name'], (value & 0x70) >> 4)
            ret_json.setdefault(sub_data[2]['name'], get_bit_val(value, 7))
        elif col['type'] == "bit4":
            sub_data = col['sub_data']
            ret_json.setdefault(sub_data[0]['name'], get_byte_height_4(value))
            ret_json.setdefault(sub_data[1]['name'], get_byte_low_4(value))
        elif col['type'] == "bit7":
            sub_data = col['sub_data']
            ret_json.setdefault(sub_data[0]['name'], get_byte_low_7(value) * 2)
            ret_json.setdefault(sub_data[1]['name'], get_bit_val(value, 7))
        elif col['type'] == "voltage":
            value = float(value) / 10
        elif col['type'] == "Coordinate":
            value /= 1000000.0
        return value

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


if __name__ == '__main__':
    # 实时追踪
    data = 'QkRUAAoAADJezRAxAAAgAAAAAwACFQAAWf+GhiYEAWYQMEYBEzNDeEcgAQBezRAxAhNhfAbEv08AXa6z'
    frm = [{"name": "head", "length": "U2", "type": "constant"},
           {"name": "msg_type", "length": "U1", "type": "bit4", "sub_data": [
               {"name": "protocal_version"}, {"name": "cmd"},
           ]},
           {"name": "serial_num", "length": "U2", "type": "constant"},
           {"name": "attribute", "length": "U1", "type": "constant"},
           {"name": "length", "length": "U2", "type": "constant"},
           {"name": "utc_time", "length": "U4", "type": "timestamp"},
           {"name": "signal_strength", "length": "I2", "type": "constant"},
           {"name": "terminal_temperature", "length": "U1", "type": "constant"},
           {"name": "retain1", "length": "U1", "type": "pass"},
           {"name": "state_config", "length": "U4", "type": "bit", "sub_data": [
               {"name": "switch"}, {"name": "guard_against_theft"}, {"name": "rear_wheel_lock"},
               {"name": "backseat_lock"}, {"name": "floodlight"}, {"name": "battery_compartments"},
               {"name": "rear_wheel"}, {"name": "mobile"}, {"name": "positioning_way"},
               {"name": "battery_status"},
           ]},
           {"name": "terminal_voltage", "length": "U2", "type": "voltage"},
           {"name": "retain2", "length": "I1", "type": "pass"},
           {"name": "terminal_version", "length": "U1", "type": "constant"},
           {"name": "terminal_vol", "length": "U1", "type": "constant"},
           {"name": "retain3", "length": "U1", "type": "pass"},
           {"name": "imei", "length": "BCD[8]", "type": "constant"},
           {"name": "imsi", "length": "BCD[8]", "type": "constant"},
           {"name": "location", "length": "U1", "type": "bit", "sub_data": [
               {"name": "location_type8"}
           ]},
           {"name": "acc", "length": "U1", "type": "bit", "sub_data": [
               {"name": "dynamic_static_logo"},{"name": "acc_status"}
           ]},
           {"name": "first_time_stamp", "length": "U4", "type": "timestamp"},
           {"name": "first_point_lat", "length": "I4", "type": "Coordinate"},
           {"name": "first_point_lng", "length": "I4", "type": "Coordinate"},
           {"name": "first_point_speed", "length": "I1", "type": "constant"},
           {"name": "satellite_direction", "length": "U1", "type": "bit4", "sub_data": [
               {"name": "first_point_direction"}, {"name": "first_point_star_num"}
           ]}
           ]

    struct = IOTStruct()
    ret_json = struct.unpack(data, frm)
    print(ret_json)
