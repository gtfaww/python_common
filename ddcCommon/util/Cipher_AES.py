# -*- coding: utf-8 -*-
# @Author：huanghailong
# @File：Cipher_AES.py
# @Version：1.0
# @Time：2020/6/12 15:00
'''
Created on 2018年7月7日
@author: ray
'''

import Crypto.Cipher.AES
import Crypto.Random
import base64
import binascii


class Cipher_AES:
    cipher = getattr(Crypto.Cipher, "AES")
    pad = {"default": lambda x, y: x + (y - len(x) % y) * " ".encode("utf-8"),
           "PKCS5Padding": lambda x, y: x + (y - len(x) % y) * chr(y - len(x) % y).encode("utf-8")}
    unpad = {"default": lambda x: x.rstrip(),
             "PKCS5Padding": lambda x: x[:-ord(x[-1])]}
    encode = {"base64": base64.encodebytes,
              "hex": binascii.b2a_hex}
    decode = {"base64": base64.decodebytes,
              "hex": binascii.a2b_hex}

    def __init__(self, key=None, iv=None, cipher_method=None, pad_method="default", code_method=None):
        self.__key = key if key else "abcdefgh12345678"  # 密钥（长度必须为16、24、32）
        self.__iv = iv if iv else Crypto.Random.new().read(Cipher_AES.cipher.block_size)  # 向量（长度与密钥一致，ECB模式不需要）
        self.__cipher_method = cipher_method.upper() if cipher_method and isinstance(cipher_method,
                                                                                     str) else "MODE_ECB"  # 加密方式，["MODE_ECB"|"MODE_CBC"|"MODE_CFB"]等
        self.__pad_method = pad_method  # 填充方式，解决 Java 问题选用"PKCS5Padding"
        self.__code_method = code_method  # 编码方式，目前只有"base64"、"hex"两种
        if self.__cipher_method == "MODE_CBC":
            self.__cipher = Cipher_AES.cipher.new(self.__key.encode("utf-8"), Cipher_AES.cipher.MODE_CBC,
                                                  self.__iv.encode("utf-8"))
        else:
            self.__cipher = Cipher_AES.cipher.new(self.__key.encode("utf-8"), Cipher_AES.cipher.MODE_ECB)

    def __getitem__(self, item):
        def get3value(item):
            return item.start, item.stop, item.step

        type_, method, _ = get3value(item)
        dict_ = getattr(Cipher_AES, type_)
        return dict_[method] if method in dict_ else dict_["default"]

    def encrypt(self, text):
        cipher_text = b"".join([self.__cipher.encrypt(i) for i in self.text_verify(text.encode("utf-8"))])
        encode_func = Cipher_AES.encode.get(self.__code_method)
        if encode_func:
            cipher_text = encode_func(cipher_text)
        return cipher_text.decode("utf-8").rstrip()

    def decrypt(self, cipher_text):
        cipher_text = cipher_text.encode("utf-8")
        decode_func = Cipher_AES.decode.get(self.__code_method)
        if decode_func:
            cipher_text = decode_func(cipher_text)
        return self.pad_or_unpad("unpad", self.__cipher.decrypt(cipher_text).decode("utf-8"))

    def text_verify(self, text):
        while len(text) > len(self.__key):
            text_slice = text[:len(self.__key)]
            text = text[len(self.__key):]
            yield text_slice
        else:
            if len(text) == len(self.__key):
                yield text
            else:
                yield self.pad_or_unpad("pad", text)

    def pad_or_unpad(self, type_, contents):
        lambda_func = self[type_: self.__pad_method]
        return lambda_func(contents, len(self.__key)) if type_ == "pad" else lambda_func(contents)
