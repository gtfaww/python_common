"""
python AES256 ECB pkcs7
pip install pycryptodome

"""

import base64

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


def aes_encrypt(key, aes_str):
    # 使用key,选择加密方式
    aes = AES.new(key, AES.MODE_ECB)
    pad_pkcs7 = pad(aes_str.encode('utf-8'), AES.block_size, style='pkcs7')  # 选择pkcs7补全
    encrypt_aes = aes.encrypt(pad_pkcs7)

    # 加密结果
    # encrypted_text = str(encrypt_aes, encoding='utf-8')  # 解码
    # encrypted_text_str = str(encrypted_text).replace("\n", "")
    # 此处我的输出结果老有换行符，所以用了临时方法将它剔除

    return encrypt_aes


def aes_dncrypt(key, encrypt_str):
    # 使用key,选择加密方式
    aes = AES.new(key, AES.MODE_ECB)
    # encrypt_str = base64.decodebytes(encrypt_str.encode('utf-8'))
    dencrypt_str = aes.decrypt(encrypt_str.encode('utf-8'))
    # print(dencrypt_str.decode())
    unpad_pkcs7 = unpad(dencrypt_str, AES.block_size, style='pkcs7')  # 选择pkcs7补全

    return unpad_pkcs7


if __name__ == '__main__':
    # key的长度需要补长(16倍数),补全方式根据情况而定,此处我就手动以‘0’的方式补全的32位key
    # key字符长度决定加密结果,长度16：加密结果AES(128),长度32：结果就是AES(256)
    # key = "cee60757342c441997b2e5ed54c4028d"
    # # 加密字符串长同样需要16倍数：需注意,不过代码中pad()方法里，帮助实现了补全（补全方式就是pkcs7）
    # aes_str = "Hello World"
    # encryption_result = aes_encrypt(key, aes_str)
    # print(encryption_result)
    #
    # dncrypt_result = aes_dncrypt(key, 'hWfua/xA9rRWOQ4wEjL0fw==')
    # print(dncrypt_result)

    key = b"AES_KEY"
    #
    print(key)
    finalKey = [0 for _ in range(16)]
    print(finalKey)
    i = 0
    for b in key:
        print(b)

    for b in key:
        finalKey[i % 16] ^= b
        i += 1
    print(len(finalKey))

    # key = binascii.b2a_hex(bytes(finalKey[:8]))
    # print(len(key))

    abc = "[{\"parentType\":\"parent\",\"familyname\":\"aa\",\"familyno\":\"15503030000\",\"displayorder\":1},{\"parentType\":\"parent2\",\"familyname\":\"aaa\",\"familyno\":\"15503030001\",\"displayorder\":2},{\"parentType\":\"parent3\",\"familyname\":\"a1\",\"familyno\":\"15503030002\",\"displayorder\":3},{\"parentType\":\"dad\",\"familyname\":\"a2\",\"familyno\":\"15503030003\",\"displayorder\":4},{\"parentType\":\"mum\",\"familyname\":\"a3\",\"familyno\":\"15503030004\",\"displayorder\":5},{\"parentType\":\"grandad\",\"familyname\":\"a4\",\"familyno\":\"15503030005\",\"displayorder\":6},{\"parentType\":\"granny\",\"familyname\":\"a5\",\"familyno\":\"15503030006\",\"displayorder\":7},{\"parentType\":\"grandma\",\"familyname\":\"a6\",\"familyno\":\"15503030007\",\"displayorder\":8},{\"parentType\":\"grandpa\",\"familyname\":\"a7\",\"familyno\":\"15503030008\",\"displayorder\":9},{\"parentType\":\"keeper\",\"familyname\":\"a8\",\"familyno\":\"15503030009\",\"displayorder\":10}]"

    encryption_result = aes_encrypt(bytearray(finalKey), abc)
    print(encryption_result)

    s = "10dbb67c06c9e598be29610de25c8d11ae3d7052c03041e4e56b6a64e23a6f37cbeda6ebf068eaa2561b42bd1ff4c3f25c3167bcd1e7f1b5bc9e5dc6cb817aad8dd851ee65f25a8b9fd0970af8c16d31c25433a7f94cd67516ca55097140055a2c947cbd80b67e970f96816be53f6ee2374a5878e43e662b6057b333c105e9051083b7ded23e477d8ba3743f0bdc8dfe7444c1c341538bc900e5c96274d496bb22b93a370e9a2fe07696f4ff3733fc20bd469ab10810bb47fdb1d8e2e283e80fd02f80ef9b011b8c62fc2bcd33102bb8f56be98969ad169e3051ba1892157ef8b439a5beb03d5b1ad2e00cf0743380db77f17732a8a1c9c265ca068b8f1849a86b70ab85f3b147586ffd05a3750a9999660d4e7db1a58b23290250ad4e73dac992301f63dcdafd59969ca7e25c1eaac7574b6e1f68411986d83526d3045b75b7d0c6310b07fd6bd2d6b6ed72fcabcbc4014c09357c2421d095ded8f65b6446e0dbf379e20fca28729d4fc843618b1b5a0891c56cac72c28aa48e5b963b67019bf9e323696a59205d677947a71a8858e7faa944f5044b36cb35afba434b7e3a3c9c4bef7cd335ea7b87422e3126c3080b7412f7f5c4026f4fb1842a2f1b755c908fe6f5ca98258ea7e7d1a82cba4b7f24a37490348f3fbcbfc9dca906f79cdd7bc750cde5c2a005ca716f922e2b74392609df8ce5fdc50beb7beded15ab8ec2830712fd7c952d9e7ca5bb7ca38b4a8d261228ab9b73a0737421c13c3f90edcfc51083b7ded23e477d8ba3743f0bdc8dfe4811d3fdab650924baa7967b151752f21ab3b7804418b3bd8606c7cfe4842eca23d015ac5f5fdd2ff7f2393316d0164fd1303c715d82503600984be9a5498abaedd258043f62bafd893035d3a2f4fee720feee818d7a8ccd36f03c09ee43afbc8a1078d5b4b7afb050f423cfdadc4011f7abd2501fb94c169f5609aacab467219ebdbff3f1c59feb9aba05481096f76b1514d597e5804b7dec6bf6e86cdc852a5c3167bcd1e7f1b5bc9e5dc6cb817aad92adf070d4114aab5544e6f8407d1222ed49f1873d0309a281207e1a8da654a83cdf615573d0aef105aaa4125122ccfaa337783ef1168ad8f49b59628c3c59f1f4872e38a0d7df40fe90c5437e6e546fd5c3ffa6c4ab714485166110105b54df8604a0ede1f485e7ca4bf5b73053a955"

    dncrypt = aes_dncrypt(bytearray(finalKey),s)
    print(dncrypt)