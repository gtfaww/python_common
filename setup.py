# -*- coding: utf-8 -*-
# 19-6-27
# create by: guotengfei

from setuptools import setup

kwargs = {}

setup(
    name='ddcCommon',
    version='1.0.2',
    packages=['ddcCommon', 'ddcCommon.rabbitMQ'],
    install_requires=[
        'pika>=1.1.0'
    ],
    # scripts=[],
    # install_requires=[],
    # package_data={
    #     # If any package contains *.txt or *.rst files, include them:
    #     '': ['*.md', '*.rst'],
    #     # And include any *.msg files found in the 'hello' package, too:
    #     # 'hello': ['*.msg'],
    # },
    author='guotengfei',
    author_email='guotengfei@zzvcom.com',
    url='https://github.com/gtfaww/python_common.git',
    license='BSD License',
    keywords=[
        "ddcCommon", "rabbitMQ"
    ],
    description='VCOM移动物联网电动车项目公共模块',
    long_description=open("README.rst").read(),
    classifiers=['Development Status :: 1 - Production/Stable',
                 'Intended Audience :: Developers',
                 'License :: OSI Approved :: BSD License',
                 'Operating System :: POSIX',
                 'Operating System :: Microsoft :: Windows',
                 'Operating System :: MacOS :: MacOS X',
                 'Topic :: Software Development :: Testing',
                 'Topic :: Software Development :: Libraries',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2.6',
                 'Programming Language :: Python :: 2.7'
                 ],
    **kwargs
)
