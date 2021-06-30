# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from pyspark import SparkConf, SparkContext
from pyspark.java_gateway import launch_gateway
from pyspark.sql import SparkSession

gateway = launch_gateway()
jconf = gateway.jvm.SparkConf(True)


def get_spark_conf():
    return SparkConf(_jvm=gateway.jvm, _jconf=jconf)


def get_spark_logger():
    log4j_logger = gateway.jvm.org.apache.log4j.Logger
    return log4j_logger


def launch_spark_session(conf=None):
    sc = SparkContext(gateway=gateway, conf=conf)
    return SparkSession(sc)


def set_log_level(level_param):
    level = gateway.jvm.org.apache.log4j.Level.toLevel(level_param.upper())
    gateway.jvm.org.apache.log4j.Logger.getRootLogger().setLevel(level)


def set_file_appender(path):
    fa = gateway.jvm.org.apache.log4j.FileAppender()
    fa.setName("FILE_APPENDER")
    fa.setFile(path)
    pattern = gateway.jvm.org.apache.log4j.PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")
    fa.setLayout(pattern)
    fa.setThreshold(gateway.jvm.org.apache.log4j.Level.INFO)
    fa.setAppend(True)
    fa.activateOptions()
    logger = gateway.jvm.org.apache.log4j.Logger.getRootLogger()
    logger.addAppender(fa)
    return logger


URI = gateway.jvm.java.net.URI
Path = gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = gateway.jvm.org.apache.hadoop.conf.Configuration
