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
import json

from common.log import logger
from datahub.access.utils.forms import DateTimeEncoder
from kafka import KafkaProducer


def send_message_to_kafka(kafka_bs, topic, msgs):
    """
    发送消息到指定的kafka上的topic中（默认为key/value都为空字符串的消息）。当topic不存在时，会自动创建topic
    :param kafka_bs:  kafka集群的地址
    :param topic: topic的名称
    :param msgs: 要发送的消息列表，包含key/value两个字段
    :return: 无
    """
    producer = KafkaProducer(
        client_id="accessapi_file_report",
        bootstrap_servers=[kafka_bs],
        acks="all",
        batch_size=1048576,
    )
    for msg in msgs:
        producer.send(topic, json.dumps(msg, cls=DateTimeEncoder))
    logger.info(u"sending msgs to {} on {}!".format(topic, kafka_bs))
    producer.close(3)
