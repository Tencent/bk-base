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

from common.log import logger
from datahub.databus.exceptions import RunKafkaCmdErr
from datahub.databus.settings import PULSAR_OUTER_NAMESPACE, PULSAR_OUTER_TENANT
from datahub.databus.task.pulsar import topic as pulsar_topic_tool  # noqa
from datahub.databus.utils import pulsar_tool


def tail(
    raw_data_id,
    rest_server,
    proxy_server,
    topic,
    partition,
    token,
    limit=10,
    tenant=PULSAR_OUTER_TENANT,
    namespace=PULSAR_OUTER_NAMESPACE,
):
    """
    查看最后几条数据
    :param raw_data_id: 原始数据ID
    :param rest_server: pulsar服务地址
    :param proxy_server: pulsar服务代理地址
    :param token: 鉴权token
    :param tenant: 租户
    :param namespace: 命名空间
    :param topic: topic
    :param partition: 分区
    :param limit: message条数
    :return: message列表
    """
    try:
        lines = pulsar_tool.get_messages(
            raw_data_id,
            rest_server,
            proxy_server,
            topic,
            limit,
            token,
            tenant,
            namespace,
        )
    except RunKafkaCmdErr:
        return []

    ret = list()
    try:
        for line in lines:
            if not line:
                continue
            ret.append(
                {
                    "partition": partition,
                    "value": line,
                    "topic": topic,
                    "key": "",
                    "offset": 0,
                }
            )
    except Exception:
        logger.error(
            "pulsar tail output error, data={}".format(lines),
            exc_info=True,
        )
        return ret

    ret.reverse()
    return ret
