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
from urllib.parse import quote

import requests
from common.log import logger

# from urllib import quote #TODO
from datahub.databus.settings import (
    PULSAR_OUTER_NAMESPACE,
    PULSAR_OUTER_TENANT,
    PULSAR_READER_URL,
    SUCCESS_CODE,
)


def get_messages(
    raw_data_id,
    rest_server,
    proxy_server,
    topic,
    num_messages,
    token,
    tenant=PULSAR_OUTER_TENANT,
    namespace=PULSAR_OUTER_NAMESPACE,
):
    """
    获取消息
    :param raw_data_id: 原始数据ID
    :param namespace: 命名空间
    :param tenant: 租户
    :param rest_server: broker web url restFul
    :param proxy_server: proxy url for client
    :param topic: topic. like pulsar_test123
    :param num_messages: message numbers
    :param token: pulsar connect token
    """
    # build pulsar client
    pulsar_topic_name = (u"persistent://{}/{}/{}".format(tenant, namespace, topic)).encode("utf-8")

    tail_url = PULSAR_READER_URL + "?dataId=%s&topic=%s&serviceUrl=%s&proxyServerUrl=%s&token=%s&messageNum=%d" % (
        str(raw_data_id),
        quote(pulsar_topic_name),
        quote("http://" + rest_server),
        quote(proxy_server),
        token,
        num_messages,
    )
    logger.debug("request pulsar tail url:" + tail_url)
    res = requests.get(tail_url)
    if res.status_code not in [200, 204]:
        logger.error(
            "request pulsar tail url failed, status: %s, text: %s",
            (res.status_code, res.text),
        )

    output = json.loads(res.text)
    if output["code"] != SUCCESS_CODE:
        logger.error("get pulsar tail error, url: {}, exception:{} ".format(tail_url, output["errors"]))

    # 每条结果以PULSAR_READER_SPLIT_WORD 特殊字符划分
    return output["data"]
