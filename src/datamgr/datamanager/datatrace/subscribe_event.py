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
from __future__ import absolute_import, print_function, unicode_literals

import logging
import time
import traceback

from common.exceptions import EsBulkDataError
from datatrace.data_trace_elasticsearch import DataTraceElasticSearchClient
from datatrace.handlers.data_trace_handler import DataTraceHandler
from datatrace.parser import parse_data_trace_event
from datatrace.subscriber import DataTraceSubscriber

logger = logging.getLogger(__name__)


def handle_event_content_and_report_es(body, message):
    """
    足迹处理方法(数据足迹事件订阅的回调事件)

    :param body: 消息实体 for example:
        {
            "dispatch_id": "12602147-101e-4e28-a4f2-8bd4dd0ba96a",
            "event_name": "EventUpdateAttr",
            "triggered_by": "admin",
            "msg_id": "fec1f943-dcc2-4696-9a0a-21da1a4babe0",
            "event_capture_listening_type": "on_update",
            "event_message": "{}"
             "triggered_at": "2021-04-13 15:38:42",
             "event_type": "mutation"
        }
    :type body: bytes
    :param message: 消息实例
    :return:
    """
    try:
        # 1) 对订阅到的足迹事件内容进行处理
        changed_model, data_trace_info_dict = parse_data_trace_event(body)
        if changed_model is None:
            return

        handler = DataTraceHandler(changed_model, data_trace_info_dict)
        data_trace_event_list = handler.get_data_trace_event_list()

        # 2) 事件上报es
        if not data_trace_event_list:
            return
        es_client = DataTraceElasticSearchClient()
        try:
            es_client.bulk_data(data_trace_event_list)
        except EsBulkDataError as e:
            logger.error(
                f"report data_trace event to es fail, event content: {data_trace_event_list} for: {e.message}"
            )
    except Exception as e:
        logger.error(
            f"handle data trace info and report corresponding event to es fail, for:{e},trace:{traceback.format_exc()}"
        )
        time.sleep(1)
    finally:
        message.ack()


def subscribe_data_trace_event():
    """订阅数据足迹事件

    :return:
    """
    data_trace_subscriber = DataTraceSubscriber()
    data_trace_subscriber.set_callback(callback=handle_event_content_and_report_es)
    try:
        data_trace_subscriber.subscribe()
    except Exception as e:
        logger.info(f"subscribe_data_trace_event error, for:{e}")
