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

import json
import logging
from datetime import datetime

from utils.time import datetime_to_str

logger = logging.getLogger(__name__)


def parse_data_trace_event(body):
    """解析订阅到的数据足迹body

    :param body: 订阅到的数据足迹body
    :return:
    """
    logger.info(f"parse_data_trace_event, body:{body}")
    body_dict = json.loads(body)
    message_dict = json.loads(body_dict.get("event_message", "{}"))

    # 变更的dgraph model
    changed_model = message_dict.get("md_name", None)
    if changed_model is None:
        return None, None

    data_trace_info_dict = dict(
        event_id=body_dict.get("msg_id", ""),
        dispatch_id=message_dict.get("dispatch_id", ""),
        event_name=message_dict.get("event_name", ""),
        changed_md=changed_model,
        identifier_key=message_dict.get("identifier_key", ""),
        identifier_value=message_dict.get("identifier_value", ""),
        differ=message_dict.get("differ", {}),
        origin_data=message_dict.get("origin_data", {}),
        changed_data=message_dict.get("changed_data", {}),
        created_by=message_dict.get("triggered_by", None),
        created_at=datetime_to_str(datetime.now()),
    )
    return changed_model, data_trace_info_dict
