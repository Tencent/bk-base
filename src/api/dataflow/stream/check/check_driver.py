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

from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.stream.handlers import db_helper
from dataflow.stream.utils.checkpoint_manager import CheckpointManager


def _check_redis():
    """
    根据不同地区获取 redis 并检查可用性
    @return:
    """
    component_status = True
    try:
        geog_area_codes = TagHelper.list_tag_geog()
        message = {}
        for geog_area_code in geog_area_codes:
            try:
                checkpoint_manager = CheckpointManager(geog_area_code)
                if not checkpoint_manager.enable_sentinel:
                    connection = db_helper.RedisConnection(
                        checkpoint_manager.host,
                        checkpoint_manager.port,
                        checkpoint_manager.password,
                        10,
                    )
                else:
                    connection = db_helper.RedisSentinelConnection(
                        checkpoint_manager.host_sentinel,
                        checkpoint_manager.port_sentinel,
                        checkpoint_manager.name_sentinel,
                    )
                status = connection.ping()
                _message = {
                    "message": "ok" if status else "unavailable",
                    "status": status,
                    "enable_sentinel": checkpoint_manager.enable_sentinel,
                }
            except Exception as e:
                component_status = False
                _message = {"message": "{}".format(e), "status": False}
            message[geog_area_code] = _message
    except Exception as e:
        component_status = False
        message = "{}".format(e)
    return component_status, message


def __component_check():
    """
    组件 check
    """

    def format_rtn(status, message):
        return {"status": status, "message": message}

    rtn = {"redis": format_rtn(*_check_redis())}
    return rtn


def check():
    rtn = {}
    rtn.update(__component_check())
    return rtn
