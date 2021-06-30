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

from common.business import APIModel
from common.local import get_request_username
from common.log import sys_logger
from datalab.constants import (
    END_TIME,
    KEYWORD,
    RESULT_TABLE_ID,
    START_TIME,
    TIME_TAKEN,
    TOTAL,
)
from datalab.es_query.models import DatalabEsQueryHistory

try:
    from functools import wraps
except ImportError:
    from django.utils.functional import wraps


class ResultTable(APIModel):
    @staticmethod
    def save_es_query_history():
        """
        记录es查询历史装饰器
        """

        def _wrap(func):
            @wraps(func)
            def _deco(self, request, *args, **kwargs):
                param = request.data
                record = DatalabEsQueryHistory()
                record.result_table_id = kwargs.get(RESULT_TABLE_ID)
                record.created_by = get_request_username()
                try:
                    _f = func(self, request, *args, **kwargs)
                    record.result = True
                    record.err_msg = None
                except Exception as e:
                    record.result = False
                    record.err_msg = str(e)
                    raise e
                else:
                    record.time_taken = _f.data.get(TIME_TAKEN)
                    record.total = _f.data.get(TOTAL)
                    return _f
                finally:
                    try:
                        record.keyword = param.get(KEYWORD)
                        # 无关键字时不记录
                        if record.keyword:
                            record.search_range_start_time = param.get(START_TIME)
                            record.search_range_end_time = param.get(END_TIME)
                            record.save()
                    except Exception as e:
                        sys_logger.error(
                            "save_es_query_history_error|ex_name:{}|message:{}".format(type(e).__name__, str(e))
                        )

            return _deco

        return _wrap
