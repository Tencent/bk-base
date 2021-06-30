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

from datalab.constants import (
    CREATED_AT,
    DATALAB_ES_QUERY_HISTORY,
    ES_QUERY,
    SEARCH_RANGE_END_TIME,
    SEARCH_RANGE_START_TIME,
    TIME,
)
from datalab.es_query.time_handler import strftime_local
from django.db import models
from django.forms import model_to_dict


class DatalabEsQueryHistory(models.Model):
    id = models.AutoField(primary_key=True)
    result_table_id = models.CharField(max_length=256)
    search_range_start_time = models.DateTimeField(null=True)
    search_range_end_time = models.DateTimeField(null=True)
    keyword = models.TextField(null=True)
    time_taken = models.CharField(max_length=32, default=0)
    total = models.IntegerField(default=0)
    result = models.BooleanField(default=True)
    err_msg = models.TextField(null=True)
    active = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.CharField(max_length=128, default="")
    updated_at = models.DateTimeField(blank=True, null=True)
    updated_by = models.CharField(max_length=128, default="")
    description = models.TextField(default="")

    class Meta:
        managed = False
        db_table = DATALAB_ES_QUERY_HISTORY
        app_label = ES_QUERY

    @classmethod
    def list_query_history(cls, operator, rt_id):
        records = cls.objects.filter(
            created_by=operator,
            result_table_id=rt_id,
        ).order_by("-%s" % CREATED_AT)

        return_result = []
        for record in records:
            if record.keyword:
                history_dict = model_to_dict(record)
                history_dict.update(
                    {
                        TIME: strftime_local(record.created_at),
                        SEARCH_RANGE_START_TIME: strftime_local(record.search_range_start_time),
                        SEARCH_RANGE_END_TIME: strftime_local(record.search_range_end_time),
                    }
                )
                return_result.append(history_dict)
        return return_result
