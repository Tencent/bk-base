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

from django.db.models import Sum

from dataflow.models import DebugErrorLog, DebugMetricLog, DebugResultDataLog


def get_debug_metric_log(**kwargs):
    return (
        DebugMetricLog.objects.filter(**kwargs)
        .values("processing_id")
        .annotate(
            input_total_count=Sum("input_total_count"),
            output_total_count=Sum("output_total_count"),
            filter_discard_count=Sum("filter_discard_count"),
            transformer_discard_count=Sum("transformer_discard_count"),
            aggregator_discard_count=Sum("aggregator_discard_count"),
        )
    )


def get_error_log(**kwargs):
    return list(DebugErrorLog.objects.filter(**kwargs).values())


def get_debug_metric_log_for_processing(**kwargs):
    return DebugMetricLog.objects.filter(**kwargs).aggregate(
        input_total_count=Sum("input_total_count"),
        output_total_count=Sum("output_total_count"),
        filter_discard_count=Sum("filter_discard_count"),
        transformer_discard_count=Sum("transformer_discard_count"),
        aggregator_discard_count=Sum("aggregator_discard_count"),
    )


def get_debug_error_log_for_processing(**kwargs):
    return list(DebugErrorLog.objects.filter(**kwargs).values())


def get_result_data_for_processing(limit_num, **kwargs):
    return DebugResultDataLog.objects.filter(**kwargs).order_by("-debug_date")[:limit_num]


def insert_debug_metric_log(**kwargs):
    DebugMetricLog(**kwargs).save()


def update_debug_metric_log_end_time(debug_end_at, **kwargs):
    DebugMetricLog.objects.filter(**kwargs).update(debug_end_at=debug_end_at)


def insert_debug_error_log(**kwargs):
    DebugErrorLog(**kwargs).save()


def update_debug_metric_log(debug_id, job_id, result_table_id, **kwargs):
    DebugMetricLog.objects.filter(debug_id=debug_id, job_id=job_id, processing_id=result_table_id).update(**kwargs)


def insert_debug_result_data_log(**kwargs):
    DebugResultDataLog(**kwargs).save()


def delete_debug_metric_log(assign_time):
    DebugMetricLog.objects.filter(debug_start_at__lte=assign_time).delete()


def delete_debug_error_log(assign_time):
    DebugErrorLog.objects.filter(debug_date__lte=assign_time).delete()


def delete_debug_result_data_log(assign_time):
    DebugResultDataLog.objects.filter(thedate__lte=assign_time).delete()


def filter_debug_metric_log(**kwargs):
    return DebugMetricLog.objects.filter(**kwargs)
