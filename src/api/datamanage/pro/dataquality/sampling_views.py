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


from datamanage.pro.dataquality.mixins.base_mixins import BaseMixin
from datamanage.pro.dataquality.serializers.sampling import DataSetSamplingSerializer
from datamanage.utils.dbtools.influx_util import influx_query
from django.core.cache import cache
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.views import APIViewSet


class DataSetSamplingViewSet(BaseMixin, APIViewSet):
    @list_route(methods=["get"], url_path="result_tables")
    @params_valid(serializer=DataSetSamplingSerializer)
    def result_tables(self, request, params):
        heat_score = params.get("heat_score")
        heat_rate = params.get("heat_rate")
        recent_data_time = params.get("recent_data_time")

        datasets = set()
        # 按最近多长时间有数据对数据集进行过滤
        if recent_data_time:
            sql = """
                SELECT SUM(data_inc) as output_count FROM data_loss_output_total
                WHERE storage_cluster_type = 'kafka' and time > now() - {recent_data_time}s
                GROUP BY data_set_id, storage_cluster_type
            """.format(
                recent_data_time=recent_data_time
            )
            results = influx_query(sql, db="monitor_data_metrics", is_dict=True)
            for item in results:
                if item["output_count"] > 0:
                    try:
                        # dataid的数据集不进行采样
                        int(item["data_set_id"])
                    except ValueError:
                        datasets.add(item["data_set_id"])

        if heat_rate is not None and heat_score is not None:
            dataset_stocktake_metrics = cache.get("data_value_stocktake")
            dataset_stocktake_metrics = [x for x in dataset_stocktake_metrics if x["dataset_type"] == "result_table"]
            dataset_stocktake_metrics.sort(key=lambda x: -x.get("heat_score", 0))

            # 获取热度分阈值index
            if heat_score:
                score_index = self.find_heat_score_index(
                    heat_score,
                    dataset_stocktake_metrics,
                    0,
                    len(dataset_stocktake_metrics) - 1,
                )
            else:
                score_index = len(dataset_stocktake_metrics)

            # 获取热度比例index
            if heat_rate:
                rate_index = len(dataset_stocktake_metrics) * heat_rate // 100
            else:
                rate_index = len(dataset_stocktake_metrics)

            # 两个index取最小那个以使两个条件都满足
            dataset_stocktake_metrics = dataset_stocktake_metrics[0 : min(score_index, rate_index)]

            tmp_datasets = set()
            for item in dataset_stocktake_metrics:
                if item["dataset_id"] in datasets:
                    tmp_datasets.add(item["dataset_id"])
            datasets = tmp_datasets

        return Response(list(datasets))

    def find_heat_score_index(self, heat_score, ordered_metrics, start_index, end_index):
        if start_index == end_index:
            return start_index
        mid_index = (start_index + end_index) // 2 + 1
        if heat_score > ordered_metrics[mid_index]["heat_score"]:
            return self.find_heat_score_index(heat_score, ordered_metrics, 0, mid_index - 1)
        else:
            return self.find_heat_score_index(heat_score, ordered_metrics, mid_index, end_index)
