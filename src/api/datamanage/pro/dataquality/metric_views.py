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


from django_filters.rest_framework import DjangoFilterBackend

from common.views import APIModelViewSet

from datamanage.utils.drf import DataPageNumberPagination
from datamanage.pro.dataquality.mixins.base_mixins import BaseMixin
from datamanage.pro.dataquality.models.metric import DataQualityMetric
from datamanage.pro.dataquality.serializers.metric import DataQualityMetricSerializer


class DataQualityMetricsViewSet(BaseMixin, APIModelViewSet):
    model = DataQualityMetric
    lookup_field = model._meta.pk.name
    filter_backends = (DjangoFilterBackend,)
    pagination_class = DataPageNumberPagination
    serializer_class = DataQualityMetricSerializer
    ordering_fields = ('id', 'created_at')
    ordering = ('-id',)

    def get_queryset(self):
        return self.model.objects.filter(active=True)

    def list(self, request):
        """
        @api {get} /datamanage/dataquality/metrics/ 数据质量指标列表

        @apiVersion 3.5.0
        @apiGroup DataQualityMetric
        @apiName dataquality_metrics_list
        @apiDescription 数据质量指标列表

        @apiSuccess (200) {Number} data.metric_name 质量指标名
        @apiSuccess (200) {String} data.metric_alias 质量指标别名
        @apiSuccess (200) {String} data.metric_type 质量指标类型
        @apiSuccess (200) {String} data.metric_unit 质量指标单位
        @apiSuccess (200) {String} data.sensitivity 质量指标敏感度
        @apiSuccess (200) {String} data.description 质量指标描述
        @apiSuccess (200) {String} data.created_by 创建人
        @apiSuccess (200) {String} data.created_at 创建时间
        @apiSuccess (200) {String} data.updated_by 更新人
        @apiSuccess (200) {String} data.updated_at 更新时间

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    {
                        "metric_name": "last10output",
                        "metric_alias": "最近10分钟数据量",
                        "metric_type": "data_flow",
                        "metric_unit": "",
                        "sensitivity": "private",
                        "description": "最近10分钟数据量",
                        "created_by": "admin",
                        "created_at": "2020-07-27 10:30:00",
                        "updated_by": "admin",
                        "updated_at": "2020-07-27 10:31:00"
                    }
                ]
            }
        """
        return super(DataQualityMetricsViewSet, self).list(request)
