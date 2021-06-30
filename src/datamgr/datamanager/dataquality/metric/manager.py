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
import logging

from sqlalchemy.sql.expression import true

from dataquality import session
from dataquality.exceptions import DataQualityError, MetricNotSupportedError
from dataquality.metric.factory import MetricFactory
from dataquality.model.core import DataQualityMetric


class MetricManager(object):
    def __init__(self):
        self._metrics = {}

        self.init_metrics()

    def init_metrics(self):
        metric_configs = session.query(DataQualityMetric).filter(
            DataQualityMetric.active == true()
        )

        for metric_config in metric_configs:
            try:
                self._metrics[metric_config.name] = MetricFactory(metric_config)
            except DataQualityError as e:
                logging.error(e, exc_info=True)

    def get_metric(self, metric_name):
        if metric_name in self._metrics:
            return self._metrics[metric_name]
        else:
            metric_config = (
                session.query(DataQualityMetric)
                .filter(
                    DataQualityMetric.metric_name == metric_name
                    and DataQualityMetric.active == true()
                )
                .first()
            )

            if not metric_config:
                raise MetricNotSupportedError(message_kv={"metric_name": metric_name})

            self._metrics[metric_name] = MetricFactory(metric_config)
            return self._metrics[metric_name]
