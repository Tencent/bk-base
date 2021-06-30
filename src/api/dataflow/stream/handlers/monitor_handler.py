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

from dataflow.pizza_settings import FLINK_CODE_VERSION
from dataflow.shared.stream.utils.resource_util import get_flink_session_std_cluster
from dataflow.stream.handlers import processing_job_info, processing_stream_job


class MonitorHandler(object):
    @staticmethod
    def get_store_flink_running_job():
        return (
            processing_stream_job.filter(component_type="flink", status="running", implement_type="sql")
            .exclude(cluster_group="debug")
            .values(
                "stream_id",
                "cluster_group",
                "cluster_name",
                "deploy_mode",
                "updated_at",
            )
        )

    @staticmethod
    def get_store_flink_job_info(geog_area_code):
        return (
            processing_job_info.where(component_type="flink", implement_type="sql")
            .filter(jobserver_config__contains=geog_area_code)
            .values("job_id")
        )

    @staticmethod
    def get_store_yarn_session(geog_area_code):
        session_clusters = get_flink_session_std_cluster(geog_area_code=geog_area_code, active=1)
        clusters = []
        for one_session in session_clusters:
            clusters.append({"cluster_name": one_session["cluster_name"]})
        return clusters

    @staticmethod
    def get_store_yarn_cluster():
        return (
            processing_stream_job.filter(
                component_type="flink",
                status="running",
                implement_type="sql",
                deploy_mode="yarn-cluster",
            )
            .exclude(cluster_group="debug")
            .values("stream_id")
        )

    @staticmethod
    def get_store_yarn_session_job():
        return (
            processing_stream_job.filter(component_type="flink", status="running", implement_type="sql")
            .exclude(deploy_mode="yarn-cluster")
            .exclude(cluster_group="debug")
            .values("stream_id")
        )

    @staticmethod
    def get_db_spark_streaming_running_job():
        return (
            processing_stream_job.filter(
                component_type="spark_structured_streaming",
                status="running",
                implement_type="code",
                deploy_mode="yarn-cluster",
            )
            .exclude(cluster_group="debug")
            .values("stream_id")
        )

    @staticmethod
    def get_db_flink_code_running_job():
        flink_code = "{}-{}".format("flink", FLINK_CODE_VERSION)
        return (
            processing_stream_job.filter(
                component_type=flink_code,
                status="running",
                implement_type="code",
                deploy_mode="yarn-cluster",
            )
            .exclude(cluster_group="debug")
            .values("stream_id")
        )
