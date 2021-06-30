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

import json
import time

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo
from dataflow.flow.api_models import ResultTable
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.models import FlowInfo
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.stream.models import ProcessingJobInfo, ProcessingStreamJob
from dataflow.stream.settings import DeployMode


class FlowLogUtil(object):
    @staticmethod
    def get_processing_type_and_schedule_id(job_id):
        """
        获取某个job对应的 processing_type 和 schedule id
        @param job_id: job_id
        @return: processing_type 和 schedule id
        """
        processing_job_info = ProcessingJobInfo.objects.get(job_id=job_id)
        if processing_job_info:
            processing_type = processing_job_info.processing_type
            if processing_type == "batch":
                return processing_type, job_id
            elif processing_type == "model":
                return processing_type, job_id
            elif processing_type == "stream":
                stream_job = ProcessingStreamJob.objects.filter(stream_id=job_id)
                if stream_job:
                    deploy_mode = stream_job[0].deploy_mode
                    cluster_name = stream_job[0].cluster_name
                    if deploy_mode == DeployMode.YARN_CLUSTER.value:
                        return processing_type, job_id
                    elif deploy_mode == DeployMode.YARN_SESSION.value:
                        return processing_type, cluster_name
        return None, None

    @staticmethod
    def get_cluster_id_and_geog_area_code(job_id):
        """
        查询某个job所在的cluster_id和geog_area_code
        @param {String} job_id
        @return: the cluster id and geog_area_code for a job
        """
        job_info = ProcessingJobInfo.objects.get(job_id=job_id)
        processing_type = job_info.processing_type
        cluster_group = job_info.cluster_group
        geog_area_code = TagHelper.get_geog_area_code_by_cluster_group(cluster_group)
        cluster_id = JobNaviHelper.get_jobnavi_cluster(processing_type)
        return cluster_id, geog_area_code

    @staticmethod
    def get_jobnavi_helper_last_execute_id(job_id):
        """
        获取job对应的正在运行任务的execute id及对应的jobnavi_helper（该请求5秒超时后返回空值）
        :param job_id: job_id
        :return: jobnavi_helper and execute_id
        """
        logger.info("begin to get_execute_id_for_job job_id(%s)" % (job_id))

        (
            processing_type,
            schedule_id,
        ) = FlowLogUtil.get_processing_type_and_schedule_id(job_id)
        logger.info(
            "get schedule_id({}) processing_type({}) for job_id({})".format(schedule_id, processing_type, job_id)
        )

        (
            cluster_id,
            geog_area_code,
        ) = FlowLogUtil.get_cluster_id_and_geog_area_code(job_id)
        logger.info("get cluster_id({}) geog_area_code({}) for job_id({})".format(cluster_id, geog_area_code, job_id))

        jobnavi_helper = JobNaviHelper(geog_area_code, cluster_id)
        execute_id = None

        for i in range(5):
            # get last execute id, fail or success both OK
            data = jobnavi_helper.get_schedule_execute_result(schedule_id, "1")
            logger.info("[get execute id] schedule id {} and result is {}".format(schedule_id, data))

            if data and len(data) >= 1 and data[0]["execute_info"]["id"]:
                logger.info("The cluster name {} result is {}".format(schedule_id, str(data)))
                execute_id = data[0]["execute_info"]["id"]
                logger.info("finish to get_execute_id_for_job job_id({}), execute_id({})".format(job_id, execute_id))
                return jobnavi_helper, execute_id
            time.sleep(1)
        logger.error("Jobnavi get execute info timeout, schedule id is %s" % schedule_id)
        return jobnavi_helper, execute_id

    @staticmethod
    def get_last_execute_info_for_job(job_id):
        """
        获取job对应的正在运行任务的execute id及对应的jobnavi_helper（该请求5秒超时后返回空值）
        :param job_id: job_id
        :return: jobnavi_helper and execute_id
        """
        logger.info("begin to get_execute_id_for_job job_id(%s)" % (job_id))

        (
            processing_type,
            schedule_id,
        ) = FlowLogUtil.get_processing_type_and_schedule_id(job_id)
        logger.info(
            "get schedule_id({}) processing_type({}) for job_id({})".format(schedule_id, processing_type, job_id)
        )

        (
            cluster_id,
            geog_area_code,
        ) = FlowLogUtil.get_cluster_id_and_geog_area_code(job_id)
        logger.info("get cluster_id({}) geog_area_code({}) for job_id({})".format(cluster_id, geog_area_code, job_id))

        jobnavi_helper = JobNaviHelper(geog_area_code, cluster_id)
        execute_id = None
        schedule_time = None

        for i in range(5):
            # get last execute id, fail or success both OK
            data = jobnavi_helper.get_schedule_execute_result(schedule_id, "1")
            logger.info("[get execute id] schedule id {} and result is {}".format(schedule_id, data))

            if data and len(data) >= 1 and data[0]["execute_info"]["id"]:
                logger.info("The cluster name {} result is {}".format(schedule_id, str(data)))
                execute_id = data[0]["execute_info"]["id"]
                schedule_time = data[0]["schedule_time"]

                logger.info(
                    "finish to get_execute_id_for_job job_id(%s), execute_id(%s), schedule_time(%s)"
                    % (job_id, execute_id, schedule_time)
                )
                return execute_id, schedule_time
            time.sleep(1)
        logger.error("Jobnavi get execute info timeout, schedule id is %s" % schedule_id)
        return execute_id, schedule_time

    @staticmethod
    def list_rt_by_flowid(flow_id):
        """
        查询某个Flow中的分类型任务列表及对应的rt列表，仅支持spark/flink任务
        {
            "batch": {
                "591_iptest_expand3_offline": {
                    "schedule_period": "hour",
                    "component_type": "spark",
                    "result_tables": [
                        {
                        'result_table_id': '591_iptest_expand3_offline',
                        'result_table_name_alias': '591_iptest_expand3_offline'
                        }
                    ]
                }
            },
            "stream": {
                "13035_7ecd590e3473496f887ae37b3ff36f9a": {
                    "schedule_period": "realtime",
                    "component_type": "flink",
                    "result_tables": [
                        {
                        'result_table_id': '591_iptest_expand',
                        'result_table_name_alias': '591_iptest_expand'
                        },
                        {
                        'result_table_id': '591_iptest_expand2',
                        'result_table_name_alias': '591_iptest_expand2'
                        },
                        {
                        'result_table_id': '591_iptest_expand3',
                        'result_table_name_alias': '591_iptest_expand3'
                        }
                    ]
                }
            }
        }
        @param {String} flow_id
        """
        ret = {}
        flow_info = list(FlowInfo.objects.filter(flow_id=flow_id, status=FlowInfo.STATUS.RUNNING))

        if flow_info:
            for one_node_id in flow_info[0].flow_node_ids:
                node_handler = NODE_FACTORY.get_node_handler(node_id=one_node_id)
                one_job = node_handler.get_job()
                if one_job:
                    job_id = one_job.job_id
                    component_type = None
                    deploy_mode = "yarn"
                    one_processing_job_info = None
                    try:
                        one_processing_job_info = ProcessingJobInfo.objects.get(job_id=job_id)
                        if one_processing_job_info:
                            component_type = one_processing_job_info.component_type
                            if not component_type.lower().startswith("spark") and not component_type.lower().startswith(
                                "flink"
                            ):
                                # only spark/flink supported now
                                continue
                    except ProcessingJobInfo.DoesNotExist:
                        logger.warning("can not get processingjobinfo for job_id(%s)" % (job_id))
                        continue
                    processing_type = one_job.job_type
                    schedule_period = None
                    if processing_type == "batch":
                        try:
                            processing_batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(job_id)
                            schedule_period = processing_batch_info.schedule_period
                        except ProcessingBatchInfo.DoesNotExist:
                            if one_processing_job_info.job_config is not None:
                                job_config = json.loads(one_processing_job_info.job_config)
                                if "submit_args" in job_config:
                                    submit_args = json.loads(job_config["submit_args"])
                                    if "schedule_period" in submit_args:
                                        schedule_period = submit_args["schedule_period"]
                    elif processing_type == "stream":
                        schedule_period = "realtime"
                        try:
                            processing_stream_job = ProcessingStreamJob.objects.get(stream_id=job_id)
                            deploy_mode = processing_stream_job.deploy_mode
                            if deploy_mode.startswith("yarn"):
                                deploy_mode = "yarn"
                            elif deploy_mode.startswith("k8s"):
                                deploy_mode = "k8s"
                        except ProcessingStreamJob.DoesNotExist:
                            logger.warning("can not get processingstreamjob for job_id(%s)" % (job_id))
                    elif processing_type.endswith("model"):
                        if one_processing_job_info.job_config is not None:
                            job_config = json.loads(one_processing_job_info.job_config)
                            if "submit_args" in job_config:
                                submit_args = json.loads(job_config["submit_args"])
                                if "schedule_period" in submit_args:
                                    schedule_period = submit_args["schedule_period"]
                        if component_type and component_type.lower() == "spark_structured_streaming":
                            schedule_period = "realtime"
                        processing_type = "model"
                    if processing_type not in ret:
                        ret[processing_type] = {}
                    if job_id not in ret[processing_type]:
                        ret[processing_type][job_id] = {"result_tables": []}
                    ret[processing_type][job_id]["schedule_period"] = schedule_period
                    ret[processing_type][job_id]["component_type"] = component_type
                    ret[processing_type][job_id]["deploy_mode"] = deploy_mode

                    result_table_ids = node_handler.result_table_ids
                    for one_rt in result_table_ids:
                        rt = ResultTable(one_rt)
                        rt_name_alias = rt.result_table_name_alias
                        logger.info(
                            "job_id ({}), rt ({}), processing_type ({})".format(job_id, one_rt, processing_type)
                        )
                        ret[processing_type][job_id]["result_tables"].append(
                            {
                                "result_table_id": one_rt,
                                "result_table_name_alias": rt_name_alias,
                            }
                        )
        return ret
