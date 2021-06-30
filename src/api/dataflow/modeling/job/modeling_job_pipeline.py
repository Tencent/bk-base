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
import uuid

from common.local import get_request_username
from conf import dataapi_settings as settings

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo
from dataflow.modeling.exceptions.comp_exceptions import CreateJobError
from dataflow.modeling.job import job_driver
from dataflow.modeling.job import job_driver as mlsql_batch_driver
from dataflow.modeling.job.modeling_jobs import ModelingSparkMLLibJob, ModelingTensorflowJob
from dataflow.modeling.processing.bulk_params import DataflowProcessingBulkParams, DatalabProcessingBulkParams
from dataflow.modeling.processing.modeling_processing import DataflowTableOperator, DatalabTableOperator
from dataflow.modeling.processing.tensorflow.tensorflow_batch_operator import DataflowTensorFlowOperator
from dataflow.modeling.settings import USE_SCENARIO
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.shared.bksql.bksql_helper import BksqlApiHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper


class ModelingJobPipeLine(object):

    job = None
    modeling_job = None

    def __init__(self, job_id):
        self.job = ProcessingJobInfoHandler.get_proc_job_info(job_id)
        jobserver_config = json.loads(self.job.jobserver_config)
        self.geog_area_code = jobserver_config["geog_area_code"]
        self.cluster_id = jobserver_config["cluster_id"]
        self.modeling_job = self.get_job()

    def get_job(self):
        component_type = self.job.component_type
        if component_type == "spark_mllib":
            return ModelingSparkMLLibJob(self.job)
        elif component_type == "tensorflow":
            return ModelingTensorflowJob(self.job)

    @classmethod
    def get_table_operator(cls, params):
        if "use_scenario" in params:
            use_scenario = params["use_scenario"]
        else:
            use_scenario = "datalab"
            params["use_scenario"] = use_scenario

        if use_scenario == "datalab":
            bulk_params = DatalabProcessingBulkParams(params)
            operator = DatalabTableOperator(bulk_params)
        elif use_scenario == "dataflow":
            component_type = params["component_type"]
            if component_type == "spark_mllib":
                bulk_params = DataflowProcessingBulkParams(params)
                operator = DataflowTableOperator(bulk_params)
            else:
                operator = DataflowTensorFlowOperator(params)
        return operator

    @classmethod
    def create_model_processing(cls, params):
        operator = cls.get_table_operator(params)
        return operator.create_processing()

    @classmethod
    def create_mlsql_job(cls, processing_info, basic_params):
        job_config = {
            "heads": processing_info["heads"],
            "tails": processing_info["tails"],
            "project_id": basic_params["project_id"],
            "use_type": basic_params["use_type"],
            "component_type": basic_params["component_type"],
            "notebook_id": basic_params["notebook_id"],
            "cell_id": basic_params["cell_id"],
        }
        geog_area_code = TagHelper.get_geog_area_code_by_project(basic_params["project_id"])
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        jobserver_config = {"geog_area_code": geog_area_code, "cluster_id": cluster_id}
        geog_area_code = TagHelper.get_geog_area_code_by_project(basic_params["project_id"])
        cluster_id = JobNaviHelper.get_jobnavi_cluster(geog_area_code, "batch")
        jobserver_config = {"geog_area_code": geog_area_code, "cluster_id": cluster_id}
        job_info = cls.create_job(
            basic_params["project_id"],
            json.dumps(job_config),
            jobserver_config,
            processing_ids=processing_info["processing_ids"],
        )
        return job_info["job_id"]

    @classmethod
    def create_job(cls, project_id, job_config, jobserver_config, processing_ids=[]):
        try:
            job_id = "{}_{}".format(project_id, str(uuid.uuid4()).replace("-", ""))
            # 取任一Process，获取job的基本属性
            processing_id = processing_ids[0]
            batch_processing = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
            job_config_map = json.loads(job_config)
            job_config_map["processor_type"] = batch_processing.processor_type
            job_config_map["processor_logic"] = batch_processing.processor_logic
            job_config_map["schedule_period"] = batch_processing.schedule_period
            job_config_map["count_freq"] = batch_processing.count_freq
            job_config_map["delay"] = batch_processing.delay
            job_config_map["submit_args"] = batch_processing.submit_args
            params = cls.save_job(job_id, job_config_map, jobserver_config, processing_ids)
            return params
        except Exception as e:
            logger.error("create job error:%s" % e)
            raise CreateJobError(message_kv={"content": "{}".format(e)})

    @classmethod
    def save_job(
        cls,
        job_id,
        job_config,
        jobserver_config,
        processing_ids=[],
        cluster_group=None,
        cluster_name=None,
    ):
        if not cluster_group or not cluster_name:
            # 执行时并未指定集群信息，则获取默认的
            cluster_group = settings.MLSQL_CLUSEER_GRUUP
            default_cluster_group = settings.MLSQL_DEFAULT_CLUSTER_GROUP
            default_cluster_name = settings.MLSQL_DEFAULT_CLUSTER_NAME
            cluster_group, cluster_name = ModelingUtils.get_cluster_group(
                cluster_group, default_cluster_group, default_cluster_name
            )
        # 获取processing的component_type
        processing_batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_ids[0])
        params = {
            "bk_username": get_request_username(),
            "job_id": job_id,
            "job_config": json.dumps(job_config),
            "jobserver_config": jobserver_config,
            "cluster_group": cluster_group,
            "cluster_name": cluster_name,
            "component_type": processing_batch_info.component_type,
        }
        ProcessingJobInfoHandler.create_proc_job_info(params)
        logger.info(processing_ids)
        for process_id in processing_ids:
            logger.info("update processing:" + process_id)
            ProcessingBatchInfo.objects.filter(processing_id=process_id).update(batch_id=job_id)
        return params

    def start_mlsql_job(self, params=None):
        params = self.modeling_job.prepare_job(params)
        exec_id = self.modeling_job.submit_job(params)
        submit_args = {"exec_id": exec_id}
        current_time_mills = int(time.time() * 1000)
        ProcessingBatchJobHandler.save_proc_batch_job(
            self.job.job_id,
            self.job,
            current_time_mills,
            self.job.created_by,
            submit_args=json.dumps(submit_args),
        )
        return exec_id

    def check_job(self):
        return self.modeling_job.check_job()

    def clear_job(self, status, bk_username):
        return self.modeling_job.clear_job(status, bk_username)

    def stop_job(self, task=None):
        # stop_model_job(self.modeling_job)
        return self.modeling_job.stop_job(task)

    def update_job(self, params):
        return self.modeling_job.update_job(params)

    def delete_job(self, params):
        return self.modeling_job.delete_job(params)

    def record_exec_id(self, exec_id):
        self.modeling_job.record_exec_id(exec_id)

    def merge_return_object(self, exec_id):
        return_object = {
            "job_id": self.job.job_id,
            "exec_id": exec_id,
            "geog_area_code": self.geog_area_code,
            "cluster_id": self.cluster_id,
        }
        return return_object

    def start_multi_jobs(self, is_restart):
        """
        dataflow内启动模型应用节点，某个节点可能会生成多个Job
        @param is_restart:是否会重启
        @return:
        """
        schedule_id = self.job.job_id
        current_time_mills = int(time.time() * 1000)
        created_by = get_request_username()
        mlsql_batch_driver.register_schedule(schedule_id, current_time_mills, created_by, is_restart)
        ProcessingBatchJobHandler.save_proc_batch_job(
            self.job.job_id,
            self.job,
            current_time_mills,
            self.job.created_by,
            submit_args=json.loads(self.job.job_config)["submit_args"],
        )
        return schedule_id

    def multi_stop_jobs(self, type="batch"):
        jobserver_config = json.loads(self.job.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_id = jobserver_config["cluster_id"]
        jobnavi_helper = JobNaviHelper(geog_area_code, cluster_id)
        jobnavi_helper.stop_schedule(self.job.job_id)
        jobnavi_helper.delete_schedule(self.job.job_id)

    @classmethod
    def create_multi_jobs(cls, jobserver_config, processing_id, cluster_group, cluster_name):
        try:
            # 这里的processing_id为整个mlsql流程中最后一个Process，其名称即对应输出表名
            # 按我们创建Process的规则，其它processing的Id是按顺序编号的，其中第一个输出就是｛process_id｝_mlsql_1
            # 因此我们可以直接得到整个过程的heads,及tails
            processing_info_list = ProcessingBatchInfoHandler.get_proc_batch_info_by_prefix(processing_id)

            # 得到所有process的拓扑结构，即获取每个Process的input与output
            # processing_topology = cls.get_process_topology(heads, tails, processing_id)
            # 根据process的依赖关系，结合“merge原则”，将现有的process合并成为不同的job
            # job_detail_dict = cls.get_job_topology(project_id, heads, processing_topology)
            job_id_list = []
            for processing_info in processing_info_list:
                try:
                    job_id = processing_info.processing_id
                    new_job_config = {
                        "count_freq": processing_info.count_freq,
                        "processor_type": processing_info.processor_type,
                        "component_type": processing_info.component_type,
                        "schedule_period": processing_info.schedule_period,
                        "delay": processing_info.delay,
                        "use_type": USE_SCENARIO.DATAFLOW.value,
                        "submit_args": processing_info.submit_args,
                        "processor_logic": processing_info.processor_logic,
                    }
                    cls.save_job(
                        job_id,
                        new_job_config,
                        jobserver_config,
                        [job_id],
                        cluster_group=cluster_group,
                        cluster_name=cluster_name,
                    )
                    job_id_list.append(job_id)
                except Exception as e:
                    for job_id in job_id_list:
                        processing_job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)
                        processing_job_info.delete()
                    raise e
            return {"job_id": processing_id}
        except Exception as e:
            logger.error("create job error:%s" % e)
            raise CreateJobError(message_kv={"content": "{}".format(e)})

    def update_multi_jobs(self, jobserver_config):
        batch_processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(self.job.job_id)
        processing_job_info_list = ProcessingJobInfoHandler.get_proc_job_info_by_prefix(self.job.job_id)
        for processing_job_info in processing_job_info_list:
            new_job_config = {
                "count_freq": batch_processing_info.count_freq,
                "processor_type": batch_processing_info.processor_type,
                "component_type": processing_job_info.component_type,
                "schedule_period": batch_processing_info.schedule_period,
                "delay": batch_processing_info.delay,
                "use_type": USE_SCENARIO.DATAFLOW.value,
                "submit_args": batch_processing_info.submit_args,
                "processor_logic": batch_processing_info.processor_logic,
            }
            params = {
                "bk_username": get_request_username(),
                "job_id": processing_job_info.job_id,
                "job_config": json.dumps(new_job_config),
                "jobserver_config": json.dumps(jobserver_config),
            }
            ProcessingJobInfoHandler.update_proc_job_info(params)
        return self.job.job_id

    def multi_delete_jobs(self):
        logger.info("multi delete job:" + self.job.job_id)
        job_info_list = ProcessingJobInfoHandler.get_proc_job_info_by_prefix(self.job.job_id)
        jobservcer_config = None
        for job_info in job_info_list:
            jobservcer_config = json.loads(job_info.jobserver_config)
            logger.info("delete job:" + job_info.job_id)
            job_info.delete()
        # jobnavi上移除
        jobnavi = job_driver.JobNaviHelper(jobservcer_config["geog_area_code"], jobservcer_config["cluster_id"])
        jobnavi.delete_schedule(self.job.job_id)

    @classmethod
    def get_mlsql_result(cls, task):
        mlsql_result = {
            "result_table": {"add": [], "remove": []},
            "model": {"add": [], "remove": []},
        }
        # 某个实体在一次执行中可能有多次操作（先drop,然后create,也有可能返过来）
        # 此map按顺序记录对某个实例所有的操作，最终是add还是remove取map中每个元素对应list的最后一个值
        if task.status != "success":
            return mlsql_result
        else:
            task_args = json.loads(task.context)["args"]
            sql_list = task_args["sql_list"]
            # 先解析drop操作的
            for sql in sql_list:
                sql_args = {"sql": sql, "properties": {}}
                parse_result = BksqlApiHelper.list_result_entity_by_mlsql_parser(sql_args)
                entity_name = parse_result["entity"]
                entity_type = parse_result["type"]
                entity_type = "result_table" if entity_type == "table" else "model"
                operate_type = parse_result["operate"]
                if operate_type != "drop":
                    continue
                else:
                    result_item = {"name": entity_name, "sql": sql}
                    mlsql_result[entity_type]["remove"].append(result_item)

            # 其它操作，从job信息中直接获取
            if "job_id" in task_args:
                # 新增的直接从processing中取
                job_id = task_args["job_id"]
                processing_list = ProcessingBatchInfoHandler.get_proc_batch_info_lst_by_batch_id(job_id)
                for processing in processing_list:
                    processing_id = processing.processing_id
                    processor_logic = json.loads(processing.processor_logic)
                    sql = processor_logic["sql"]
                    operate_type = processor_logic["operate_type"] if "operate_type" in processor_logic else "create"
                    result_item = {"name": processing_id, "sql": sql}
                    if operate_type == "train":
                        mlsql_result["model"]["add"].append(result_item)
                    else:
                        mlsql_result["result_table"]["add"].append(result_item)
        return mlsql_result
