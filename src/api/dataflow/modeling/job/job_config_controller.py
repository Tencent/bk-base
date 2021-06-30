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
from datetime import datetime, timedelta

from conf import dataapi_settings as settings

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.processings import processings_driver
from dataflow.modeling.debug.debugs import filter_debug_metric_log
from dataflow.modeling.exceptions.comp_exceptions import ModelNotExistsError
from dataflow.modeling.handler.algorithm_version import AlgorithmVersionHandler
from dataflow.modeling.handler.model_release import ModelReleaseHandler
from dataflow.modeling.job.job_config_driver import generate_complete_config
from dataflow.modeling.job.job_driver import get_input_info, get_output_info, get_window_info, update_process_and_job
from dataflow.modeling.job.tensorflow.tensorflow_job_driver import TensorFlowJobDriver
from dataflow.modeling.settings import PARSED_TASK_TYPE, TABLE_TYPE
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.pizza_settings import BASE_DATAFLOW_URL
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.stream.result_table.result_table_for_common import fix_filed_type

MODEL_AUTO_UPDATE_POLICY_DELAY = "update_policy_delay"
MODEL_AUTO_UPDATE_POLICY_IMMEDIATELY = "update_policy_immediately"


class ModelingJobConfigController(object):
    def __init__(self, job_id, job_type, run_mode, is_debug):
        self.job_id = job_id
        self.job_type = job_type
        self.run_mode = run_mode
        self.is_debug = is_debug

    def generate_job_config(self):
        if self.job_type == "spark_mllib":
            return self.generate_spark_job_config()
        elif self.job_type == "tensorflow":
            return self.generate_tensorflow_job_config()

    def generate_tensorflow_job_config(self):
        batch_job_info = ProcessingBatchJobHandler.get_proc_batch_job(self.job_id)
        return TensorFlowJobDriver.get_param(batch_job_info)

    def generate_spark_job_config(self):
        sources = {}
        transformers = {}
        sinks = {}
        job_info = ProcessingJobInfoHandler.get_proc_job_info(self.job_id)
        batch_job_info = ProcessingBatchJobHandler.get_proc_batch_job(self.job_id)
        job_config = json.loads(job_info.job_config)
        if "heads" not in job_config and "tails" not in job_config:
            return self.generate_simple_job_config(job_info)
        heads = job_config.get("heads")
        tails = job_config.get("tails")
        result_table_info = self.load_chain(heads, tails)
        # 根据source,sink的属性，对返回的信息做微调
        logger.info("result table info:" + json.dumps(result_table_info))
        logger.info("heads:" + json.dumps(heads))
        for result_table in result_table_info:
            if result_table in heads:
                if "parents_info" in result_table_info[result_table]:
                    del result_table_info[result_table]["parents_info"]
                if "parents" in result_table_info[result_table]:
                    del result_table_info[result_table]["parents"]
                sources[result_table] = result_table_info[result_table]
            else:
                table_need_sink = True
                if "transform" in result_table_info[result_table]:
                    table_transformer = result_table_info[result_table]["transform"]
                    if (
                        "task_type" in table_transformer
                        and table_transformer["task_type"] == PARSED_TASK_TYPE.SUB_QUERY.value
                    ):
                        table_need_sink = False
                    del table_transformer["parents_info"]
                    transformers[table_transformer["id"]] = table_transformer
                del result_table_info[result_table]["parents_info"]
                del result_table_info[result_table]["transform"]
                # 子查询数据不需要落地，所以不需要放入sink
                if table_need_sink:
                    sinks[result_table] = result_table_info[result_table]
                    del sinks[result_table]["parents"]

            """
            elif result_table in tails:
                del result_table_info[result_table]['parents_info']
                sinks[result_table] = result_table_info[result_table]
            else:
                del result_table_info[result_table]['parents_info']
                transformers[result_table] = result_table_info[result_table]
            """
        # sink的id要与其对应的transform的id是一致的
        # self.redefine_transform_and_sink(transformers, sinks)
        url = BASE_DATAFLOW_URL
        url = url.rstrip("/").rstrip("dataflow").rstrip("/")
        final_transform = {
            "job_id": self.job_id,
            "job_name": self.job_id,
            "job_type": self.job_type,
            "run_mode": self.run_mode,
            "time_zone": "Asia/Shanghai",
            "resource": {
                "cluster_group": job_info.cluster_group,
                "queue_name": job_info.cluster_name,
            },
            "nodes": {"source": sources, "transform": transformers, "sink": sinks},
            "metric": {"metric_rest_api_url": url + "/datamanage/dmonitor/metrics/report/"},
            "schedule_time": batch_job_info.schedule_time,
            "data_flow_url": url,
        }
        return final_transform

    def redefine_transform_and_sink(self, transformers, sinks):
        # 将sink的id与其对应的transform的id调整为一致的
        # 同时去除sink中parents，即transform与sink的关联用id直接关联
        try:
            for sink_id in sinks:
                sink_object = sinks[sink_id]
                sink_parents = sink_object["parents"]
                for parent in sink_parents:
                    tranformer = transformers[parent]
                    del transformers[parent]
                    tranformer["id"] = sink_id
                    tranformer["name"] = sink_id
                    transformers[sink_id] = tranformer
                del sink_object["parents"]
        except Exception:
            # logger.error("redefine transform error %s" % e)
            pass

    def load_mlsql_result_table(self, table_info, is_head):
        """
        获取rt的基本信息、parents信息，这里需要注意的是：
        每个rt的parent信息为其对应的transform，transform的parent信息则为另外一个rt或是model
        所以这个函数不仅获取生成当前rt的表和模型，同时也获取了生成此rt对应的transform
        """

        # 基本信息
        input_output_type = "input"
        if not is_head:
            input_output_type = "output"
        result_table = {
            "id": table_info["result_table_id"],
            "name": "{}_{}".format(table_info["result_table_name"], table_info["bk_biz_id"]),
            "description": table_info["description"],
            "type": "data",
        }
        # field相关信息
        fields = []
        for field in table_info["fields"]:
            if "timestamp" == field["field_name"]:
                continue
            else:
                common_field = {
                    "field": field["field_name"],
                    "type": fix_filed_type(field["field_type"]),
                    "origin": field["origins"] or [],
                    "description": field["description"],
                }
                fields.append(common_field)

        result_table["fields"] = fields
        result_table[input_output_type] = {
            "type": "hdfs",
            "format": "parquet",
            "table_type": TABLE_TYPE.QUERY_SET.value,
        }

        # storage相关信息
        if "hdfs" in table_info["storages"]:
            hdfs_storages = table_info["storages"]["hdfs"]
            data_type = hdfs_storages["data_type"]
            # 获取iceberg相关配置
            if data_type == "iceberg":
                iceberg_hdfs_config = StorekitHelper.get_hdfs_conf(table_info["result_table_id"])
                iceberg_config = {
                    "hdfs_config": iceberg_hdfs_config,
                    "physical_table_name": hdfs_storages["physical_table_name"],
                }
                result_table[input_output_type]["iceberg_config"] = iceberg_config
                result_table[input_output_type]["format"] = "iceberg"

            if input_output_type == "output":
                result_table[input_output_type]["path"] = "{hdfs_url}{path}".format(
                    hdfs_url=json.loads(hdfs_storages["storage_cluster"]["connection_info"])["hdfs_url"],
                    path=hdfs_storages["physical_table_name"],
                )
                result_table["storages"] = hdfs_storages
            else:
                result_table[input_output_type]["path"] = "{hdfs_url}{path}".format(
                    hdfs_url=json.loads(hdfs_storages["storage_cluster"]["connection_info"])["hdfs_url"],
                    path=hdfs_storages["physical_table_name"],
                )
        # 根据processing生成transform信息
        processing_id = None
        if "processing_id" in table_info["data_processing"]:
            processing_id = table_info["data_processing"]["processing_id"]
        transform = {}
        if not processing_id or not ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
            return [transform, result_table]
        else:
            processing_info = processings_driver.get_processing_batch_info(processing_id)

        processor_logics = json.loads(processing_info.processor_logic)
        logger.info("processor logics:" + processing_info.processor_logic)
        if not is_head:
            result_table[input_output_type]["mode"] = processor_logics["mode"]
        processor_logic = processor_logics["logic"]
        processor_model = {}
        if "model" in processor_logics:
            # logic中有模型的信息，说明是MLSQL相关的语句
            processor_model = processor_logics["model"]
        # 根据processing_batch_info生成transform
        # 根据约定，transform中需要的processor信息存储在bach_info的Logic字段中，所以需要取出来转换
        # transform_name = '{model_name}_run'.format(model_name=transform_entity_name)
        transform = {
            "id": processing_id,
            "name": processing_id,
            "type": processor_logic["type"],
            "parents": [],
            "processor": processor_logic["processor"],
            "interpreter": processor_logic["interpreter"],
            "fields": fields,
            "description": processing_id,
            "task_type": processor_logic["task_type"]
            if "task_type" in processor_logic
            else PARSED_TASK_TYPE.MLSQL_QUERY.value,
        }
        if "udfs" in processor_logics:
            transform["udfs"] = processor_logics["udfs"]
        # 获取表的parent信息，
        # rt的parent信息
        result_table["parents"] = [processing_id]
        result_table["parents_info"] = [{"tags": ["transform"], "data_set_id": processing_id}]

        # 根据dp信息生成transform的parents，即生成此dp的表及模型
        processing_info = DataProcessingHelper.get_dp_via_erp(processing_id)
        logger.info("table data processing info:" + json.dumps(processing_info))
        parents_info, data_parents = self.load_table_parents(processing_info)
        transform["parents"].extend(data_parents)
        transform["parents_info"] = parents_info
        if processor_model and "path" in processor_model:
            transform["parents"].append(processor_model["name"])
            transform["parents_info"].append({"data_set_id": processor_model["name"], "tags": ["model"]})
        # 返回信息包括transform以及当前rt
        return [transform, result_table]

    def load_table_parents(self, processing_info):
        data_parents = []
        parents_info = []
        for one_input in processing_info["inputs"]:
            if one_input["data_set_type"] == "result_table":
                tags = one_input.get("tags", [])
                if "static_join" not in tags:
                    data_parents.append(one_input["data_set_id"])
                parents_info.append({"data_set_id": one_input["data_set_id"], "tags": tags})
        return parents_info, data_parents

    def load_mlsql_result_model(self, model_name, is_head):
        """
        与load_table_result_model对应，此获取获取模型对应的transform及rt
        """
        input_output_type = "input"
        if not is_head:
            input_output_type = "output"
        model = ModelingHelper.get_model(model_name)
        if not model:
            raise ModelNotExistsError(message_kv={"name": model_name})
        else:
            # 获取模型的基本信息
            result_table = {
                "id": model_name,
                "name": model_name,
                "description": model_name,
                "type": "model",
                input_output_type: {
                    "type": "hdfs",
                    "format": model["algorithm_name"],
                    "path": model["storage"]["path"],
                },
            }
            if not is_head:
                # 模型只能overwrite
                result_table[input_output_type]["mode"] = "overwrite"
            # 获取模型对应的processing_batch_info,转换为transform
            # 模型对应的process_id即为模型的名称
            processing_info = processings_driver.get_processing_batch_info(model_name)
            processor_logics = json.loads(processing_info.processor_logic)
            processor_logic = processor_logics["logic"]
            # transform_name = '{model_name}_train'.format(model_name=model_name)
            transform = {
                "id": model_name,
                "name": model_name,
                "type": processor_logic["type"],
                "parents": [],
                "processor": processor_logic["processor"],
                "interpreter": processor_logic["interpreter"],
                "fields": [],
                "description": model_name,
                "task_type": processor_logic["task_type"]
                if "task_type" in processor_logic
                else PARSED_TASK_TYPE.MLSQL_QUERY.value,
            }
            if "udfs" in processor_logics:
                transform["udfs"] = processor_logics["udfs"]
            result_table["parents"] = [model_name]
            result_table["parents_info"] = [{"data_set_id": model_name, "tags": ["transform"]}]
            # 获取此processnng_batch对应的DP，进而得到其parents表信息
            processing_info = DataProcessingHelper.get_dp_via_erp(model_name)
            logger.info("model data processing info:" + json.dumps(processing_info))
            if processing_info:
                parents_info, data_parents = self.load_table_parents(processing_info)
                transform["parents"].extend(data_parents)
                transform["parents_info"] = parents_info
            else:
                transform["parents_info"] = []
            return [transform, result_table]

    def load_mlsql_result(self, result_table_id, is_head):
        """
        根据实体id，找到其详细信息，尤其是parent的相关信息，以实现链式的向上遍历
        """
        # 在mlsql内，其实体有两类，rt与model，以下两个分支进行处理
        table_info = self.get_result_tables(result_table_id)
        if "result_table_id" in table_info:
            # 实体为rt
            logger.info("load table:" + result_table_id)
            return self.load_mlsql_result_table(table_info, is_head)
        else:
            # 实体为model
            logger.info("load model:" + result_table_id)
            return self.load_mlsql_result_model(result_table_id, is_head)

    def get_result_tables(self, result_table_id):
        table_data = ResultTableHelper.get_result_table(result_table_id)
        if not table_data:
            return {}

        fields = ResultTableHelper.get_result_table_fields(result_table_id)
        table_data["fields"] = fields

        storage_data = ResultTableHelper.get_result_table_storage(result_table_id, "hdfs")
        table_data["storages"] = storage_data

        data_processing = DataProcessingHelper.get_data_processing(result_table_id)
        table_data["data_processing"] = data_processing
        return table_data

    def merge_transform_and_entity(self, transform, entity):
        if not transform:
            final_transform = entity.copy()
        else:
            final_transform = transform.copy()
            final_transform["id"] = entity["id"]
            final_transform["name"] = entity["name"]
            if "parents" in transform:
                final_transform["parents"] = transform["parents"]
        return final_transform

    def load_chain(self, heads, tails):
        """
        此过程与flink的load_chain基本是一致的，是一个遍历的过程，直到to_be_loaded为空停止
        """
        result_tables = {}
        to_be_loaded = list(tails)
        logger.info(heads)
        logger.info(tails)
        while to_be_loaded:
            loading = list(to_be_loaded)
            to_be_loaded = []
            for result_table_id in loading:
                if result_table_id in result_tables:
                    continue
                logger.info("load entity:" + result_table_id)
                load_results = self.load_mlsql_result(result_table_id, result_table_id in heads)
                logger.info("load result 1:" + json.dumps(load_results))
                transform = load_results[0]
                entity = load_results[1]
                entity["transform"] = transform
                result_tables[result_table_id] = entity

                """
                if not (result_table_id in heads or result_table_id in tails):
                    result_tables[result_table_id] = self.merge_transform_and_entity(transform, entity)
                else:
                    result_tables[result_table_id] = entity
                    if transform:
                        result_tables[transform['id']] = transform
                """
                if transform:
                    for parent_info in transform["parents_info"]:
                        parent_id = parent_info["data_set_id"]
                        if parent_id in heads:
                            parent_load_result = self.load_mlsql_result(parent_id, True)
                            logger.info("load result 2:" + json.dumps(parent_load_result))
                            result_tables[parent_id] = parent_load_result[1]
                        else:
                            parent_list = [parent_id]
                            to_be_loaded.extend(parent_list)
        return result_tables

    def get_and_update_model_config(self, current_config, submit_args):
        """
        结合现有的模型信息，以及更新策略，计算得到本次执行最新的模型配置信息
        @param current_config: 现有的模型模型信息
        @param submit_args: 任务提交信息，这里包括了模型更新策略
        @return:
        """
        node_info = submit_args["node"]
        dependence_info = submit_args["dependence"]
        deploy_model_info = node_info["deployed_model_info"]
        auto_update_enabled = deploy_model_info["enable_auto_update"]
        if not auto_update_enabled:
            # 自动更新没有打开，返回现有的配置
            return current_config
        else:
            model_id = deploy_model_info["input_model"]
            model_version = deploy_model_info["model_version"]
            auto_update = deploy_model_info["auto_update"]
            # 先检查当前模型最新版本的版本号
            params = {"model_id": model_id, "active": 1, "publish_status": "latest"}
            release_result = ModelReleaseHandler.where(**params)[0]
            if model_version == release_result.version_index:
                # 模型没有更新
                return current_config
            else:
                # 模型有更新，需要根据更新策略生成新的配置
                update_policy = auto_update["update_policy"]
                if update_policy == MODEL_AUTO_UPDATE_POLICY_DELAY:
                    # 延迟更新，需要确定是否可以更新了
                    updated_at = release_result.updated_at
                    update_time = auto_update["update_time"]
                    # 根据版本生成时间，和延迟时间，计算得到次日更新时间
                    update_hour = int(update_time.split(":")[0])
                    update_minite = int(update_time.split(":")[1])
                    # 对于指定的更新时间，有两种可能更新，今天指定时间或是明天指定时间
                    # 比如设置为21:00，那更新时间可能是今天（模型产生当天）21：00，也可能是次日21：00
                    # 需要根据模型产生时间点与这两个可能值相比较，来确定返回旧的配置还是获取新的配置
                    # 当天更新时间
                    today_update_time = updated_at.replace(hour=update_hour, minute=update_minite, second=0)
                    # 次日更新时间
                    tommorrow_update_time = today_update_time + timedelta(days=1)

                    # 为清晰起见，我们暂时不将如下if合并，而是展开为完整的分支
                    if updated_at < today_update_time:
                        # 新模型生成时间小于当天指定更新时间,比如最新模型在20点产生，但是更新时间指定为23点
                        if datetime.now() < today_update_time:
                            # 未到今天的更新时间，比如现在是21点，返回旧有配置
                            return current_config
                        else:
                            # 已过了今天的更新时间，比如23：30，那么返回新的配置
                            pass
                    else:
                        # 新模型生成时间大于当天指定更新时间，比如最新模型在20点，但是更新时间点定为16点
                        if datetime.now() < tommorrow_update_time:
                            # 当前时间小于次日更新时间，比如现在是10点，寻么不更新，返回旧配置
                            return current_config
                        else:
                            # 当前时间大于次日更新时间，比如现在是17点，返回新配置
                            pass

                # 至此，更新策略为立即或是过了更新时间，那需要计算新的配置
                latest_model_config_template = release_result.model_config_template
                output_info = get_output_info(node_info)
                # 从前端传递的内容中解析出输入表的信息
                input_info = get_input_info(dependence_info)
                # 得到窗口信息
                window_info = get_window_info(input_info["name"], dependence_info, node_info)
                # 获取参数信息
                model_params = deploy_model_info["model_params"]
                # 得到最新的模型配置信息
                new_model_config = generate_complete_config(
                    json.loads(latest_model_config_template),
                    input_info,
                    output_info,
                    window_info,
                    model_params,
                )
                deploy_model_info["model_version"] = release_result.version_index

                # 获得新的配置，需要回写
                update_process_and_job(output_info["name"], new_model_config, submit_args)
                return new_model_config

    def generate_simple_job_config(self, job_info):
        """
        如果在Job的配置中没有heads与tails，那么就表示此Job来自模型应用节点
        这类节点的配置直接取Processing信息里的processor_logic即可，不需要再次变换
        但这里有一点要注意：
           由于模型应用节点创建的时候不知道RT的存储，所以每个配置内的sink，就需要在此时进行初始化
           所以这里有获取表的存储并重新设置output的过程
        @return:
        """
        # 注意：在我们创建的job, dp，rt和process里，名字均是对应生成表的名称,所以可以根据job_id来直接获取
        # process或是job，甚至是dp或rt的信息
        result_table_storage = ResultTableHelper.get_result_table_storage(self.job_id, "hdfs")["hdfs"]
        result_data_type = result_table_storage["data_type"]
        result_table_connect = json.loads(result_table_storage["storage_cluster"]["connection_info"])
        table_path = "{hdfs_url}/{hdfs_path}".format(
            hdfs_url=result_table_connect["hdfs_url"],
            hdfs_path=result_table_storage["physical_table_name"],
        )
        batch_job_info = ProcessingBatchJobHandler.get_proc_batch_job(self.job_id)
        submit_args = json.loads(json.loads(job_info.job_config)["submit_args"])
        processor_logic = json.loads(json.loads(job_info.job_config)["processor_logic"])

        # 考虑到模型的自动更新功能，模型相关的属性需要在每次执行前动态的获取，因此引入下面的get_and_update的过程
        # 如果未到自动更新时间，直接返回旧配置，如果需要更新，则返回更新之后的配置
        new_processor_logic = self.get_and_update_model_config(processor_logic, submit_args)

        sink_info = new_processor_logic["sink"]
        for table_id in sink_info:
            if sink_info[table_id]["type"] == "data":
                sink_info[table_id]["output"]["path"] = table_path
                sink_info[table_id]["output"]["table_type"] = TABLE_TYPE.RESULT_TABLE.value
                sink_info[table_id]["storages"] = {"hdfs": result_table_storage}
                if result_data_type == "iceberg":
                    iceberg_config = {
                        "physical_table_name": result_table_storage["physical_table_name"],
                        "hdfs_config": StorekitHelper.get_hdfs_conf(table_id),
                    }
                    sink_info[table_id]["output"]["iceberg_config"] = iceberg_config
                    sink_info[table_id]["output"]["format"] = "iceberg"

        url = BASE_DATAFLOW_URL
        url = url.rstrip("/").rstrip("dataflow").rstrip("/")
        final_transform = {
            "job_id": self.job_id,
            "job_name": self.job_id,
            "job_type": self.job_type,
            "run_mode": self.run_mode,
            "time_zone": "Asia/Shanghai",
            "resource": {
                "cluster_group": job_info.cluster_group,
                "queue_name": job_info.cluster_name,
            },
            "nodes": new_processor_logic,
            "metric": {"metric_rest_api_url": url + "/datamanage/dmonitor/metrics/report/"},
            "schedule_time": batch_job_info.schedule_time,
            "data_flow_url": url,
        }
        return final_transform

    @classmethod
    def get_init_jobs(cls, heads):
        init_jobs = {}
        for processing_id in heads:
            if not init_jobs:
                init_jobs[processing_id] = [processing_id]
                continue
            # 判断新的processing_id是否可以合入已有的job
            existing_job_to_merge = False
            for existing_processing_id in init_jobs:
                if cls.processing_can_merge(processing_id, existing_processing_id):
                    init_jobs[existing_processing_id].append(processing_id)
                    existing_job_to_merge = True
                    break
            # 未找到任何可以合并的job，则需要生成一个新的Job
            if not existing_job_to_merge:
                init_jobs[processing_id] = [processing_id]
        return init_jobs

    @classmethod
    def processing_can_merge(cls, new_processing_id, existing_processing_id):
        return True

    @classmethod
    def get_mlsql_config(cls, mlsql_processing_ids, project_id):
        """
        根据输入的sql列表，返回对应的job_config,Note:
          1. sql_list是有序的
        """
        single_config_list = []
        sql_index = 0
        logger.info(mlsql_processing_ids)
        for processing_id in mlsql_processing_ids:
            sql_index = sql_index + 1
            logger.info("processing id:" + processing_id)
            processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
            processing_logic = json.loads(processing_info.processor_logic)
            # 直接从processing中拿到任务的配置，不再重新解析
            parsed_task = processing_logic["task_config"]
            task_type = parsed_task["task_type"]
            source = {}
            transform = {}
            sink = {}
            # 获取source,transform与sink
            single_source = cls.format_source(parsed_task)
            for source_id in single_source:
                if single_source[source_id]:
                    # 这里剔除不存在的虚拟表，即子查询的结果是不能作为source的，但需要作为parent
                    source[source_id] = single_source[source_id]
            single_transform = cls.format_transform(parsed_task, source_info=single_source, project_id=project_id)
            transform.update(single_transform)
            if task_type == PARSED_TASK_TYPE.MLSQL_QUERY.value:
                # 子查询不需要落地
                single_sink = cls.format_sink(parsed_task)
                sink.update(single_sink)
            config = {"source": source, "transform": transform, "sink": sink}
            single_config_list.append(config)
        result_config_list = []
        first_config = single_config_list[0]
        bulk_mlsql_config = {
            "source": first_config["source"],
            "transform": first_config["transform"],
        }
        if len(single_config_list) == 1:
            bulk_mlsql_config["sink"] = first_config["sink"]
            result_config_list.append(bulk_mlsql_config)
        else:
            last_config = first_config
            for config in single_config_list[1:]:
                for config_id in config["source"]:
                    config_type = config["source"][config_id]["type"]
                    if config_type == "model":
                        bulk_mlsql_config["source"][config_id] = config["source"][config_id]
                if cls.alg_can_merge(last_config, config):
                    bulk_mlsql_config["transform"].update(config["transform"])
                else:
                    bulk_mlsql_config["sink"] = last_config["sink"]
                    result_config_list.append(bulk_mlsql_config.copy())
                    bulk_mlsql_config = {
                        "source": config["source"],
                        "transform": config["transform"],
                    }
                last_config = config
            bulk_mlsql_config["sink"] = last_config["sink"]
            result_config_list.append(bulk_mlsql_config)
        return result_config_list

    @classmethod
    def alg_can_merge(cls, last_config, confi):
        """
        判断两个transform算法是否可以合并,需要考虑：
            1. 算法所属框架：如当前spark算法与sklearn算法不能合并
            2. 考虑对应Process的属性：目前这一点不用考虑，因为datalab内生成的process调度相关设计增多一致
        """
        # todo
        return True

    @classmethod
    def format_source(cls, parsed_task):
        source_info = {}
        if "model_info" in parsed_task:
            model_info = parsed_task["model_info"]
            model_name = model_info["model_name"]
            if model_info and model_name:
                if "model_storage" in model_info:
                    # 使用模型的run过程
                    # 获取模型的基本信息
                    source_model_info = {
                        "id": model_name,
                        "input": {
                            "type": "hdfs",
                            "format": model_info["algorithm_name"],
                            "path": model_info["modelStorageId"]["path"],
                        },
                        "name": model_name,
                        "type": "model",
                    }
                    source_info[model_name] = source_model_info

        # 获取表的信息,来自parents字段，这里有一种情况:即有子查询的情况下对应的parent是不存在的
        # 在一个sql里，如果内部使用的某个表不存在，在解析阶段就报错
        # 如此到当前步骤仍旧没有报错，说明依赖的表为内部生成表，不需要放到source内
        parent_list = parsed_task["parents"]
        for parent in parent_list:
            logger.info("parent:" + parent)
            source_table_info = {}
            table_info = ResultTableHelper.get_result_table(parent, not_found_raise_exception=False)
            if table_info:
                table_fields = ResultTableHelper.get_result_table_fields(parent)
                source_filed_list = []
                for field in table_fields:
                    source_field = {
                        "origin": field["origins"] if field["origins"] else [],
                        "type": field["field_type"],
                        "field": field["field_name"],
                        "description": field["description"],
                    }
                    source_filed_list.append(source_field)
                table_storage = ResultTableHelper.get_result_table_storage(parent, "hdfs")
                connection_info = json.loads(table_storage["hdfs"]["storage_cluster"]["connection_info"])
                source_table_info = {
                    "id": parent,
                    "name": parent,
                    "input": {
                        "type": "hdfs",
                        "format": table_storage["hdfs"]["data_type"],
                        "path": "{}{}".format(
                            connection_info["hdfs_url"],
                            table_storage["hdfs"]["physical_table_name"],
                        ),
                        "table_type": TABLE_TYPE.QUERY_SET.value,
                    },
                    "fields": source_filed_list,
                    "type": "data",
                }
                if table_storage["hdfs"]["data_type"] == "iceberg":
                    iceberg_config = {
                        "physical_table_name": table_storage["hdfs"]["physical_table_name"],
                        "hdfs_config": StorekitHelper.get_hdfs_conf(parent),
                    }
                    source_table_info["input"]["iceberg_config"] = iceberg_config

            source_info[parent] = source_table_info

        return source_info

    @classmethod
    def format_transform(cls, parsed_task, source_info={}, project_id=0):
        table_name = parsed_task["table_name"]
        transform_info = {}
        transform = {
            "id": table_name,
            "name": table_name,
            "type": "data",
            "task_type": parsed_task["task_type"],
            "parents": list(source_info.keys()),
            "processor": parsed_task["processor"],
            "interpreter": parsed_task["interpreter"],
            "fields": [cls.generate_new_field(field) for field in parsed_task["fields"]],
            "description": table_name,
        }
        if parsed_task["task_type"] == PARSED_TASK_TYPE.SUB_QUERY.value:
            # 加入udf相关属性
            udfs = parsed_task["udfs"]
            if udfs:
                transform["udfs"] = udfs
        """
        if source_info:
            transform['parents'] = source_info.keys()
        """
        transform_info[table_name] = transform
        return transform_info

    @classmethod
    def generate_new_field(cls, field):
        new_field = field.copy()
        new_field["origin"] = field["origins"][0]
        del new_field["origins"]
        return new_field

    @classmethod
    def format_sink(cls, parsed_task):
        table_name = parsed_task["table_name"]
        fields = []
        for field in parsed_task["fields"]:
            fields.append(cls.generate_new_field(field))
        table_storage = ResultTableHelper.get_result_table_storage(table_name, "hdfs")
        connection_info = json.loads(table_storage["hdfs"]["storage_cluster"]["connection_info"])
        sink_info = {}
        sink = {
            "id": table_name,
            "name": table_name,
            "type": "data",
            "fields": fields,
            "output": {
                "type": "hdfs",
                "format": table_storage["hdfs"]["data_type"],
                "path": "{}{}".format(
                    connection_info["hdfs_url"],
                    table_storage["hdfs"]["physical_table_name"],
                ),
                "mode": parsed_task["write_mode"],
                "table_type": TABLE_TYPE.QUERY_SET.value,
            },
        }
        if table_storage["hdfs"]["data_type"] == "iceberg":
            iceberg_config = {
                "physical_table_name": table_storage["hdfs"]["physical_table_name"],
                "hdfs_config": StorekitHelper.get_hdfs_conf(table_name),
            }
            sink["output"]["iceberg_config"] = iceberg_config
        sink_info[table_name] = sink
        return sink_info

    @classmethod
    def get_config_input(cls, model_config):
        transform_config = model_config["transform"]
        if transform_config["task_type"] == PARSED_TASK_TYPE.SUB_QUERY.value:
            return cls.get_subquery_config_input(model_config)
        else:
            return cls.get_mlsql_config_input(model_config)

    @classmethod
    def get_subquery_config_input(cls, model_config):
        """
        从子查询中解析出整个算法的输入
        @param model_config 提取生成的第一个source及第一个transform
        """
        transform_config = model_config["transform"]
        processor_args = transform_config["processor"]["args"]
        head_config = model_config["source"]
        head_fields = head_config["fields"]
        head_field_map = {field["field"]: field for field in head_fields}
        transform_input = {"feature_columns": [], "predict_args": []}

        columns = processor_args["column"]
        for column in columns:
            field_obj = head_field_map[column]
            feature = {
                "field_name": column,
                "field_type": field_obj["type"],
                "field_alias": field_obj["description"],
                "used_by": "user",
            }
            transform_input["feature_columns"].append(feature)
        return transform_input

    @classmethod
    def get_mlsql_config_input(cls, model_config):
        transform_config = model_config["transform"]
        head_config = model_config["source"]
        head_fields = head_config["fields"]
        head_field_map = {field["field"]: field for field in head_fields}
        transform_input = {"feature_columns": [], "predict_args": [], "fields": []}
        alg = None
        args = {}
        if "processor" in transform_config:
            alg = transform_config["processor"]["name"]
            args = transform_config["processor"]["args"]
        algorithm = AlgorithmVersionHandler.get_alorithm_by_name(
            "{framework}_{name}".format(framework="spark", name=alg)
        )
        alg_config = json.loads(algorithm.config)
        spark_feature_columns_mapping = alg_config["feature_columns_mapping"]["spark"]
        predict_args = alg_config["predict_args"]
        single_feature_column_list = (
            spark_feature_columns_mapping["single"] if "single" in spark_feature_columns_mapping else []
        )
        multi_feature_column_list = (
            spark_feature_columns_mapping["multi"] if "multi" in spark_feature_columns_mapping else []
        )
        # 获取用户输入的参数名称
        arg_names = list(args.keys())
        # 根据用户输入的名称，确定用户参数的详细信息：类型，候选值等
        for name in arg_names:
            # 这里注意real_name与field_name的关系，我们配置中都是real_name,而前端传入的值却是field_name（因为国际化的问题，real_name是可能重复的，但field_name不会）
            find_feature = False
            for single_feature_column in single_feature_column_list:
                if name == single_feature_column["real_name"]:
                    # 说明是单输入参数，解析此单输入的具体值,比如 input_col=a这种
                    # single_feature_column['is_composite'] = False
                    single_feature = {
                        "field_name": args[name],
                        "field_type": head_field_map[args[name]]["type"],
                        "field_alias": head_field_map[args[name]]["description"],
                        "data_field_name": args[name],
                        "used_by": "user",
                    }
                    transform_input["feature_columns"].append(single_feature)
                    find_feature = True
                    break
            if not find_feature:
                for multi_feature_column in multi_feature_column_list:
                    if name == multi_feature_column["real_name"]:
                        # 说明是多输入参数，比如input_cols=[a,b,c]
                        # 这里就需要解析interpreter
                        interpreter = transform_config["interpreter"]
                        interpreter_obj = interpreter[args[name]]
                        interpreter_value_list = interpreter_obj["value"]
                        for item in interpreter_value_list:
                            single_feature = {
                                "field_name": item,
                                "field_type": head_field_map[item]["type"],
                                "field_alias": head_field_map[item]["description"],
                                "data_field_name": item,
                                "used_by": "user",
                            }
                            transform_input["feature_columns"].append(single_feature)
                        find_feature = True
                        break

            if not find_feature:
                # 不在feature_columns，说明不是输入参数，可能是普通的预测参数
                for args_item in predict_args:
                    if name == args_item["real_name"]:
                        transform_input["predict_args"].append(args_item)
        # 模型的输入中，除了算法的args参数外，还有一些直接从源表中提取的fields，这些也应该是算法的输入字段
        args_feature_ids = [feature["field_name"] for feature in transform_input["feature_columns"]]
        for field_item in transform_config["fields"]:
            if field_item["origin"] and field_item["field"] not in args_feature_ids:
                # 此transform的结果中的确存在直接从源表中获取的列，并且尚未加入到最终的输入中
                # 需要作为模型的输入字段
                origin_field = field_item["origin"]
                if origin_field:
                    single_feature = {
                        "field_name": origin_field,
                        "field_type": head_field_map[origin_field]["type"],
                        "field_alias": head_field_map[origin_field]["description"],
                        "data_field_name": origin_field,
                        "used_by": "user",
                    }
                    transform_input["feature_columns"].append(single_feature)
        return transform_input

    @classmethod
    def get_config_output(cls, sink_config):
        output_info = []
        for config_id in sink_config:
            fields = sink_config[config_id]["fields"]
            output_info = fields

        return output_info

    def generate_debug_config(self):
        DEBUG_URL = BASE_DATAFLOW_URL + "modeling/debugs"
        debug_info = filter_debug_metric_log(debug_id=self.job_id)[0]
        processor_logic = json.loads(debug_info.job_config)
        debug_params = {
            "debug_id": debug_info.debug_id,
            "debug_rest_api_url": "{debug_url}/{debug_id}/debug_result_info/".format(
                debug_url=DEBUG_URL, debug_id=self.job_id
            ),
            "debug_exec_id": str(debug_info.job_id),
        }
        cluster_group = settings.MLSQL_CLUSEER_GRUUP
        default_cluster_group = settings.MLSQL_DEFAULT_CLUSTER_GROUP
        default_cluster_name = settings.MLSQL_DEFAULT_CLUSTER_NAME
        cluster_group, cluster_name = ModelingUtils.get_cluster_group(
            cluster_group, default_cluster_group, default_cluster_name
        )
        url = BASE_DATAFLOW_URL
        url = url.rstrip("/").rstrip("dataflow").rstrip("/")
        final_transform = {
            "job_id": self.job_id,
            "job_name": self.job_id,
            "job_type": self.job_type,
            "run_mode": self.run_mode,
            "time_zone": "Asia/Shanghai",
            "resource": {"cluster_group": cluster_group, "queue_name": cluster_name},
            "nodes": processor_logic,
            "debug": debug_params,
            "schedule_time": int(time.time() * 1000),
            "data_flow_url": url,
        }
        return final_transform
