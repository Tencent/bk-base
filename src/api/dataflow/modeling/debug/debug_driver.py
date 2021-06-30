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
import uuid
from urllib.parse import unquote

from common.bklanguage import BkLanguage
from common.local import get_request_username
from conf.dataapi_settings import MLSQL_NODE_LABEL

from dataflow.batch.debug import debug_driver as batch_debug_driver
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.debug.debugs import filter_debug_metric_log, insert_debug_metric_log
from dataflow.modeling.job.job_config_driver import generate_workflow_config_by_list
from dataflow.modeling.settings import TABLE_TYPE
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.stream.debug import debug_driver as stream_debug_driver


class ModelingDebugDriver(object):
    def __init__(self, debug_id):
        self.debug_id = debug_id
        processing_id = filter_debug_metric_log(debug_id=debug_id)[0].processing_id
        processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
        if processing_info:
            # batch
            self.debug_driver = batch_debug_driver
            self.job_type = processing_info.component_type
        else:
            # stream
            self.debug_driver = stream_debug_driver
            self.job_type = processing_info.component_type

    def get_basic_info(self):
        return self.debug_driver.get_basic_info(self.debug_id)

    def get_node_info(self, args, language=BkLanguage.CN):
        return self.debug_driver.get_node_info(args, self.debug_id)

    @classmethod
    def create_debug(cls, params):
        tails_str = params["tails"]
        head_str = params["heads"]
        geog_area_code = params["geog_area_code"]
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        created_by = get_request_username()
        debug_id = "debug_" + str(uuid.uuid4()).replace("-", "")
        # todo 这里需要考虑如果要拆分任务的话，debug任务要如何处理
        workflow_config = generate_workflow_config_by_list([debug_id], run_mode="debug")
        logger.info(workflow_config)
        exec_id = cls.submit_jobnavi(debug_id, workflow_config, created_by, geog_area_code, cluster=cluster_id)
        logger.info("debug execution id:" + str(exec_id))
        job_config_map = {}
        for processing_id in batch_debug_driver.load_chain(head_str, tails_str):
            try:
                processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
                job_config = cls.generate_debug_config(processing_info)
                job_config_map[processing_id] = job_config
            except Exception:
                pass
        merged_job_config = cls.get_merged_debug_job_config(job_config_map)
        for processing_id in job_config_map:
            insert_debug_metric_log(
                debug_id=debug_id,
                processing_id=processing_id,
                job_id=exec_id,
                operator=created_by,
                job_config=json.dumps(merged_job_config),
                job_type="batch",
            )
        return debug_id

    @classmethod
    def get_merged_debug_job_config(cls, job_config_map):
        source_map = {}
        transform_map = {}
        sink_map = {}

        # 整理得到所有source, transform与sink
        for processing_id in job_config_map:
            job_config = job_config_map[processing_id]
            sink_map.update(job_config["sink"])
            transform_map.update(job_config["transform"])
            source_map.update(job_config["source"])
        # 由于节点之前可能有依赖关系，我们要删除source中依赖的其它transform的结果，而仅保留最原始的数据输入与模型输入
        simple_source_map = {}

        # 注意：这里的transform_map中的key与每个transform内部的id是不一致的，内部的id才是真正的表名或是模型名
        # 如果需要判断某个表是否为中间表，那就需要判断source内某个表内是否在transform内出现
        transform_id_list = []
        for transform_id in transform_map:
            transform_id_list.append(transform_map[transform_id]["id"])
        for source in source_map:
            if source not in transform_id_list:
                simple_source_map[source] = source_map[source]

        final_job_config = {
            "source": simple_source_map,
            "transform": transform_map,
            "sink": sink_map,
        }
        return final_job_config

    @classmethod
    def generate_debug_config(cls, process_info):
        processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(process_info.processing_id)
        result_table_storage = ResultTableHelper.get_result_table_storage(process_info.processing_id, "hdfs")["hdfs"]
        result_data_type = result_table_storage["data_type"]
        result_table_connect = json.loads(result_table_storage["storage_cluster"]["connection_info"])
        table_path = "{hdfs_url}/{hdfs_path}".format(
            hdfs_url=result_table_connect["hdfs_url"],
            hdfs_path=result_table_storage["physical_table_name"],
        )
        processor_logic = json.loads(processing_info.processor_logic)
        sink_info = processor_logic["sink"]
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

        return processor_logic

    @classmethod
    def submit_jobnavi(cls, debug_id, workflow_config, created_by, geog_area_code, cluster):
        jobnavi = JobNaviHelper(geog_area_code, cluster)
        jobnavi_args = {
            "schedule_id": debug_id,
            "description": " Project {} submit by {}".format(debug_id, created_by),
            "type_id": "workflow",
            "active": True,
            "exec_oncreate": True,
            "extra_info": str(json.dumps(workflow_config)),
            "node_label": MLSQL_NODE_LABEL,
        }
        res = jobnavi.create_schedule_info(jobnavi_args)
        return res

    def stop_debug(self, geog_area_code, cluster_id):
        return self.debug_driver.stop_debug(self.debug_id, geog_area_code, cluster_id)

    def set_error_data(self, params):
        self.debug_driver.set_error_data(params, self.debug_id)

    def update_metric_info(self, params):
        self.debug_driver.update_metric_info(params, self.debug_id)

    def set_result_data(self, params):
        params["result_data"] = json.dumps(params.get("result_data"))
        result_data = str(params.get("result_data"))
        result_data = json.loads(unquote(result_data).decode("UTF-8"))
        logger.info(json.dumps(result_data))
        self.debug_driver.set_result_data(params, self.debug_id)
