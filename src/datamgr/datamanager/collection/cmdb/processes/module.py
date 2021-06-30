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
from collection.common.process import BKDFlowProcessor
from collection.common.process_nodes import (
    CleanTemplate,
    IgniteStorageTemplate,
    ProcessTemplate,
)
from collection.conf import constants


class CleanCMDBModuleInfoTemplate(CleanTemplate):
    template = "clean_of_cmdb_module_info.jinja"


class IgniteStorageCMDBModuleInfoTemplate(IgniteStorageTemplate):
    template = "storage_of_cmdb_module_info.jinja"


class DataModelCMDBModuleInfoTemplate(ProcessTemplate):
    template = "datamodel_of_cmdb_module_info.jinja"


class DataModelInstCMDBModuleInfoTemplate(ProcessTemplate):
    template = "datamodel_inst_of_cmdb_module_info.jinja"


BKDFlowProcessor.regiter_process_template(CleanCMDBModuleInfoTemplate)
BKDFlowProcessor.regiter_process_template(IgniteStorageCMDBModuleInfoTemplate)
BKDFlowProcessor.regiter_process_template(DataModelCMDBModuleInfoTemplate)
BKDFlowProcessor.regiter_process_template(DataModelInstCMDBModuleInfoTemplate)


def process_cmdb_module_info():
    process_cmdb_module_config = {
        "pipeline": [
            {
                "process_node": "AccessNode",
                "process_template": "AccessCustomTemplate",
                "process_context": {
                    "bk_biz_id": constants.BKDATA_BIZ_ID,
                    "raw_data_name": constants.CMDB_MODULE_TABLE_NAME,
                    "raw_data_alias": constants.CMDB_MODULE_TABLE_ALIA,
                },
            },
            {
                "process_node": "CleanNode",
                "process_template": "CleanCMDBModuleInfoTemplate",
                "process_context": {
                    "bk_biz_id": constants.BKDATA_BIZ_ID,
                    "raw_data_id": "$0.raw_data_id",
                    "result_table_name": constants.CMDB_MODULE_TABLE_NAME,
                    "result_table_alias": constants.CMDB_MODULE_TABLE_ALIA,
                },
            },
            {
                "process_node": "IgniteStorageNode",
                "process_template": "IgniteStorageCMDBModuleInfoTemplate",
                "process_context": {
                    "bk_biz_id": constants.BKDATA_BIZ_ID,
                    "raw_data_id": "$0.raw_data_id",
                    "result_table_id": f"{constants.BKDATA_BIZ_ID}_{constants.CMDB_MODULE_TABLE_NAME}",
                    "result_table_name": constants.CMDB_MODULE_TABLE_NAME,
                    "result_table_alias": constants.CMDB_MODULE_TABLE_ALIA,
                    "storage_cluster": constants.DEFAULT_IGNITE_CLUSTER,
                },
            },
            {
                "process_node": "AuthProjectDataNode",
                "process_template": "SimpleTemplate",
                "process_context": {
                    "project_id": constants.BKPUB_PROJECT_ID,
                    "bk_biz_id": constants.BKDATA_BIZ_ID,
                    "result_table_id": f"{constants.BKDATA_BIZ_ID}_{constants.CMDB_MODULE_TABLE_NAME}",
                },
            },
            {
                "process_node": "DataModelNode",
                "process_template": "DataModelCMDBModuleInfoTemplate",
                "process_context": {
                    "project_id": constants.BKPUB_PROJECT_ID,
                    "model_name": constants.CMDB_MODULE_DATAMODEL_NAME,
                },
            },
            {
                "process_node": "DataModelInstNode",
                "process_template": "DataModelInstCMDBModuleInfoTemplate",
                "process_context": {
                    "model_id": "$4.model_id",
                    "project_id": constants.BKPUB_PROJECT_ID,
                    "bk_biz_id": constants.BKDATA_BIZ_ID,
                    "input_result_table_id": f"{constants.BKDATA_BIZ_ID}_{constants.CMDB_MODULE_TABLE_NAME}",
                    "table_name": constants.CMDB_MODULE_DATAMODEL_TABLE_NAME,
                    "cluster_name": constants.DEFAULT_IGNITE_CLUSTER,
                },
            },
        ]
    }

    BKDFlowProcessor(process_cmdb_module_config["pipeline"]).build()
