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

from common.local import get_request_username

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.exceptions.comp_exceptions import HDFSNotExistsError
from dataflow.modeling.handler.mlsql_model_info import MLSqlModelInfoHandler
from dataflow.modeling.settings import PARSED_TASK_TYPE
from dataflow.shared.datalab.datalab_helper import DataLabHelper
from dataflow.shared.handlers import processing_cluster_config
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class ModelingUtils(object):
    @staticmethod
    def clear(processing_ids, notebook_info):
        if processing_ids:
            for processing_id in processing_ids:
                try:
                    (
                        delete_model,
                        delete_table,
                        delete_dp,
                        delete_processing,
                    ) = ModelingUtils.get_need_clear_label(processing_id)
                    if delete_model:
                        # drop models
                        logger.info("delete model:" + processing_id)
                        ModelingUtils.delete_model(processing_id, notebook_info=notebook_info)
                        logger.info("delete model processing:" + processing_id)
                        ModelingUtils.delete_data_processing(processing_id, notebook_info)
                    if delete_processing:
                        # drop processings
                        logger.info("delete processing:" + processing_id)
                        ProcessingBatchInfoHandler.delete_proc_batch_info(processing_id)
                    if delete_dp and delete_table:
                        logger.info("delete table:" + processing_id)
                        ModelingUtils.delete_result_tables(processing_id, **notebook_info)
                except Exception as e:
                    logger.error("clear error %s" % e)

    @staticmethod
    def get_need_clear_label(processing_id):
        delete_model = False
        delete_table = False
        delete_dp = False
        delete_processing = False
        processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
        processor_logic = json.loads(processing_info.processor_logic)
        logic = processor_logic["logic"]
        task_type = logic["task_type"]
        if task_type == PARSED_TASK_TYPE.SUB_QUERY.value:
            # 如果是子查询，则对应表及DP全部清除
            delete_model = False
            delete_dp = True
            delete_table = True
            delete_processing = True
        else:
            operate_type = processor_logic["operate_type"]
            if operate_type == "train":
                delete_model = True
                delete_table = False
                delete_processing = True
                delete_dp = True
            elif operate_type == "insert":
                delete_model = False
                delete_table = False
                delete_processing = False
                delete_dp = False
            elif operate_type == "create":
                delete_model = False
                delete_table = True
                delete_processing = True
                delete_dp = True
            else:
                pass
        return delete_model, delete_table, delete_dp, delete_processing

    @staticmethod
    def delete_result_tables(result_table_id, notebook_id=None, cell_id=None, bk_username=None):
        DataLabHelper.delete_query_set(result_table_id, notebook_id, cell_id, bk_username)

    @staticmethod
    def truncate_result_tables(result_table_id, notebook_id=None, cell_id=None, bk_username=None):
        DataLabHelper.truncate_query_set(result_table_id, notebook_id, cell_id, bk_username)

    @staticmethod
    def delete_data_processing(processing_id, notebook_info):
        bk_username = get_request_username()
        if not bk_username and notebook_info:
            bk_username = notebook_info["bk_username"]
        DataProcessingHelper.delete_dp_with_user(processing_id, bk_username=bk_username)

    @staticmethod
    def update_model_status(model_names, status="trained", active=1):
        for name in model_names:
            model_info = MLSqlModelInfoHandler.fetch_model_by_name(name)
            if model_info:
                params = {"status": status, "active": active}
                MLSqlModelInfoHandler.update_model(name, **params)

    @staticmethod
    def get_cluster_group(
        cluster_group,
        default_cluster_group,
        default_cluster_name,
        component_type="spark",
    ):
        cluster_config = ModelingUtils.get_cluster_config(cluster_group, component_type)
        if cluster_config:
            logger.info("get pointed cluster config...")
            return cluster_group, cluster_config.cluster_name
        else:
            cluster_config = ModelingUtils.get_cluster_config(default_cluster_group, component_type)
            if cluster_config:
                logger.info("get default cluster config...")
                return default_cluster_group, cluster_config.cluster_name
            else:
                logger.info("return default cluster config...")
                return default_cluster_group, default_cluster_name

    @staticmethod
    def get_cluster_config(cluster_group, component_type):
        return (
            processing_cluster_config.where(component_type=component_type, cluster_group=cluster_group)
            .order_by("-priority")
            .first()
        )

    @staticmethod
    def get_hdfs_cluster_group(cluster_type, cluster_name):
        # todo 这里需要考虑地域的情况
        cluster_config = StorekitHelper.get_storage_cluster_config(cluster_type, cluster_name)
        if cluster_config:
            return cluster_config["id"], cluster_config["connection"]["hdfs_url"]
        else:
            raise HDFSNotExistsError(message_kv={"content": cluster_name})

    @staticmethod
    def get_hdfs_cluster_group_by_area_code(cluster_type, geog_area_code):
        cluster_config = StorekitHelper.get_default_storage(cluster_type, geog_area_code)
        if cluster_config:
            return cluster_config["id"], cluster_config["connection"]["hdfs_url"]
        else:
            raise HDFSNotExistsError(message_kv={"content": geog_area_code})

    @staticmethod
    def delete_model(model_name, active=None, notebook_info={}):
        created_by = None
        if notebook_info:
            created_by = notebook_info["bk_username"]
        MLSqlModelInfoHandler.delete_model_by_permission(model_name, created_by)

    @staticmethod
    def convert_to_unicode(input_obj):
        if isinstance(input_obj, dict):
            result_dict = {}
            for item_key in input_obj:
                new_item_key = item_key.decode("utf-8")
                result_dict[new_item_key] = ModelingUtils.convert_to_unicode(input_obj[item_key])
            return result_dict
        elif isinstance(input_obj, list):
            result_list = []
            for item in input_obj:
                result_list.append(ModelingUtils.convert_to_unicode(item))
            return result_list
        elif isinstance(input_obj, str):
            return input_obj.decode("utf-8")
        else:
            return input_obj

    @staticmethod
    def convert_to_string(input_obj):
        if isinstance(input_obj, dict):
            result_dict = {}
            for item_key in input_obj:
                if isinstance(item_key, str):
                    new_item_key = str(item_key)
                else:
                    new_item_key = item_key
                result_dict[new_item_key] = ModelingUtils.convert_to_string(input_obj[item_key])
            return result_dict
        elif isinstance(input_obj, list):
            result_list = []
            for item in input_obj:
                result_list.append(ModelingUtils.convert_to_string(item))
            return result_list
        elif isinstance(input_obj, str):
            return str(input_obj)
        else:
            return input_obj
