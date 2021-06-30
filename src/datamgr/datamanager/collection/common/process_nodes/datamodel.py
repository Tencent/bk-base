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

from api import dataflow_api, datamanage_api, meta_api

from .base import ProcessNode

logger = logging.getLogger()


class DataModelNode(ProcessNode):
    """
    数据模型构建节点
    """

    def build(self):
        model_config = self.params_template.content
        model_name = model_config["model_name"]
        project_id = model_config["project_id"]

        model_info = self.get_model_info(project_id, model_name)
        if model_info is not None:
            logger.info(
                f"[DataModelNode] DataModel({project_id}:{model_name}) has exist ...."
            )
            model_id = model_info["model_id"]

            if model_info["latest_version"] is None:
                logger.info("[DataModelNode] No latest_version, re-publish datamodel.")
                datamanage_api.datamodels.release(
                    {"model_id": model_id, "version_log": "init"}, raise_exception=True
                )

            return {"model_id": model_id}

        data = datamanage_api.datamodels.import_model(
            model_config, raise_exception=True
        ).data
        model_id = data["model_id"]
        return {"model_id": model_id}

    @staticmethod
    def get_model_info(project_id, model_name):
        data = datamanage_api.datamodels.list(
            {"project_id": project_id, "model_name": model_name}, raise_exception=True
        ).data
        if len(data) > 0:
            return data[0]

        return None


class DataModelInstNode(ProcessNode):
    """
    数据模型应用节点
    """

    def build(self):
        inst_config = self.params_template.content
        bk_biz_id = inst_config["bk_biz_id"]
        table_name = inst_config["main_table"]["table_name"]

        result_table_id = f"{bk_biz_id}_{table_name}"

        result_table_info = self.get_result_table_info(result_table_id)
        if result_table_info is not None:
            logger.info(
                f"[DataModelInstNode] Instance({result_table_id}) has exist ...."
            )
            return {"result_table_id": result_table_id}

        data = datamanage_api.generate_datamodel_instance(
            inst_config, raise_exception=True
        ).data
        flow_id = data["flow_id"]

        dataflow_api.flows.start({"flow_id": flow_id}, raise_exception=True)

        return {"result_table_id": result_table_id}

    @staticmethod
    def get_result_table_info(result_table_id):
        data = meta_api.result_tables.retrieve(
            {"result_table_id": result_table_id}, raise_exception=True
        ).data
        if "result_table_id" in data:
            return data

        return None
