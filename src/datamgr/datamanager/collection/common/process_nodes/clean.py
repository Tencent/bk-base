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

from api import databus_api

from .base import ProcessNode, ProcessTemplate

logger = logging.getLogger()


class CleanTemplate(ProcessTemplate):
    template = None
    # 以这个文件为例子 template = 'cmdb_topo_clean.json'

    def get_clean_config(self):
        return self.content

    def get_bk_biz_id(self):
        return self.content["bk_biz_id"]

    def get_result_table_name(self):
        return self.content["result_table_name"]


class CleanNode(ProcessNode):
    """
    数据清洗节点
    """

    def build(self):
        bk_biz_id = self.params_template.get_bk_biz_id()
        result_table_name = self.params_template.get_result_table_name()
        processing_id = f"{bk_biz_id}_{result_table_name}"

        clean_info = self.get_clean_info_by_processing_id(processing_id)
        if clean_info is not None:
            logger.info(f"[CleanNode] Clean({processing_id}) has exist ....")
            return {"processing_id": processing_id}

        # 创建清洗配置
        clean_config = self.params_template.get_clean_config()
        databus_api.cleans.create(clean_config, raise_exception=True)

        # 启动清洗任务
        databus_api.start_task({"result_table_id": processing_id}, raise_exception=True)

        return {"processing_id": processing_id}

    @staticmethod
    def get_clean_info_by_processing_id(processing_id):
        """
        读取清洗配置，若为空则表示不存在
        """
        response = databus_api.cleans.retrieve({"processing_id": processing_id})
        return response.data


class IgniteStorageTemplate(ProcessTemplate):
    template = None


class IgniteStorageNode(ProcessNode):
    """
    数据入库节点
    """

    def build(self):
        storage_config = self.params_template.content
        storage_type = storage_config["storage_type"]
        result_table_id = storage_config["result_table_id"]
        raw_data_id = storage_config["raw_data_id"]

        storage_info = self.get_storage_info(raw_data_id, result_table_id, storage_type)
        if storage_info is not None:
            logger.info(
                f"[StorageNode] Storage({result_table_id}:{storage_type}) has exist ...."
            )
            return {"result_table_id": result_table_id}

        # 创建入库配置
        databus_api.data_storages.create(storage_config, raise_exception=True)

        # 启动任务
        databus_api.start_task(
            {"result_table_id": result_table_id, "storages": [storage_type]},
            raise_exception=True,
        )
        return {"result_table_id": result_table_id}

    @staticmethod
    def get_storage_info(raw_data_id, result_table_id, storage_type):
        data = databus_api.data_storages.list({"raw_data_id": raw_data_id}).data
        for d in data:
            if (
                d["result_table_id"] == result_table_id
                and d["storage_type"] == storage_type
            ):
                return d
        return None
