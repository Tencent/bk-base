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

from common.exceptions import ApiRequestError
from common.local import get_request_username
from common.log import sys_logger
from django.utils.translation import ugettext as _

from dataflow.shared.api.modules.meta import MetaApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.meta.erp.erp_helper import ErpHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.meta.transaction.meta_transaction_helper import MetaTransactionHelper

DATA_PROCESSING_NOT_FOUND_ERR = "1521040"


class DataProcessingHelper(object):
    @staticmethod
    def set_data_processing(args):
        res = MetaApi.data_processings.create(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_data_processing(args):
        res = MetaApi.data_processings.update(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_data_processing(processing_id, with_data=False):
        res = MetaApi.data_processings.delete({"processing_id": processing_id, "with_data": with_data})
        try:
            res_util.check_response(res)
        except ApiRequestError as e:
            if e.code == DATA_PROCESSING_NOT_FOUND_ERR:
                sys_logger.info(
                    "bk_username({}) try to delete a not exist processing({})".format(
                        get_request_username(), processing_id
                    )
                )
                return processing_id
            else:
                sys_logger.exception(e)
                raise e
        return res.data

    @staticmethod
    def delete_dp_with_user(processing_id, with_data=False, bk_username=None):
        res = MetaApi.data_processings.delete(
            {
                "processing_id": processing_id,
                "with_data": with_data,
                "bk_username": bk_username,
            }
        )
        try:
            res_util.check_response(res)
        except ApiRequestError as e:
            if e.code == DATA_PROCESSING_NOT_FOUND_ERR:
                sys_logger.info(
                    "bk_username({}) try to delete a not exist processing({})".format(
                        get_request_username(), processing_id
                    )
                )
                return processing_id
            else:
                sys_logger.exception(e)
                raise e
        return res.data

    @staticmethod
    def bulk_delete_dp(processings):
        res = MetaApi.data_processings.bulk({"processings": processings})
        res_util.check_response(res, check_success=False)
        return res.data

    @staticmethod
    def bulk_delete_dp_by_username(processings, bk_username):
        res = MetaApi.data_processings.bulk({"processings": processings, "bk_username": bk_username})
        res_util.check_response(res, check_success=True)
        return res.data

    @staticmethod
    def get_data_processing(processing_id):
        res = MetaApi.data_processings.retrieve({"processing_id": processing_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_dp_via_erp(processing_id):
        """
        注意，暂时无法统一获取地域标签，元数据待支持
        @param processing_id:
        @return:
        """
        retrieve_args = [
            {
                "?:start_filter": "processing_id in ['{}']".format(processing_id),
                "*": True,
                "?:typed": "DataProcessing",
                "~DataProcessingRelation.processing": {
                    "id": True,
                    "~Tag.targets": {"code": True},
                    "data_directing": True,
                    "storage_type": True,
                    "storage_cluster_config_id": True,
                    "data_set_id": True,
                    "data_set_type": True,
                    "channel_cluster_config_id": True,
                },
            }
        ]
        erp_data = ErpHelper.query_via_erp(retrieve_args)["DataProcessing"]
        if not erp_data:
            return {}
        if len(erp_data) != 1:
            sys_logger.info("获取数据处理信息不唯一: %s" % len(erp_data))
            raise ApiRequestError(_("获取数据处理信息不唯一"))
        # 转换输出，与 `get_data_processing` 接口兼容
        data = erp_data[0]
        dp_relations = data.get("~DataProcessingRelation.processing")
        inputs = []
        outputs = []
        for dp_relation in dp_relations:
            tags = []
            for tag_target in dp_relation.get("~Tag.targets", []):
                tags.append(tag_target["code"])
            relation_info = {
                "storage_type": dp_relation["storage_type"],
                "storage_cluster_config_id": dp_relation["storage_cluster_config_id"],
                "data_set_id": dp_relation["data_set_id"],
                "data_set_type": dp_relation["data_set_type"],
                "channel_cluster_config_id": dp_relation["channel_cluster_config_id"],
                "tags": tags,
            }
            if dp_relation["data_directing"] == "input":
                inputs.append(relation_info)
            elif dp_relation["data_directing"] == "outputs":
                outputs.append(relation_info)
        data["inputs"] = inputs
        data["outputs"] = outputs
        del data["~DataProcessingRelation.processing"]
        return data

    @staticmethod
    def check_data_processing(processing_id):
        res = MetaApi.data_processings.retrieve({"processing_id": processing_id})
        try:
            res_util.check_response(res)
        except ApiRequestError as e:
            if e.code != DATA_PROCESSING_NOT_FOUND_ERR:
                raise e
            return False
        return True if res.data else False

    @staticmethod
    def get_data_processing_type(processing_id):
        res = DataProcessingHelper.get_data_processing(processing_id)
        return res["processing_type"]

    @staticmethod
    def meta_transaction(api_operate_list):
        return MetaTransactionHelper.create_meta_transaction(api_operate_list)

    @staticmethod
    def is_result_table_exist(result_table_id):
        return bool(ResultTableHelper.get_result_table(result_table_id, not_found_raise_exception=False))

    @staticmethod
    def is_model_serve_mode_offline(processing_id):
        retrieve_args = [
            {
                "?:filter": 'processing_id="%s"' % processing_id,
                "*": True,
                "?:typed": "DataProcessing",
                "~ModelInstance.data_processing": {"serving_mode": True},
            }
        ]
        erp_data = ErpHelper.query_via_erp(retrieve_args)["DataProcessing"]
        if not erp_data:
            return False
        if len(erp_data) != 1:
            sys_logger.info("获取数据处理信息不唯一: %s" % len(erp_data))
            raise ApiRequestError(_("获取数据处理信息不唯一"))

        data = erp_data[0]
        if "processing_type" in data and data.get("processing_type") == "model":
            return data.get("~ModelInstance.data_processing", [{}])[0].get("serving_mode") in ["offline", "batch"]
        return False
