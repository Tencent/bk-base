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

from dataflow.shared.api.modules.meta import MetaApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util

RESULT_TABLE_NOT_FOUND_ERR = "1521020"
RESULT_TABLE_DELETE_ERR = "1521022"
TDW_RESULT_TABLE_BEEN_USED_ERR = "1521303"


class ResultTableHelper(object):
    @staticmethod
    def set_result_table(result_table):
        res = MetaApi.result_tables.create(result_table)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_result_table(
        result_table_id,
        related=None,
        not_found_raise_exception=True,
        check_usability=False,
        extra=False,
    ):
        if not related:
            related = []
        res = MetaApi.result_tables.retrieve(
            {
                "result_table_id": result_table_id,
                "related": related,
                "check_usability": check_usability,
                "extra": extra,
            }
        )
        try:
            res_util.check_response(res)
        except ApiRequestError as e:
            if not_found_raise_exception or e.code != RESULT_TABLE_NOT_FOUND_ERR:
                raise e
            return None
        return res.data

    @staticmethod
    def update_result_table(result_table):
        res = MetaApi.result_tables.update(result_table)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_result_table(result_table_id):
        res = MetaApi.result_tables.delete({"result_table_id": result_table_id})
        try:
            res_util.check_response(res)
        except ApiRequestError as e:
            if e.code == RESULT_TABLE_NOT_FOUND_ERR:
                sys_logger.info(
                    "bk_username(%s) try to delete a not exist result_table(%s)"
                    % (get_request_username(), result_table_id)
                )
                return result_table_id
            else:
                sys_logger.exception(e)
                raise e
        return res.data

    @staticmethod
    def get_result_table_fields(result_table_id):
        res = MetaApi.result_tables.fields({"result_table_id": result_table_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_result_table_storage(result_table_id, cluster_type=None):
        request_params = {"result_table_id": result_table_id}
        if cluster_type:
            request_params["cluster_type"] = cluster_type
        res = MetaApi.result_tables.storages(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_result_table_type(result_table_id):
        result_table = ResultTableHelper.get_result_table(result_table_id)
        return result_table["result_table_type"]["result_table_type_code"]

    @staticmethod
    def get_processing_type(result_table_id):
        result_table = ResultTableHelper.get_result_table(result_table_id)
        return result_table["processing_type"]

    @staticmethod
    def get_tdw_app_group_info(app_group_name, user):
        res = MetaApi.tdw_app_group.retrieve({"app_group_name": app_group_name, "user": user})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_mine_tdw_app_group(user):
        res = MetaApi.tdw_app_group.mine({"bk_username": user})
        res_util.check_response(res)
        return res.data
