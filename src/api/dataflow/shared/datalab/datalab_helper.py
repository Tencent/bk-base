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

from common.local import get_request_username

from dataflow.shared.api.modules.datalab import DataLabAPI
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class DataLabHelper(object):
    @staticmethod
    def create_queryset_by_dp(notebook_id, cell_id, data_processing_info):
        res = DataLabAPI.query_set.create(
            {
                "notebook_id": notebook_id,
                "cell_id": cell_id,
                "result_tables": data_processing_info["result_tables"],
                "data_processings": data_processing_info["data_processings"],
                "bkdata_authentication_method": data_processing_info["bkdata_authentication_method"],
                "bk_username": data_processing_info["bk_username"],
            }
        )
        res_util.check_response(res, enable_errors=True)

    @staticmethod
    def create_query_set(notebook_id, cell_id, table_name, fields, inputs, bk_username=None):
        table_params = {"result_table_id": table_name, "fields": fields}
        data_processings = [
            {
                "processing_id": table_params["result_table_id"],
                "inputs": inputs,
                "outputs": [table_name],
            }
        ]
        data_processing = {
            "bkdata_authentication_method": "user",
            "result_tables": [table_params],
            "data_processings": data_processings,
        }

        if bk_username is None:
            data_processing["bk_username"] = get_request_username()
        else:
            data_processing["bk_username"] = bk_username

        DataLabHelper.create_queryset_by_dp(notebook_id, cell_id, data_processing)

    @staticmethod
    def truncate_query_set(result_table_id, notebook_id, cell_id, bk_username=None):
        if bk_username is None:
            bk_username = get_request_username()
        params = {
            "notebook_id": notebook_id,
            "cell_id": cell_id,
            "result_table_id": result_table_id,
            "bk_username": bk_username,
            "bkdata_authentication_method": "user",
        }
        res = DataLabAPI.query_set_truncate.truncate(params)
        res_util.check_response(res, enable_errors=True)

    @staticmethod
    def delete_query_set(result_table_id, notebook_id, cell_id, bk_username=None):
        param = {
            "notebook_id": notebook_id,
            "cell_id": cell_id,
            "result_table_id": result_table_id,
            "bkdata_authentication_method": "user",
            "processing_id": result_table_id,
        }

        if bk_username is None:
            param["bk_username"] = get_request_username()
        else:
            param["bk_username"] = bk_username

        res = DataLabAPI.query_set.delete(param)
        res_util.check_response(res, enable_errors=True)

    @staticmethod
    def get_notebook_models(notebook_id):
        param = {"notebook_id": notebook_id}
        res = DataLabAPI.query_notebook_model.list(param)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_notebook_model_detail(model_name, notebook_id, output_type):
        param = {
            "notebook_id": notebook_id,
            "output_name": model_name,
            "output_type": output_type,
        }
        res = DataLabAPI.query_notebook_model_detail.retrieve(param)
        return res
