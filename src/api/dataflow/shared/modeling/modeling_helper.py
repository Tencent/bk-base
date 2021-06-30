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
import re

from common.django_utils import DataResponse
from rest_framework.response import Response

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.exceptions.comp_exceptions import (
    CreateJobError,
    CreateModelProcessingException,
    DDLSqlExecuteError,
    DropTableNotExistsError,
    StartJobError,
    UpdateModelError,
)
from dataflow.shared.api.modules.modeling import ModelingAPI
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.datalab.datalab_helper import DataLabHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class ModelingHelper(object):
    @staticmethod
    def get_model(model_name):
        res = ModelingAPI.basic_model.retrieve({"model_name": model_name})
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_model_processing(args):
        res = ModelingAPI.processing.bulk(args)
        if not res.is_success():
            raise CreateModelProcessingException(message=res.message, code=res.code)
        else:
            return res.data

    @staticmethod
    def create_model_job(args):
        res = ModelingAPI.job.create(args)
        if not res.is_success():
            raise CreateJobError(message=res.message, code=res.code)
        else:
            return res.data

    @staticmethod
    def update_modeling_job(**kwargs):
        res = ModelingAPI.job.update(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_job(job_id):
        request_params = {"job_id": job_id}
        res = ModelingAPI.job.delete(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def start_model_job(args):
        res = ModelingAPI.job.start(args)
        if not res.is_success():
            raise StartJobError(message=res.message, code=res.code)
        else:
            return res.data

    @staticmethod
    def stop_model_job(job_id):
        args = {"job_id": job_id}
        res = ModelingAPI.job.stop(args)
        res_util.check_response(res)
        return Response(job_id)

    @staticmethod
    def check_mlsql_job(job_id):
        args = {"job_id": job_id}
        res = ModelingAPI.job.sync_status(args)
        return res

    @staticmethod
    def clear_mlsql_job(job_id, status, bk_username=None):
        args = {"job_id": job_id, "status": status, "bk_username": bk_username}
        res = ModelingAPI.job.clear(args)
        res_util.check_response(res)
        clear_resp = DataResponse(result=True, code=res.code)
        clear_resp.message = res.message
        clear_resp.data = res.data
        return clear_resp

    @staticmethod
    def delete_model(model_name, active=None, notebook_info=None):

        params = {
            "model_name": model_name,
        }
        if notebook_info:
            params["bk_username"] = notebook_info["bk_username"]
        if not active:
            params["active"] = 0
        else:
            params["active"] = active
        res = ModelingAPI.basic_model.delete(params)
        if not res.is_success():
            raise DDLSqlExecuteError(code=res.code, message_kv={"content": res.message})
        return res.data

    @staticmethod
    def update_model(model_name, params):
        params["model_name"] = model_name
        res = ModelingAPI.basic_model.update(params)
        if not res.is_success():
            raise UpdateModelError(message_kv={"content": res.message})

    """
    @staticmethod
    def show_models(ddl_data=None):
        params = {
            'bk_username': get_request_username()
        }
        res = ModelingAPI.basic_model.list(params)
        res_util.check_response(res)
        if not res.is_success():
            raise DDLSqlExecuteError(code=res.code, message_kv={'content': res.message})
        else:
            model_list = res.data
            if not ddl_data:
                return model_list
            else:
                result_list = []
                match_regex = '^' + ddl_data.replace('%', '(.)*').replace('_', '.') + '$'
                for model in model_list:
                    model_name = model['model_name']
                    if re.search(match_regex, model_name):
                        result_list.append(model)
                return result_list
    """

    @staticmethod
    def show_models(notebook_id, ddl_data=None):
        output = DataLabHelper.get_notebook_models(notebook_id)
        result_list = []
        if not ddl_data:
            for model_name in output["models"]:
                result_list.append(model_name)
                # model_res = ModelingAPI.basic_model.retrieve({'model_name': model_name})
                # result_list.append(model_res.data)
        else:
            match_regex = "^" + ddl_data.replace("%", "(.)*").replace("_", ".") + "$"
            for model_name in output["models"]:
                if re.search(match_regex, model_name):
                    # model_res = ModelingAPI.basic_model.retrieve({'model_name': model_name})
                    # result_list.append(model_res.data)
                    result_list.append(model_name)
        return result_list

    @staticmethod
    def show_tables(notebook_id, ddl_data=None):
        output = DataLabHelper.get_notebook_models(notebook_id)
        result_list = []
        if not ddl_data:
            for table_name in output["result_tables"]:
                result_list.append(table_name)
        else:
            result_list = []
            match_regex = "^" + ddl_data.replace("%", "(.)*").replace("_", ".") + "$"
            for table_name in output["result_tables"]:
                if re.search(match_regex, table_name):
                    result_list.append(table_name)
        return result_list

    @staticmethod
    def drop_table(result_table_id, notebook_id, cell_id, if_exists=False):
        table_info = ResultTableHelper.get_result_table(result_table_id, not_found_raise_exception=False)
        if not table_info:
            # 表不存在
            message = "表不存在:" + result_table_id
            if not if_exists:
                raise DropTableNotExistsError(
                    message_kv={"content": result_table_id},
                    errors={"name": result_table_id},
                )
            else:
                return message
        params = {
            "result_table_id": result_table_id,
            "notebook_id": notebook_id,
            "cell_id": cell_id,
        }
        res = ModelingAPI.model_queryset.delete_table(params)
        if not res.is_success():
            raise DDLSqlExecuteError(message_kv={"content": res.message})
        else:
            # 先删除附属的系统表
            ModelingHelper.delete_parent_system_table(result_table_id, notebook_id, cell_id)
            # 再删除真正的实体信息
            ProcessingBatchInfoHandler.delete_proc_batch_info(result_table_id)
            return res.data

    @staticmethod
    def delete_parent_system_table(processing_id, notebook_id, cell_id):
        delete_tables = []
        logger.info("processing id:" + processing_id)
        data_processing = DataProcessingHelper.get_data_processing(processing_id)
        logger.info("data processing:" + json.dumps(data_processing))
        if data_processing:
            processing_inputs = data_processing["inputs"]
            for data_set in processing_inputs:
                input_table = data_set["data_set_id"]
                parent_processing = DataProcessingHelper.get_data_processing(input_table)
                if parent_processing["generate_type"] == "system":
                    delete_tables.append(input_table)
            logger.info("parents need to delete:" + json.dumps(delete_tables))
            for table_item in delete_tables:
                params = {
                    "result_table_id": table_item,
                    "notebook_id": notebook_id,
                    "cell_id": cell_id,
                }
                res = ModelingAPI.model_queryset.delete_table(params)
                if not res.is_success():
                    raise DDLSqlExecuteError(message_kv={"content": res.message})
                else:
                    # 表删除成功，删除对应的Processing
                    ProcessingBatchInfoHandler.delete_proc_batch_info(table_item)
                    return res.data

    @staticmethod
    def truncate_table(result_table_id, notebook_id, cell_id):
        params = {
            "result_table_id": result_table_id,
            "notebook_id": notebook_id,
            "cell_id": cell_id,
        }
        res = ModelingAPI.model_queryset.truncate_table(params)
        if not res.is_success():
            raise DDLSqlExecuteError(message_kv={"content": res.message})
        else:
            return res.data

    @staticmethod
    def show_sql(model_name, notebook_id, output_type):
        res = DataLabHelper.get_notebook_model_detail(model_name, notebook_id, output_type)
        if not res.is_success():
            raise DDLSqlExecuteError(message_kv={"content": res.message})
        else:
            return res.data

    @staticmethod
    def create_multi_processings(**args):
        res = ModelingAPI.processing.multi_save_processings(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_multi_processings(**args):
        res = ModelingAPI.processing.multi_update_processings(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_multi_processings(**args):
        # delete_params = {
        #     'processing_id': processing_id
        # }
        res = ModelingAPI.processing.multi_delete_processings(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_multi_jobs(**args):
        res = ModelingAPI.job.multi_create_jobs(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_multi_jobs(**args):
        # update_args = {
        #     'job_id': job_id,
        #     'jobserver_config': jobserver_config
        # }
        res = ModelingAPI.job.multi_update_jobs(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def start_multi_jobs(**args):
        # start_args = {
        #     'job_id': job_id,
        #     'is_restart': is_restart
        # }
        res = ModelingAPI.job.multi_start_jobs(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def stop_multi_jobs(**args):
        # stop_args = {
        #     'job_id': job_id,
        #     'project_id': project_id
        # }
        res = ModelingAPI.job.multi_stop_jobs(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_multi_jobs(**args):
        # delete_args = {
        #     'job_id': job_id
        # }
        res = ModelingAPI.job.multi_delete_jobs(args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_debug(heads, tails, **kwargs):
        kwargs.update({"heads": heads, "tails": tails})
        res = ModelingAPI.debugs.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_debug_basic_info(debug_id, **kwargs):
        kwargs.update({"debug_id": debug_id})
        res = ModelingAPI.debugs.basic_info(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_debug_node_info(debug_id, result_table_id, **kwargs):
        kwargs.update({"debug_id": debug_id, "result_table_id": result_table_id})
        res = ModelingAPI.debugs.node_info(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def stop_debug(debug_id, **kwargs):
        kwargs.update({"debug_id": debug_id})
        res = ModelingAPI.debugs.stop(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_processing(**request_params):
        res = ModelingAPI.processing.create(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_processing(**request_params):
        res = ModelingAPI.processing.update(request_params)
        res_util.check_response(res, self_message=False)
        return res.data
