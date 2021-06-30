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

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.shared.datalab.datalab_helper import DataLabHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.modeling.modeling_helper import ModelingHelper


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
                        ModelingHelper.delete_model(processing_id, notebook_info=notebook_info)
                        logger.info("delete model processing:" + processing_id)
                        ModelingUtils.delete_data_processing(processing_id)
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
        processor_loginc = json.loads(processing_info.processor_logic)
        operate_type = processor_loginc["operate_type"]
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
    def delete_data_processing(processing_id):
        DataProcessingHelper.delete_data_processing(processing_id)
        """
        url = BASE_META_URL + 'data_processings/' + processing_id + '/'
        resp = requests.delete(url)
        return resp.json()['result']
        """

    @staticmethod
    def update_model_status(model_names, status="trained", active=1):
        for name in model_names:
            model_info = ModelingHelper.get_model(name)
            if model_info:
                params = {"model_name": name, "status": status, "active": active}
                ModelingHelper.update_model(name, params)
