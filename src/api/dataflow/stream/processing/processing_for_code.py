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
from django.utils.translation import ugettext_lazy as _

from dataflow.shared.log import stream_logger as logger
from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import ResultTableExistsError
from dataflow.stream.handlers import processing_stream_info
from dataflow.stream.processing.processing_utils import ProcessingUtils
from dataflow.stream.settings import (
    RELEASE_ENV,
    RELEASE_ENV_TGDP,
    RUN_VERSION,
    STREAM_TYPE,
    USER_GENERATE_TYPE,
    DeployMode,
    ImplementType,
)


class Processing(object):

    _processing_id = None
    _processing_info = {}

    def __init__(self, processing_id):
        self._processing_id = processing_id

    @property
    def processing_info(self):
        if not self._processing_info:
            self._processing_info = processing_stream_info.get(self.processing_id)
        return self._processing_info

    @property
    def processing_id(self):
        return self._processing_id

    @property
    def implement_type(self):
        # TODO: 暂时用 processor_type 替代 implement_type
        implement_type = self.processing_info.processor_type
        if implement_type != ImplementType.CODE.value:
            implement_type = ImplementType.SQL.value
        return implement_type

    @property
    def component_type(self):
        return self.processing_info.component_type

    @property
    def deploy_mode(self):
        if self.implement_type == ImplementType.CODE.value:
            deploy_mode = DeployMode.YARN_CLUSTER.value
        elif RUN_VERSION == RELEASE_ENV or RUN_VERSION == RELEASE_ENV_TGDP:
            deploy_mode = DeployMode.YARN_CLUSTER.value
        else:
            deploy_mode = None
        return deploy_mode

    @property
    def programming_language(self):
        if self.implement_type != ImplementType.CODE.value:
            programming_language = "java"
        else:
            programming_language = json.loads(self.processing_info.processor_logic)["programming_language"]
        return programming_language

    def update_processing(self, args):
        # flink类型任务额外记录支持多版本，并执行code代码检查
        programming_language = args["dict"]["programming_language"]
        code = args["dict"]["code"]
        if self.component_type == "spark_structured_streaming":
            code_check_group = "spark-structured-streaming-code"
        else:
            code_check_group = "stream-code-%s" % self.component_type
        # 代码检查 todo code test
        ProcessingUtils.code_check(programming_language, code, code_check_group, code_check_group)

        # 获取更新前的output
        old_processing_info = MetaApiHelper.get_data_processing(self.processing_id)
        old_output_result_table_ids = []
        for one_output in old_processing_info["outputs"]:
            old_output_result_table_ids.append(one_output["data_set_id"])

        # 获取需要创建的和更新的output result table
        to_create_result_tables = []
        to_update_result_tables = []
        output_result_table_ids = []
        for output in args["outputs"]:
            output_result_table_id = "{}_{}".format(
                output["bk_biz_id"],
                output["result_table_name"],
            )
            if output_result_table_id in output_result_table_ids:
                raise ResultTableExistsError(_("结果表%s已存在，不允许重复创建" % output_result_table_id))
            output_result_table_ids.append(output_result_table_id)
            if output_result_table_id in old_output_result_table_ids:
                to_update_result_tables.append(output_result_table_id)
            else:
                to_create_result_tables.append(output_result_table_id)

        logger.info("The rt %s need to create." % to_create_result_tables)
        logger.info("The rt %s need to update." % to_update_result_tables)

        # 获取需要删除的result table
        to_delete_result_tables = args["delete_result_tables"]

        logger.info("The rt %s need to delete." % to_delete_result_tables)

        # 检查需要创建的rt是否已经存在
        for create_result_table_id in to_create_result_tables:
            if MetaApiHelper.is_result_table_exist(create_result_table_id):
                raise ResultTableExistsError(_("结果表%s已存在，不允许重复创建" % create_result_table_id))

        api_operate_list = []
        # 增加更新data processing的操作
        update_processing_params = {
            "project_id": args["project_id"],
            "processing_id": self.processing_id,
            "processing_alias": args["processing_alias"],
            "processing_type": STREAM_TYPE,
            "generate_type": USER_GENERATE_TYPE,
            "bk_username": get_request_username(),
            "description": args["processing_alias"],
            "inputs": ProcessingUtils.generate_processing_inputs(args["input_result_tables"]),
            "outputs": ProcessingUtils.generate_processing_outputs(output_result_table_ids),
            "tags": args["tags"],
        }
        api_operate_list.append(
            {
                "operate_object": "data_processing",
                "operate_type": "update",
                "operate_params": update_processing_params,
            }
        )
        # 先增加删除result table的操作
        for delete_result_table_id in to_delete_result_tables:
            api_operate_list.append(
                {
                    "operate_object": "result_table",
                    "operate_type": "destroy",
                    "operate_params": {"result_table_id": delete_result_table_id},
                }
            )
        # 增加创建/更新result table的操作
        for output_result_table in args["outputs"]:
            result_table_id = "{}_{}".format(
                output_result_table["bk_biz_id"],
                output_result_table["result_table_name"],
            )
            table_params = {
                "bk_biz_id": output_result_table["bk_biz_id"],
                "project_id": args["project_id"],
                "result_table_id": result_table_id,
                "result_table_name": output_result_table["result_table_name"],
                "result_table_name_alias": output_result_table["result_table_name_alias"],
                "generate_type": USER_GENERATE_TYPE,
                "bk_username": get_request_username(),
                "description": output_result_table["result_table_name_alias"],
                "count_freq": 0,
                "fields": [
                    {
                        "field_name": "timestamp",
                        "field_type": "timestamp",
                        "field_alias": "timestamp",
                        "is_dimension": 0,
                        "origins": None,
                        "field_index": 1,
                    }
                ],
                "tags": args["tags"],
            }
            field_index = 1
            for field in output_result_table["fields"]:
                field_index += 1
                tmp_field = {
                    "field_name": field["field_name"],
                    "field_type": field["field_type"],
                    "field_alias": field["field_alias"],
                    "is_dimension": 0,
                    "origins": None,
                    "field_index": field_index,
                    "roles": {"event_time": field["event_time"]},
                }
                table_params["fields"].append(tmp_field)

            if result_table_id in to_create_result_tables:
                api_operate_list.append(
                    {
                        "operate_object": "result_table",
                        "operate_type": "create",
                        "operate_params": table_params,
                    }
                )
            elif result_table_id in to_update_result_tables:
                api_operate_list.append(
                    {
                        "operate_object": "result_table",
                        "operate_type": "update",
                        "operate_params": table_params,
                    }
                )

        # 信息保存到processing_stream_info
        processor_args = {
            "static_data": args["static_data"],
            "input_result_tables": args["input_result_tables"],
            "output_result_tables": output_result_table_ids,
        }

        processor_args.update(args["dict"])

        processing_stream_info.update(
            processing_id=self.processing_id,
            processor_type=args["implement_type"],
            processor_logic=json.dumps(processor_args),
            updated_by=get_request_username(),
            description=args.get("processing_alias"),
        )

        meta_transaction_result = MetaApiHelper.meta_transaction(api_operate_list)
        logger.info("request meta transaction api, and result is %s" % str(meta_transaction_result))
        return {
            "result_table_ids": output_result_table_ids,
            "processing_id": self.processing_id,
        }


class FlinkCodeProcessing(Processing):
    def update_processing(self, args):
        return super(FlinkCodeProcessing, self).update_processing(args)


class SparkCodeProcessing(Processing):
    def update_processing(self, args):
        return super(SparkCodeProcessing, self).update_processing(args)
