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

from dataflow.component.utils.time_util import get_datetime
from dataflow.shared.log import stream_logger as logger
from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import ProcessingExistsError, ResultTableExistsError
from dataflow.stream.handlers import processing_stream_info
from dataflow.stream.processing.processing_utils import ProcessingUtils
from dataflow.stream.settings import FLINK_CODE_VERSION, STREAM_TYPE, USER_GENERATE_TYPE


class ProcessingBuilder(object):
    def create_processing(self, args):
        component_type = args["component_type"]
        logger.info("to create " + component_type + " code processing " + args["processing_id"])
        # 检测data processing是否存在
        if MetaApiHelper.is_data_processing_exist(args["processing_id"]):
            raise ProcessingExistsError(_("数据处理%s已存在，不允许重复创建" % args["processing_id"]))

        # 组装输出result table id 并检测result table是否存在
        output_result_table_ids = []
        for output in args["outputs"]:
            check_result_table_id = "{}_{}".format(
                output["bk_biz_id"],
                output["result_table_name"],
            )
            if (
                MetaApiHelper.is_result_table_exist(check_result_table_id)
                or check_result_table_id in output_result_table_ids
            ):
                raise ResultTableExistsError(_("结果表%s已存在，不允许重复创建" % check_result_table_id))
            output_result_table_ids.append(check_result_table_id)

        # flink类型任务额外记录支持多版本，并执行code代码检查
        programming_language = args["dict"]["programming_language"]
        code = args["dict"]["code"]
        if component_type == "flink":
            component_type = "{}-{}".format(component_type, FLINK_CODE_VERSION)
            code_check_group = "stream-code-%s" % component_type
        else:
            code_check_group = "spark-structured-streaming-code"
        # 代码检查 todo code test
        ProcessingUtils.code_check(programming_language, code, code_check_group, code_check_group)

        # 拼接输出result table信息
        result_tables = []
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
            result_tables.append(table_params)

        # 创建data processing
        processing_params = {
            "project_id": args["project_id"],
            "processing_id": args["processing_id"],
            "processing_alias": args["processing_alias"],
            "processing_type": STREAM_TYPE,
            "generate_type": USER_GENERATE_TYPE,
            "bk_username": get_request_username(),
            "description": args["processing_alias"],
            "result_tables": result_tables,
            "inputs": ProcessingUtils.generate_processing_inputs(args["input_result_tables"]),
            "outputs": ProcessingUtils.generate_processing_outputs(output_result_table_ids),
            "tags": args["tags"],
        }

        # 信息保存到processing_stream_info
        processor_args = {
            "static_data": args["static_data"],
            "input_result_tables": args["input_result_tables"],
            "output_result_tables": output_result_table_ids,
        }

        processor_args.update(args["dict"])

        processing_stream_info.save(
            processing_id=args["processing_id"],
            processor_type=args["implement_type"],
            processor_logic=json.dumps(processor_args),
            component_type=component_type,
            created_at=get_datetime(),
            created_by=get_request_username(),
            description=args.get("processing_alias"),
        )
        MetaApiHelper.set_data_processing(processing_params)

        return {
            "result_table_ids": output_result_table_ids,
            "processing_id": args["processing_id"],
        }


class FlinkCodeProcessingBuilder(ProcessingBuilder):
    def create_processing(self, args):
        return super(FlinkCodeProcessingBuilder, self).create_processing(args)


class SparkCodeProcessingBuilder(ProcessingBuilder):
    def create_processing(self, args):
        return super(SparkCodeProcessingBuilder, self).create_processing(args)
