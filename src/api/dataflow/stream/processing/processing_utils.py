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

import base64

from dataflow.shared.codecheck.codecheck_helper import CodeCheckHelper
from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import SourceIllegal, StreamCodeCheckError


class ProcessingUtils(object):
    @classmethod
    def generate_result_table_input(cls, data_set_id, storage_type, channel_cluster_config_id, tags=[]):
        return {
            "data_set_type": "result_table",
            "data_set_id": data_set_id,
            "channel_cluster_config_id": channel_cluster_config_id,
            "storage_type": storage_type,
            "storage_cluster_config_id": None,
            "tags": tags,
        }

    @classmethod
    def generate_result_table_dummy_output(cls, result_table_id):
        return {
            "data_set_type": "result_table",
            "data_set_id": result_table_id,
            "storage_cluster_config_id": None,
            "channel_cluster_config_id": None,
            "storage_type": None,
        }

    @classmethod
    def generate_processing_inputs(cls, input_result_tables):
        inputs = []
        for input_result_table_id in input_result_tables:
            input_result_table_info = MetaApiHelper.get_result_table(input_result_table_id, related=["storages"])
            if "kafka" not in input_result_table_info["storages"]:
                raise SourceIllegal()
            channel_cluster_config_id = input_result_table_info["storages"]["kafka"]["storage_channel"][
                "channel_cluster_config_id"
            ]
            storage_type = "channel"
            one_input = cls.generate_result_table_input(input_result_table_id, storage_type, channel_cluster_config_id)
            inputs.append(one_input)
        return inputs

    @classmethod
    def generate_processing_outputs(cls, output_result_table_ids):
        outputs = []
        for output_result_table_id in output_result_table_ids:
            outputs.append(cls.generate_result_table_dummy_output(output_result_table_id))
        return outputs

    @classmethod
    def code_check(
        cls,
        code_language,
        check_content,
        blacklist_group_name,
        parser_group_name,
        check_flag=True,
    ):
        data = CodeCheckHelper.verify_code(
            code_language,
            base64.urlsafe_b64encode(bytes(check_content)),
            blacklist_group_name,
            check_flag,
            parser_group_name,
        )
        # 当 data 中 parse_status 为 false，说明语法有错误，提示出来
        if "fail" == data["parse_status"]:
            raise StreamCodeCheckError(data["parse_message"])
        # 当 data 中 parse_status 为 true，而且 parse_result 不为空，说明代码使用了限制方法，提示出来
        if "success" == data["parse_status"] and data["parse_result"]:
            message = []
            for info in data["parse_result"]:
                message.append(
                    "Line %s in the file %s, the method %s is illegal."
                    % (info["line_no"], info["file_name"], info["method_name"])
                )
            raise StreamCodeCheckError("; ".join(message))
