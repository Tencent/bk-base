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

from dataflow.batch.periodic.processings_sql_v2_driver import ProcessingsSqlV2Driver
from dataflow.modeling.processing.tensorflow.tensorflow_batch_info_builder import TensorFlowBatchInfoBuilder
from dataflow.modeling.processing.tensorflow.tensorflow_code_processings_back_end import (
    TensorFlowCodeProcessingsBackend,
)
from dataflow.modeling.processing.tensorflow.tensorflow_processings_validator import TensorFlowProcessingsValidator


class TensorFlowProcessingDriver(ProcessingsSqlV2Driver):
    @staticmethod
    def create_processings(args):
        batch_info_params_builder = TensorFlowBatchInfoBuilder()
        batch_info_params_obj = batch_info_params_builder.build_sql_from_flow_api(args)
        processings_validator = TensorFlowProcessingsValidator()
        processings_validator.validate(batch_info_params_obj)
        processing_bachend = TensorFlowCodeProcessingsBackend(batch_info_params_obj)
        return processing_bachend.create_processings()

    @staticmethod
    def update_processings(args):
        batch_info_params_builder = TensorFlowBatchInfoBuilder()
        batch_info_params_obj = batch_info_params_builder.build_sql_from_flow_api(args)
        processings_validator = TensorFlowProcessingsValidator()
        processings_validator.validate(batch_info_params_obj)
        processing_bachend = TensorFlowCodeProcessingsBackend(batch_info_params_obj)
        return processing_bachend.update_processings()

    @staticmethod
    def delete_processings(processing_id, args):
        TensorFlowCodeProcessingsBackend.delete_models(processing_id)
        # 然后清理其它内容
        ProcessingsSqlV2Driver.delete_processings(processing_id, args)
