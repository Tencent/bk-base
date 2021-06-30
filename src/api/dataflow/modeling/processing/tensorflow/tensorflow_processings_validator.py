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

from django.utils.translation import ugettext_lazy as _

from dataflow.batch.exceptions.comp_execptions import BatchTimeCompareError, BatchUnsupportedOperationError
from dataflow.batch.periodic.backend.validator.processings_validator import ProcessingsValidator
from dataflow.modeling.job.tensorflow.tensorflow_batch_job_builder import TensorFlowBatchJobBuilder


class TensorFlowProcessingsValidator(ProcessingsValidator):
    def __init__(self):
        super(TensorFlowProcessingsValidator, self).__init__()

    def validate_output_data_offset(self, periodic_batch_info_params_obj):
        """
        :param periodic_batch_info_params_obj:
        :type periodic_batch_info_params_obj: PeriodicBatchInfoParams
        """
        if len(periodic_batch_info_params_obj.output_result_tables) == 0:
            # 没有输出表，不用校验
            return
        try:
            TensorFlowBatchJobBuilder.calculate_output_offset(
                periodic_batch_info_params_obj.input_result_tables,
                periodic_batch_info_params_obj.output_result_tables[0],
                periodic_batch_info_params_obj.count_freq,
                periodic_batch_info_params_obj.schedule_period,
            )
        except BatchTimeCompareError:
            raise BatchUnsupportedOperationError(_("当前配置无法算出默认存储分区，请激活自定义出库配置"))
