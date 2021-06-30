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
from dataflow.batch.periodic.param_info.builder.periodic_batch_job_builder import PeriodicBatchJobBuilder
from dataflow.batch.utils.time_util import BatchTimeTuple


class ProcessingsValidator(object):
    def validate(self, periodic_batch_info_params_obj):
        """
        :param periodic_batch_info_params_obj:
        :type periodic_batch_info_params_obj:
        dataflow.batch.periodic.param_info.periodic_batch_info_params.PeriodicBatchInfoParams
        """
        self.validate_input(periodic_batch_info_params_obj)
        self.validate_output_data_offset(periodic_batch_info_params_obj)

    def validate_input(self, periodic_batch_info_params_obj):
        """
        :param periodic_batch_info_params_obj:
        :type periodic_batch_info_params_obj:
        dataflow.batch.periodic.param_info.periodic_batch_info_params.PeriodicBatchInfoParams
        """
        for input_table in periodic_batch_info_params_obj.input_result_tables:
            if (
                input_table.window_type.lower() == "scroll"
                or input_table.window_type.lower() == "slide"
                or input_table.window_type.lower() == "accumulate"
            ):
                self.__check_greater_than_value(input_table.window_offset, "window_offset", "0H")

            if input_table.window_type.lower() == "slide" or input_table.window_type.lower() == "accumulate":
                self.__check_greater_than_value(input_table.window_size, "window_size", "0H")

            if input_table.window_type.lower() == "accumulate":
                self.__check_greater_than_value(input_table.window_start_offset, "window_start_offset", "0H")
                self.__check_greater_than_value(input_table.window_end_offset, "window_end_offset", "0H")
                self.__check_less_than_value(
                    input_table.window_start_offset,
                    "window_start_offset",
                    input_table.window_size,
                )
                self.__check_less_than_value(
                    input_table.window_end_offset,
                    "window_end_offset",
                    input_table.window_size,
                )
                self.__check_if_null(input_table.accumulate_start_time, "accumulate_start_time")

    def __check_greater_than_value(self, check_value, check_name, limit_value):
        self.__check_if_null(check_value, check_name)
        limit_time_tuple = BatchTimeTuple()
        limit_time_tuple.from_jobnavi_format(limit_value)
        check_value_tuple = BatchTimeTuple()
        check_value_tuple.from_jobnavi_format(check_value)
        if check_value_tuple < limit_time_tuple:
            raise BatchUnsupportedOperationError(_("{}数值必须大于{}".format(check_name, limit_value)))

    def __check_less_than_value(self, check_value, check_name, limit_value):
        self.__check_if_null(check_value, check_name)
        limit_time_tuple = BatchTimeTuple()
        limit_time_tuple.from_jobnavi_format(limit_value)
        check_value_tuple = BatchTimeTuple()
        check_value_tuple.from_jobnavi_format(check_value)
        if check_value_tuple > limit_time_tuple:
            raise BatchUnsupportedOperationError(_("{}数值必须小于{}".format(check_name, limit_value)))

    def __check_if_null(self, check_value, check_name):
        if check_value is None:
            raise BatchUnsupportedOperationError(_("{}数值不能是null".format(check_name)))

    def validate_output_data_offset(self, periodic_batch_info_params_obj):
        """
        :param periodic_batch_info_params_obj:
        :type periodic_batch_info_params_obj:
        dataflow.batch.periodic.param_info.periodic_batch_info_params.PeriodicBatchInfoParams
        """
        try:
            PeriodicBatchJobBuilder.calculate_output_offset(
                periodic_batch_info_params_obj.input_result_tables,
                periodic_batch_info_params_obj.output_result_tables[0],
                periodic_batch_info_params_obj.count_freq,
                periodic_batch_info_params_obj.schedule_period,
            )
        except BatchTimeCompareError:
            raise BatchUnsupportedOperationError(_("当前配置无法算出默认存储分区，请激活自定义出库配置"))
