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
from copy import deepcopy

from django.utils.translation import ugettext_lazy as _

from dataflow.batch.exceptions.comp_execptions import BatchIllegalArgumentError, BatchUnsupportedOperationError
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.periodic.param_info.periodic_batch_info_params import PeriodicBatchInfoParams
from dataflow.batch.periodic.param_info.periodic_batch_job_params import (
    PeriodicBatchJobParams,
    SinkNode,
    SourceNode,
    TransformNode,
)
from dataflow.batch.periodic.param_info.periodic_job_info_params import PeriodicJobInfoParams
from dataflow.batch.utils import result_table_util
from dataflow.batch.utils.time_util import BatchTimeTuple
from dataflow.shared.handlers import processing_udf_info
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class PeriodicBatchJobBuilder(object):
    def __init__(self):
        self.job_info_obj = PeriodicJobInfoParams()  # type: PeriodicJobInfoParams
        self.batch_info_obj = PeriodicBatchInfoParams()  # type: PeriodicBatchInfoParams

        self.api_params = None  # type: dict
        self.job_id = None  # type: str

        self.periodic_batch_job_obj = None  # type: PeriodicBatchJobParams

    def build_from_flow_start_api(self, job_id, args):
        """
        This is used to build runtime params
        :param params:
        :return:
        """
        self.api_params = args
        self.job_id = job_id
        self.job_info_obj.from_processing_job_info_db(job_id)
        self.batch_info_obj.from_processing_batch_info_db(self.job_info_obj.job_config.processings[0])

        batch_type = self.batch_info_obj.batch_type
        if batch_type == "batch_sql_v2":
            self.periodic_batch_job_obj = PeriodicBatchJobParams()
        else:
            raise BatchIllegalArgumentError("Found unsupported processor type ({})".format(batch_type))

        self.build_batch_job()
        return self.periodic_batch_job_obj

    def copy_and_refresh_batch_job_params(self, periodic_batch_job_obj):
        """
        This is used to refresh runtime params by calling meta api and storekit api
        :param periodic_batch_job_obj:
        :return:
        """
        self.periodic_batch_job_obj = deepcopy(periodic_batch_job_obj)
        for source_node_id in self.periodic_batch_job_obj.source_nodes:
            self.periodic_batch_job_obj.source_nodes[source_node_id] = self.refresh_source_node_with_extra_info(
                self.periodic_batch_job_obj.source_nodes[source_node_id]
            )

        for sink_node_id in self.periodic_batch_job_obj.sink_nodes:
            self.periodic_batch_job_obj.sink_nodes[sink_node_id] = self.refresh_sink_node_with_extra_info(
                self.periodic_batch_job_obj.sink_nodes[sink_node_id]
            )
        return self.periodic_batch_job_obj

    def build_batch_job(self):
        self.periodic_batch_job_obj.job_id = self.job_id
        self.periodic_batch_job_obj.bk_username = self.api_params["bk_username"]

        self.periodic_batch_job_obj.code_version = self.job_info_obj.code_version
        self.periodic_batch_job_obj.batch_type = self.batch_info_obj.batch_type
        self.periodic_batch_job_obj.programming_language = self.job_info_obj.programming_language
        self.periodic_batch_job_obj.implement_type = self.job_info_obj.implement_type

        self.build_schedule_info()
        self.build_resource_and_deploy_config()
        self.build_recovery_info()
        self.build_source_nodes()
        self.build_transform_nodes()
        self.build_sink_nodes()
        self.build_self_dependency_source_node()  # put this here because it requires sink data offset
        self.build_udf_from_udf_info()

    def build_schedule_info(self):
        self.periodic_batch_job_obj.schedule_info.is_restart = self.api_params["is_restart"]

        self.periodic_batch_job_obj.schedule_info.geog_area_code = self.job_info_obj.jobserver_config.geog_area_code
        self.periodic_batch_job_obj.schedule_info.cluster_id = self.job_info_obj.jobserver_config.cluster_id

        self.periodic_batch_job_obj.schedule_info.count_freq = self.batch_info_obj.count_freq
        self.periodic_batch_job_obj.schedule_info.schedule_period = self.batch_info_obj.schedule_period
        self.periodic_batch_job_obj.schedule_info.start_time = self.batch_info_obj.start_time

        batch_type = self.batch_info_obj.batch_type
        if batch_type.lower() == "batch_sql_v2":
            self.periodic_batch_job_obj.schedule_info.jobnavi_task_type = "spark_sql"
        else:
            raise BatchUnsupportedOperationError("Find unsupport batch type ({})".format(batch_type))

    def build_resource_and_deploy_config(self):
        self.periodic_batch_job_obj.resource.cluster_group = self.job_info_obj.cluster_group
        self.periodic_batch_job_obj.resource.queue_name = self.job_info_obj.cluster_name
        self.periodic_batch_job_obj.resource.resource_group_id = self.job_info_obj.job_config.resource_group_id
        self.periodic_batch_job_obj.deploy_config.node_label = self.job_info_obj.deploy_config.node_label
        self.periodic_batch_job_obj.deploy_config.engine_conf = self.job_info_obj.deploy_config.engine_conf
        self.periodic_batch_job_obj.deploy_config.user_engine_conf = self.job_info_obj.deploy_config.user_engine_conf

    def build_recovery_info(self):
        self.periodic_batch_job_obj.recovery_info.recovery_enable = self.batch_info_obj.recovery_enable
        self.periodic_batch_job_obj.recovery_info.recovery_interval = self.batch_info_obj.recovery_interval
        self.periodic_batch_job_obj.recovery_info.retry_times = self.batch_info_obj.recovery_times

    def build_source_nodes(self):
        for input_table in self.batch_info_obj.input_result_tables:
            self.periodic_batch_job_obj.source_nodes[
                input_table.result_table_id
            ] = self.build_source_node_from_input_table(input_table)

    def build_transform_nodes(self):
        tmp_transform_node = TransformNode()
        tmp_transform_node.id = self.batch_info_obj.processing_id
        tmp_transform_node.name = self.batch_info_obj.processing_id
        tmp_transform_node.processor_logic = self.batch_info_obj.processor_logic
        tmp_transform_node.processor_type = self.batch_info_obj.processor_type
        self.periodic_batch_job_obj.transform_nodes[tmp_transform_node.id] = tmp_transform_node

    def build_sink_nodes(self):
        for output_table in self.batch_info_obj.output_result_tables:
            self.periodic_batch_job_obj.sink_nodes[
                output_table.result_table_id
            ] = self.build_sink_node_from_output_table(output_table)

    def build_sink_node_from_output_table(self, output_table_config_param_obj):
        """
        :param output_table_config_param_obj:
        :type output_table_config_param_obj: OutputTableConfigParam
        """
        tmp_sink_node = SinkNode()
        tmp_sink_node.id = output_table_config_param_obj.result_table_id
        tmp_sink_node.name = output_table_config_param_obj.result_table_id
        tmp_sink_node.storage_type = "hdfs"
        tmp_sink_node.data_time_offset = PeriodicBatchJobBuilder.calculate_output_offset(
            self.batch_info_obj.input_result_tables,
            output_table_config_param_obj,
            self.batch_info_obj.count_freq,
            self.batch_info_obj.schedule_period,
        )
        tmp_sink_node.only_from_whole_data = output_table_config_param_obj.only_from_whole_data
        return self.refresh_sink_node_with_extra_info(tmp_sink_node)

    def refresh_sink_node_with_extra_info(self, sink_node):
        sink_node.fields = result_table_util.parse_source_fields(sink_node.id)
        storage_response = ResultTableHelper.get_result_table_storage(sink_node.id)
        sink_node.storage_conf = result_table_util.build_hdfs_params(sink_node.id, storage_response)
        for storage_name in storage_response:
            if storage_name.lower() != "hdfs":
                sink_node.call_databus_shipper = True
        return sink_node

    @staticmethod
    def calculate_output_offset(
        input_table_config_param_list,
        output_table_config_param_obj,
        count_freq,
        schedule_period,
    ):
        """
        This calculate sink data offset, will require that all source nodes have been built.
        :param input_table_config_param_list:
        :type input_table_config_param_list: List[InputTableConfigParam]
        :param output_table_config_param_obj:
        :type output_table_config_param_obj: OutputTableConfigParam
        :param count_freq:
        :type count_freq: int
        :param schedule_period:
        :type schedule_period: str
        """
        if output_table_config_param_obj.only_from_whole_data:
            data_time_tuple = BatchTimeTuple()
            data_time_tuple.set_time_with_unit(count_freq, schedule_period)
            return data_time_tuple.to_jobnavi_string()

        if output_table_config_param_obj.enable_customize_output:
            if output_table_config_param_obj.output_baseline.lower() == "schedule_time":
                output_offset_tuple = BatchTimeTuple()
                output_offset_tuple.from_jobnavi_format(output_table_config_param_obj.output_offset)
                one_hour_tuple = BatchTimeTuple(hour=1)
                if output_offset_tuple < one_hour_tuple:
                    raise BatchUnsupportedOperationError(_("使用调度时间作为输出基准，偏移值必须大于1"))

                max_window_length = BatchTimeTuple()
                # check if offset exceeded max window length
                for input_table in input_table_config_param_list:
                    if input_table.window_type.lower() == "scroll" or input_table.window_type.lower() == "slide":
                        window_size_time_tuple = BatchTimeTuple()
                        window_size_time_tuple.from_jobnavi_format(input_table.window_size)
                        window_offset_tuple = BatchTimeTuple()
                        window_offset_tuple.from_jobnavi_format(input_table.window_offset)
                        window_range_tuple = window_size_time_tuple + window_offset_tuple
                        if max_window_length < window_range_tuple:
                            max_window_length = window_range_tuple
                    elif input_table.window_type.lower() == "accumulate":
                        window_offset_tuple = BatchTimeTuple()
                        window_offset_tuple.from_jobnavi_format(input_table.window_offset)

                        window_range_tuple = window_offset_tuple + one_hour_tuple
                        if max_window_length < window_range_tuple:
                            max_window_length = window_range_tuple

                if output_offset_tuple > max_window_length:
                    raise BatchUnsupportedOperationError(
                        _("输出偏移值超过了最大范围({})".format(max_window_length.to_jobnavi_string()))
                    )

                return output_offset_tuple.to_jobnavi_string()

            for input_table in input_table_config_param_list:
                if input_table.result_table_id == output_table_config_param_obj.output_baseline.lower():
                    if input_table.window_type.lower() == "whole":
                        raise BatchUnsupportedOperationError(_("{}不能作为结果数据基准时间".format(input_table.result_table_id)))
                    output_offset_tuple = BatchTimeTuple()
                    output_offset_tuple.from_jobnavi_format(output_table_config_param_obj.output_offset)
                    window_offset_time_tuple = BatchTimeTuple()
                    window_offset_time_tuple.from_jobnavi_format(input_table.window_offset)
                    window_size_time_tuple = BatchTimeTuple()
                    window_size_time_tuple.from_jobnavi_format(input_table.window_size)

                    if output_table_config_param_obj.output_baseline_location.lower() == "start" and (
                        input_table.window_type.lower() == "scroll" or input_table.window_type.lower() == "slide"
                    ):
                        total_time_tuple = window_size_time_tuple + window_offset_time_tuple - output_offset_tuple
                    elif output_table_config_param_obj.output_baseline_location.lower() == "end":
                        total_time_tuple = window_offset_time_tuple + BatchTimeTuple(hour=1) + output_offset_tuple
                    else:
                        raise BatchUnsupportedOperationError(_("不支持选择累加窗口{}起始时间".format(input_table.result_table_id)))

                    if (
                        total_time_tuple > (window_size_time_tuple + window_offset_time_tuple)
                        or total_time_tuple <= window_offset_time_tuple
                    ):
                        raise BatchUnsupportedOperationError(
                            _("结果数据时间偏移值超出了作为基准时间的父表{}的窗口范围".format(input_table.result_table_id))
                        )
                    return total_time_tuple.to_jobnavi_string()
            raise BatchUnsupportedOperationError(
                _("结果数据基准时间{}无效".format(output_table_config_param_obj.output_baseline))
            )
        else:
            data_time_tuple = BatchTimeTuple()
            zero_time_tuple = BatchTimeTuple()
            for input_table in input_table_config_param_list:
                if input_table.window_type.lower() == "whole":
                    # if there is only whole window type the output data offset will be zero
                    continue
                window_size_time_tuple = BatchTimeTuple()
                window_size_time_tuple.from_jobnavi_format(input_table.window_size)
                offset_time_tuple = BatchTimeTuple()
                offset_time_tuple.from_jobnavi_format(input_table.window_offset)

                if input_table.window_type.lower() == "accumulate":
                    total_time_tuple = offset_time_tuple + BatchTimeTuple(hour=1)
                else:
                    total_time_tuple = window_size_time_tuple + offset_time_tuple
                if total_time_tuple < data_time_tuple or data_time_tuple == zero_time_tuple:
                    data_time_tuple = total_time_tuple

            if data_time_tuple == zero_time_tuple:
                raise BatchUnsupportedOperationError(_("当前参数无法计算出有效的结果数据时间"))

            return data_time_tuple.to_jobnavi_string()

    def build_source_node_from_input_table(self, input_table_config_param_obj):
        """
        :param input_table_config_param_obj:
        :type input_table_config_param_obj: InputTableConfigParam
        """
        tmp_source_node = SourceNode()
        tmp_source_node.id = input_table_config_param_obj.result_table_id
        tmp_source_node.name = input_table_config_param_obj.result_table_id
        tmp_source_node.window_type = input_table_config_param_obj.window_type
        tmp_source_node.dependency_rule = input_table_config_param_obj.dependency_rule

        tmp_source_node.window_offset = input_table_config_param_obj.window_offset
        tmp_source_node.window_size = input_table_config_param_obj.window_size
        tmp_source_node.window_start_offset = input_table_config_param_obj.window_start_offset
        tmp_source_node.window_end_offset = input_table_config_param_obj.window_end_offset

        tmp_source_node.accumulate_start_time = input_table_config_param_obj.accumulate_start_time
        tmp_source_node.storage_type = "ignite" if input_table_config_param_obj.is_static else "hdfs"
        self.refresh_source_node_with_extra_info(tmp_source_node)

        if tmp_source_node.role == "batch" and self.check_if_upstream_self_dependent(tmp_source_node.id):
            tmp_source_node.self_dependency_mode = "upstream"
        return tmp_source_node

    def build_self_dependency_source_node(self):
        if self.batch_info_obj.is_self_dependency:
            job_id = self.periodic_batch_job_obj.job_id

            self_dependency_source_node = SourceNode()
            self_dependency_source_node.id = job_id
            self_dependency_source_node.name = job_id
            self_dependency_source_node.window_type = "slide"
            self_dependency_source_node.window_size = "1H"

            data_time_tuple = BatchTimeTuple()
            data_time_tuple.from_jobnavi_format(self.periodic_batch_job_obj.sink_nodes[job_id].data_time_offset)

            one_hour_offset_tuple = BatchTimeTuple(hour=1)
            frequency_tuple = BatchTimeTuple()
            frequency_tuple.set_time_with_unit(
                self.periodic_batch_job_obj.schedule_info.count_freq,
                self.periodic_batch_job_obj.schedule_info.schedule_period,
            )
            window_offset_tuple = frequency_tuple + data_time_tuple - one_hour_offset_tuple

            self_dependency_source_node.window_offset = window_offset_tuple.to_jobnavi_string()
            self_dependency_source_node.self_dependency_mode = "current"
            self_dependency_source_node.storage_type = "hdfs"
            self_dependency_source_node.dependency_rule = self.batch_info_obj.self_dependency_rule

            self.refresh_source_node_with_extra_info(self_dependency_source_node)
            self.periodic_batch_job_obj.source_nodes[self_dependency_source_node.id] = self_dependency_source_node

    def check_if_upstream_self_dependent(self, node_id):
        batch_info_db_obj = ProcessingBatchInfoHandler.get_proc_batch_info_by_batch_id(node_id)
        if batch_info_db_obj.processor_logic == "batch_sql_v2":
            batch_info_obj = PeriodicBatchInfoParams()
            batch_info_obj.from_processing_batch_info_db_obj(batch_info_db_obj)
            return batch_info_obj.is_self_dependency
        else:
            is_self_dependency = False
            submit_args = json.loads(batch_info_db_obj.submit_args)
            if (
                "advanced" in submit_args
                and "self_dependency" in submit_args["advanced"]
                and submit_args["advanced"]["self_dependency"]
            ):
                is_self_dependency = True
        return is_self_dependency

    def refresh_source_node_with_extra_info(self, source_node):
        source_node.fields = result_table_util.parse_source_fields(source_node.id)
        if source_node.storage_type.lower() == "ignite":
            source_node.storage_conf = result_table_util.parse_ignite_params(source_node.id)
        else:
            source_node.storage_conf = result_table_util.parse_hdfs_params(source_node.id)

        self.set_params_from_meta(source_node)
        return source_node

    def set_params_from_meta(self, source_node):
        """
        :param source_node:
        :type source_node: SourceNode
        """
        result_table = ResultTableHelper.get_result_table(source_node.id)
        source_node.role = result_table["processing_type"]
        source_node.is_managed = result_table["is_managed"] if "is_managed" in result_table else 1

    def build_udf_from_udf_info(self):
        udfs = processing_udf_info.where(processing_id=self.batch_info_obj.processing_id)
        for udf in udfs:
            tmp_udf = {
                "udf_info": udf.udf_info,
                "job_id": self.job_info_obj.processing_id,
                "udf_name": udf.udf_name,
                "processing_type": "batch",
                "processing_id": self.job_info_obj.processing_id,
            }
            self.periodic_batch_job_obj.udf.append(tmp_udf)
