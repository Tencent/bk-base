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
import random
import time
from datetime import datetime, timedelta

import dataflow.pizza_settings as pizza_settings
from dataflow.batch import settings
from dataflow.batch.api.api_helper import DatabusHelper
from dataflow.batch.exceptions.comp_execptions import BatchNotImplementError
from dataflow.batch.utils import resource_util
from dataflow.batch.utils.time_util import BatchTimeTuple
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class PeriodicSchedulerRegister(object):
    def __init__(self, periodic_batch_job_params_obj):
        """
        :param periodic_batch_job_params_obj:
        :type periodic_batch_job_params_obj:
        dataflow.batch.periodic.param_info.periodic_batch_job_params.PeriodicBatchJobParams
        """
        self.periodic_batch_job_params_obj = periodic_batch_job_params_obj
        self.processing_id = self.periodic_batch_job_params_obj.job_id
        self.bk_username = self.periodic_batch_job_params_obj.bk_username
        self.is_restart = self.periodic_batch_job_params_obj.schedule_info.is_restart

        self.jobnavi = JobNaviHelper(
            self.periodic_batch_job_params_obj.schedule_info.geog_area_code,
            self.periodic_batch_job_params_obj.schedule_info.cluster_id,
        )
        self.existed_schedule_info = self.jobnavi.get_schedule_info(self.processing_id)

        self.type_id = self.periodic_batch_job_params_obj.schedule_info.jobnavi_task_type

    def register_jobnavi(self):
        if self.periodic_batch_job_params_obj.schedule_info.schedule_period == "hour":
            period_unit = "H"
        elif self.periodic_batch_job_params_obj.schedule_info.schedule_period == "day":
            period_unit = "d"
        elif self.periodic_batch_job_params_obj.schedule_info.schedule_period == "week":
            period_unit = "W"
        elif self.periodic_batch_job_params_obj.schedule_info.schedule_period == "month":
            period_unit = "M"
        else:
            period_unit = ""

        period = {
            "timezone": pizza_settings.TIME_ZONE,
            "cron_expression": "",
            "frequency": self.periodic_batch_job_params_obj.schedule_info.count_freq,
            "period_unit": period_unit,
            "first_schedule_time": self.get_start_time(),
            "delay": "0H",
        }

        extra_info = {
            "run_mode": pizza_settings.RUN_MODE,
            "type": self.periodic_batch_job_params_obj.batch_type,
            "queue": self.periodic_batch_job_params_obj.resource.queue_name,
            "cluster_group_id": self.periodic_batch_job_params_obj.resource.cluster_group,
            "service_type": "batch",
            "component_type": "spark",
            "engine_conf_path": "/dataflow/batch/jobs/{job_id}/get_engine_conf/".format(job_id=self.processing_id),
        }

        if self.periodic_batch_job_params_obj.resource.resource_group_id is not None:
            extra_info["resource_group_id"] = self.periodic_batch_job_params_obj.resource.resource_group_id

        if pizza_settings.RUN_VERSION == pizza_settings.RELEASE_ENV:
            extra_info["license"] = {
                "license_server_url": pizza_settings.LICENSE_SERVER_URL,
                "license_server_file_path": pizza_settings.LICENSE_SERVER_FILE_PATH,
            }

        jobnavi_args = {
            "schedule_id": self.processing_id,
            "description": " Project {} submit by {}".format(self.processing_id, self.bk_username),
            "type_id": self.type_id,
            "period": period,
            "parents": self.get_parents(),
            "execute_before_now": self.is_restart,
            "active": False,
            "node_label": self.get_jobnavi_node_label(),
            "data_time_offset": self.get_data_time_offset(),
            "recovery": {
                "enable": self.periodic_batch_job_params_obj.recovery_info.recovery_enable,
                "interval_time": self.periodic_batch_job_params_obj.recovery_info.recovery_interval,
                "retry_times": self.periodic_batch_job_params_obj.recovery_info.retry_times,
            },
            "extra_info": str(json.dumps(extra_info)),
        }

        if self.existed_schedule_info:
            batch_logger.info(jobnavi_args)
            self.jobnavi.update_schedule_info(jobnavi_args)
        else:
            batch_logger.info(jobnavi_args)
            jobnavi_args["execute_before_now"] = False
            self.jobnavi.create_schedule_info(jobnavi_args)

        self.jobnavi.start_schedule(self.processing_id)

    def get_data_time_offset(self):
        return self.periodic_batch_job_params_obj.sink_nodes[self.processing_id].data_time_offset

    def get_parents(self):
        parents_args = []
        for source_node_id in self.periodic_batch_job_params_obj.source_nodes:
            source_node = self.periodic_batch_job_params_obj.source_nodes[source_node_id]
            result_table_meta = ResultTableHelper.get_result_table(source_node.id, related="data_processing")
            source_type = result_table_meta["processing_type"]
            if (
                source_type == "batch"
                and not self.is_tdw_data_source(self.periodic_batch_job_params_obj.batch_type, result_table_meta)
            ) or self.is_model_serve_mode_offline(source_node.id):
                parents_args.append(self.handle_batch_upstream(source_node))
            elif DatabusHelper.is_rt_batch_import(source_node.id):
                self.handle_import_tdw_to_hdfs_upstream(source_node)
                parents_args.append(self.handle_batch_upstream(source_node))
        return parents_args

    def is_tdw_data_source(self, batch_type, result_table_meta):
        if batch_type in ["tdw", "tdw_jar"]:
            return result_table_meta["is_managed"] == 0 or (
                result_table_meta["processing_type"] == "storage" and result_table_meta["platform"] == "tdw"
            )
        return False

    def is_model_serve_mode_offline(self, result_table_id):
        return DataProcessingHelper.is_model_serve_mode_offline(result_table_id)

    def get_start_time(self):

        if self.existed_schedule_info:
            is_rt_schedule_equal_to_existed = False
            is_rt_start_time_in_same_hour = False
            if (
                self.existed_schedule_info["period"]["frequency"]
                and self.existed_schedule_info["period"]["frequency"]
                == self.periodic_batch_job_params_obj.schedule_info.count_freq
                and self.existed_schedule_info["period"]["period_unit"]
                and self.existed_schedule_info["period"]["period_unit"]
                == self.periodic_batch_job_params_obj.schedule_info.schedule_period
            ):
                is_rt_schedule_equal_to_existed = True

            existed_start_time_dt_obj = datetime.fromtimestamp(
                self.existed_schedule_info["period"]["first_schedule_time"] / 1000.0
            )
            existed_start_time_dt_obj_in_hour = existed_start_time_dt_obj.replace(minute=0, second=0, microsecond=0)

            new_start_time_dt_obj = datetime.fromtimestamp(
                self.periodic_batch_job_params_obj.schedule_info.start_time / 1000.0
            )
            new_start_time_dt_obj_in_hour = new_start_time_dt_obj.replace(minute=0, second=0, microsecond=0)

            is_rt_start_time_in_same_hour = int(time.mktime(new_start_time_dt_obj_in_hour.timetuple())) == int(
                time.mktime(existed_start_time_dt_obj_in_hour.timetuple())
            )

            if is_rt_schedule_equal_to_existed and is_rt_start_time_in_same_hour:
                return self.existed_schedule_info["period"]["first_schedule_time"]
            else:
                new_time_tuple = new_start_time_dt_obj_in_hour.replace(
                    minute=existed_start_time_dt_obj.minute
                ).timetuple()
                return int(time.mktime(new_time_tuple) * 1000)

        else:
            start_time_dt_obj = datetime.fromtimestamp(
                self.periodic_batch_job_params_obj.schedule_info.start_time / 1000.0
            )
            delay_minute = random.randint(10, 35)
            dt_obj = start_time_dt_obj.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=delay_minute)
            return int(time.mktime(dt_obj.timetuple()) * 1000)

    def handle_batch_upstream(self, source_node):
        """
        :param source_node:
        :type source_node: dataflow.batch.periodic.param_info.periodic_batch_job_params.SourceNode
        """
        if source_node.window_type.lower() == "accumulate":
            return self.generate_accumulate_dependency(source_node)

        elif source_node.window_type.lower() == "slide" or source_node.window_type.lower() == "scroll":
            return self.generate_fixed_dependency(source_node)

    def generate_accumulate_dependency(self, source_node):
        """
        :param source_node:
        :type source_node: dataflow.batch.periodic.param_info.periodic_batch_job_params.SourceNode
        """
        accumulate_start_time_dt = datetime.fromtimestamp(int(source_node.accumulate_start_time / 1000))
        accumulate_start_time = str(
            int(time.mktime(accumulate_start_time_dt.replace(minute=0, second=0, microsecond=0).timetuple())) * 1000
        )
        accumulate_window_size = source_node.window_size

        window_size_time_tuple = BatchTimeTuple()
        window_size_time_tuple.from_jobnavi_format(source_node.window_size)
        start_time_offset_time_tuple = BatchTimeTuple()
        start_time_offset_time_tuple.from_jobnavi_format(source_node.window_start_offset)
        end_time_offset_time_tuple = BatchTimeTuple()
        end_time_offset_time_tuple.from_jobnavi_format(source_node.window_end_offset)

        accumulate_start_offset = self.calculate_accumulate_jobnavi_offset(
            start_time_offset_time_tuple, window_size_time_tuple
        )
        accumulate_end_offset = self.calculate_accumulate_jobnavi_offset(
            end_time_offset_time_tuple, window_size_time_tuple
        )

        parent_args = {
            "parent_id": source_node.id,
            "window_offset": source_node.window_offset,
            "dependency_rule": source_node.dependency_rule,
            "param_type": "accumulate",
            "param_value": "{}:{}:{}~{}".format(
                accumulate_start_time,
                accumulate_window_size,
                accumulate_start_offset,
                accumulate_end_offset,
            ),
        }

        return parent_args

    def calculate_accumulate_jobnavi_offset(self, offset_tuple, full_window_tuple):
        """
        :param offset_tuple:
        :type offset_tuple: BatchTimeTuple
        :param full_window_tuple:
        :type full_window_tuple: BatchTimeTuple
        :return:
        """
        # jobnavi accumulate time offset start from 1, from ui we assume 0 is the start time
        if offset_tuple == full_window_tuple:
            return "-1H"
        elif offset_tuple.month >= 0 and offset_tuple.get_hour_except_month() >= 0:
            result_tuple = offset_tuple + BatchTimeTuple(hour=1)
            return result_tuple.to_jobnavi_string()
        else:
            result_tuple = offset_tuple - BatchTimeTuple(hour=1)
            return result_tuple.to_jobnavi_string()

    def generate_fixed_dependency(self, source_node):
        """
        :param source_node:
        :type source_node: dataflow.batch.periodic.param_info.periodic_batch_job_params.SourceNode
        """
        parent_args = {
            "parent_id": source_node.id,
            "dependency_rule": source_node.dependency_rule,
            "param_type": "fixed",
            "param_value": source_node.window_size,
            "window_offset": source_node.window_offset,
        }
        return parent_args

    def handle_import_tdw_to_hdfs_upstream(self, source_node):
        parent_args = self.handle_batch_upstream(source_node)

        default_args = {
            "schedule_id": source_node.id,
            "description": "tdw import project {} created by {}".format(source_node.id, self.bk_username),
            "type_id": "default",
        }

        batch_logger.info(default_args)

        parent_rtn = self.jobnavi.get_schedule_info(source_node.id)
        if parent_rtn:
            self.jobnavi.update_schedule_info(default_args)
        else:
            self.jobnavi.create_schedule_info(default_args)
        return parent_args

    def __handle_tdw_data_source(self):
        raise BatchNotImplementError("Not support tdw schedule for this version")  # Not support in this version

    def get_jobnavi_node_label(self):
        deploy_config = self.periodic_batch_job_params_obj.deploy_config
        resource = self.periodic_batch_job_params_obj.resource
        try:
            # 离线计算配置库中任务级别标签最高
            if deploy_config.node_label is not None:
                node_label = deploy_config.node_label
                return node_label
        except Exception as e:
            batch_logger.exception(e)

        geog_area_code = self.periodic_batch_job_params_obj.schedule_info.geog_area_code
        default_node_label, _ = resource_util.get_default_jobnavi_label(geog_area_code, "batch", component_type="spark")
        node_label, _ = resource_util.get_jobnavi_label(geog_area_code, resource.cluster_group, "batch")
        batch_logger.info(
            "Node label from resource center, default node label: {}, node_label {}".format(
                default_node_label, node_label
            )
        )
        # 为了兼容jobnavi已有标签的任务,如果资源系统返回标签非默认标签使用资源系统返回标签，
        # 如果返回标签为默认标签则判断jobnavi任务是否已存在标签，如果已存在标签则使用已存在标签
        if (
            node_label == settings.SPARK_SQL_NODE_LABEL
            and self.existed_schedule_info is not None
            and self.existed_schedule_info["node_label"] in settings.SPARK_SQL_NODE_LABEL_DEFAULT_LIST
        ):
            node_label = self.existed_schedule_info["node_label"]
        return node_label
