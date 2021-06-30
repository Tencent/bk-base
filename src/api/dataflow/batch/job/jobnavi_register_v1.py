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
from dataflow.batch.utils import resource_util, result_table_util, time_util
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class JobnaviRegisterV1(object):
    def __init__(self, job_id, job_info, created_by, is_restart=False):
        self.data_processing_id = job_id
        self.job_info = job_info
        self.job_config_submit_args = json.loads(json.loads(job_info.job_config)["submit_args"])
        self.advance = self.job_config_submit_args["advanced"] if "advanced" in self.job_config_submit_args else None
        self.created_by = created_by
        self.is_restart = is_restart

        self.is_self_dependency = False
        if self.advance and "self_dependency" in self.advance and self.advance["self_dependency"]:
            self.is_self_dependency = True

        jobserver_config = json.loads(self.job_info.jobserver_config)
        self.geog_area_code = jobserver_config["geog_area_code"]
        self.cluster_id = jobserver_config["cluster_id"]
        self.jobnavi = JobNaviHelper(self.geog_area_code, self.cluster_id)
        self.existed_jobnavi_job = self.jobnavi.get_schedule_info(self.data_processing_id)

        self.to_run_time = None
        self.is_user_custom_run_time = False

    def register_jobnavi(
        self,
        schedule_time,
    ):
        # 计算延迟分钟
        delay_minute = self.calculate_delay_min(schedule_time)
        # 根据传入schedule time和延迟分钟计算datetime
        dt_time = self.get_dt_by_schedule_time(schedule_time, delay_minute)
        # 计算启动时间,
        if (
            self.advance
            and "start_time" in self.advance
            and self.advance["start_time"]
            and (self.is_self_dependency or time.time() * 1000 < int(self.advance["start_time"]) + 60 * 60 * 1000)
        ):
            self.is_user_custom_run_time = True
            to_run_dt_time = datetime.fromtimestamp(int(self.advance["start_time"]) / 1000.0) + timedelta(
                minutes=delay_minute
            )
            self.to_run_time = int(time.mktime(to_run_dt_time.timetuple()) * 1000)
        else:
            self.to_run_time = int(time.mktime(dt_time.timetuple()) * 1000)

        # 计算父表依赖参数
        parents_args = self.generate_parent_params()
        if self.is_self_dependency:
            self_dependency_parent_args = self.gen_self_dependency_parent_args()
            parents_args.append(self_dependency_parent_args)

        # 对于累加窗口计算cron_expression
        cron_expression = self.gen_accumulate_cron_expression(self.existed_jobnavi_job, dt_time)

        # 创建jobnavi参数
        jobnavi_params = self.create_jobnavi_params(cron_expression, parents_args)
        jobnavi_resp = self.create_jobnavi_schedule_job(jobnavi_params, delay_minute)
        self.jobnavi.start_schedule(self.data_processing_id)
        return jobnavi_resp

    def create_jobnavi_schedule_job(self, jobnavi_args, delay_minute):
        if self.existed_jobnavi_job:
            if JobnaviRegisterV1.is_rt_period_equal(
                self.existed_jobnavi_job, jobnavi_args, self.job_config_submit_args
            ):
                # 使用之前的启动时间
                if self.is_user_custom_run_time:
                    # 更新为用户配置的启动时间,并保存用户之前分钟延迟
                    if JobnaviRegisterV1.get_delay_minute(self.existed_jobnavi_job):
                        delay = JobnaviRegisterV1.get_delay_minute(self.existed_jobnavi_job)
                    else:
                        delay = delay_minute
                    to_run_dt_time = datetime.fromtimestamp(int(self.advance["start_time"]) / 1000.0) + timedelta(
                        minutes=delay
                    )
                    jobnavi_args["period"]["first_schedule_time"] = int(time.mktime(to_run_dt_time.timetuple()) * 1000)
                else:
                    # 使用之前的启动时间
                    del jobnavi_args["period"]["first_schedule_time"]
            batch_logger.info(jobnavi_args)
            jobnavi_resp = self.jobnavi.update_schedule_info(jobnavi_args)
        else:
            jobnavi_args["execute_before_now"] = False
            batch_logger.info(jobnavi_args)
            jobnavi_resp = self.jobnavi.create_schedule_info(jobnavi_args)
        return jobnavi_resp

    def create_jobnavi_params(self, cron_expression, parents_args):
        time_zone = pizza_settings.TIME_ZONE
        period = {
            "timezone": time_zone,
            "cron_expression": cron_expression,
            "frequency": self.job_config_submit_args["count_freq"],
        }
        if self.job_config_submit_args["schedule_period"] == "hour":
            period["period_unit"] = "H"
        elif self.job_config_submit_args["schedule_period"] == "day":
            period["period_unit"] = "d"
        elif self.job_config_submit_args["schedule_period"] == "week":
            period["period_unit"] = "W"
        elif self.job_config_submit_args["schedule_period"] == "month":
            period["period_unit"] = "M"
        else:
            period["period_unit"] = ""

        if cron_expression:
            period["first_schedule_time"] = int(time.time() * 1000)
        else:
            period["first_schedule_time"] = str(self.to_run_time)

        if self.job_config_submit_args["delay"]:
            period["delay"] = str(self.job_config_submit_args["delay"]) + "H"
        else:
            if period["period_unit"] == "d" or period["period_unit"] == "W" or period["period_unit"] == "M":
                delay = random.randint(1, 5)
                period["delay"] = str(delay) + "H"
            else:
                period["delay"] = ""

        data_time_offset = JobnaviRegisterV1.get_jobnavi_data_offset(
            self.job_config_submit_args, self.data_processing_id
        )
        node_label = self.get_jobnavi_node_label()
        type_id = self.get_type_id()
        extra_info = self.create_jobnavi_extra_info()
        jobnavi_args = {
            "schedule_id": self.data_processing_id,
            "description": " Project {} submit by {}".format(self.data_processing_id, self.created_by),
            "type_id": type_id,
            "period": period,
            "parents": parents_args,
            "execute_before_now": self.is_restart,
            "active": False,
            "node_label": node_label,
            "data_time_offset": data_time_offset,
            "recovery": {"enable": False, "interval_time": "1H", "retry_times": 0},
            "extra_info": str(json.dumps(extra_info)),
        }

        self.create_recovery_info(jobnavi_args)
        return jobnavi_args

    def create_recovery_info(self, jobnavi_args):
        if self.advance and "recovery_enable" in self.advance:
            jobnavi_args["recovery"]["enable"] = self.advance["recovery_enable"]
        if self.advance and "recovery_interval" in self.advance:
            jobnavi_args["recovery"]["interval_time"] = self.advance["recovery_interval"]
        if self.advance and "recovery_times" in self.advance:
            jobnavi_args["recovery"]["retry_times"] = self.advance["recovery_times"]
        return jobnavi_args

    def get_type_id(self):
        if self.job_config_submit_args.get("batch_type", "default") == "spark_python_code":
            type_id = "spark_python_code"
        else:
            type_id = "spark_sql"
        return type_id

    def create_jobnavi_extra_info(self):
        if self.job_config_submit_args.get("batch_type", "default") == "spark_python_code":
            extra_info = {}
        else:
            extra_info = {
                "run_mode": pizza_settings.RUN_MODE,
                "makeup": False,
                "debug": False,
                "type": "batch_sql",
                "queue": self.job_info.cluster_name,
                "cluster_group_id": self.job_info.cluster_group,
                "service_type": "batch",
                "component_type": "spark",
                "engine_conf_path": "/dataflow/batch/jobs/{job_id}/get_engine_conf/".format(
                    job_id=self.data_processing_id
                ),
            }

            job_config = json.loads(self.job_info.job_config)
            if "resource_group_id" in job_config:
                resource_group_id = json.loads(self.job_info.job_config)["resource_group_id"]
                extra_info["resource_group_id"] = resource_group_id

            if pizza_settings.RUN_VERSION == pizza_settings.RELEASE_ENV:
                extra_info["license"] = {
                    "license_server_url": pizza_settings.LICENSE_SERVER_URL,
                    "license_server_file_path": pizza_settings.LICENSE_SERVER_FILE_PATH,
                }
        extra_info["geog_area_code"] = self.geog_area_code
        return extra_info

    def get_jobnavi_node_label(self):
        node_label = JobnaviRegisterV1.gen_jobnavi_node_label(
            self.job_info, self.existed_jobnavi_job, self.geog_area_code
        )
        batch_logger.info("Get current node_label {} for {}".format(node_label, self.data_processing_id))
        return node_label

    def calculate_delay_min(self, schedule_time):
        sr = time.gmtime(schedule_time / 1000.0)
        # delay 10min to ensure data in position
        local_time_min = int(time.strftime("%M", time.localtime()))
        if 32 > local_time_min > 5:
            delay_minute = local_time_min + 5
        else:
            delay_minute = 0 if sr.tm_min > 10 else 11 + random.randint(0, 20)
        if self.job_config_submit_args.get("batch_type", "default") in ["tdw", "tdw_jar"]:
            # tdw 节点不要设置延迟
            delay_minute = 0
        return delay_minute

    def get_dt_by_schedule_time(self, schedule_time, delay_minute):
        if self.job_config_submit_args["schedule_period"] == "week":
            schedule_time = time_util.get_monday_timestamp(schedule_time / 1000) * 1000
        elif self.job_config_submit_args["schedule_period"] == "month":
            schedule_time = time_util.get_month_first_day_timestamp(schedule_time / 1000) * 1000
        else:
            schedule_time = time_util.get_today_zero_hour_timestamp(schedule_time / 1000) * 1000
        dt_time = datetime.fromtimestamp(schedule_time / 1000.0) + timedelta(minutes=delay_minute)
        return dt_time

    def gen_accumulate_cron_expression(self, existed_jobnavi_job, dt_time):
        cron_expression = ""
        if self.job_config_submit_args["accumulate"]:
            data_start = self.job_config_submit_args["data_start"]
            data_end = self.job_config_submit_args["data_end"]
            # cron_expression start time must delay 1h
            job_delay_minute = str(dt_time.minute)
            if existed_jobnavi_job:
                old_cron_expression = existed_jobnavi_job["period"]["cron_expression"]
                if old_cron_expression:
                    batch_logger.info("old cron expression: " + old_cron_expression)
                    # split cron, min index 1
                    job_delay_minute = old_cron_expression.split(" ")[1]
            batch_logger.info("job_delay_minute: " + job_delay_minute)
            if data_start == 0 and data_end == 23:
                cron_expression = "0 " + job_delay_minute + " 0/1 * * ?"
            elif data_start != 0 and data_end == 23:
                cron_expression = "0 " + job_delay_minute + " 0," + str(data_start + 1) + "-23 * * ?"
            else:
                cron_expression = (
                    "0 " + job_delay_minute + " " + str(data_start + 1) + "-" + str(data_end + 1) + " * * ?"
                )
        return cron_expression

    def generate_parent_params(self):
        parents_args = []
        for parent_rt_id in self.job_config_submit_args["result_tables"]:
            dependency_rule = (
                self.job_config_submit_args["result_tables"][parent_rt_id]["dependency_rule"]
                if "dependency_rule" in self.job_config_submit_args["result_tables"][parent_rt_id]
                else "all_finished"
            )
            result_table = ResultTableHelper.get_result_table(parent_rt_id, related="data_processing")
            parent_rt_type = result_table["processing_type"]
            if "data_processing" in result_table and "processing_id" in result_table["data_processing"]:
                parent_processing_id = result_table["data_processing"]["processing_id"]
            else:
                parent_processing_id = parent_rt_id

            if parent_rt_type == "batch" or result_table_util.is_model_serve_mode_offline(parent_processing_id):
                # mod parent_id 应该为 parent_processing_id
                parent_args = self.gen_batch_parent_arg(parent_processing_id, dependency_rule)
                parents_args.append(parent_args)

            elif DatabusHelper.is_rt_batch_import(parent_rt_id):
                parent_args = self.gen_batch_import_upstream_args(parent_processing_id, dependency_rule)
                parents_args.append(parent_args)
        return parents_args

    def gen_self_dependency_parent_args(self):
        current_min_window, current_min_window_unit = result_table_util.get_batch_min_window_size(
            self.job_config_submit_args, self.data_processing_id
        )
        parent_args = {
            "parent_id": self.data_processing_id,
            "dependency_rule": self.advance["self_dependency_config"]["dependency_rule"],
            "param_type": "fixed",
        }
        if self.job_config_submit_args["schedule_period"] == "hour":
            parent_args["param_value"] = str(self.job_config_submit_args["count_freq"]) + "H"
        elif self.job_config_submit_args["schedule_period"] == "day":
            parent_args["param_value"] = str(self.job_config_submit_args["count_freq"]) + "d"
        elif self.job_config_submit_args["schedule_period"] == "week":
            parent_args["param_value"] = str(self.job_config_submit_args["count_freq"]) + "W"
        elif self.job_config_submit_args["schedule_period"] == "month":
            parent_args["param_value"] = str(self.job_config_submit_args["count_freq"]) + "M"
        parent_args["window_offset"] = JobnaviRegisterV1.calculate_jobnavi_window_offset(
            self.job_config_submit_args["count_freq"],
            self.job_config_submit_args["schedule_period"],  # 由于flow限制调度时间单位与窗口单位一致
            current_min_window,
            current_min_window_unit,
            self.job_config_submit_args["schedule_period"],
        )
        return parent_args

    def gen_batch_parent_arg(self, parent_rt_id, dependency_rule):
        parent_args = {
            "parent_id": parent_rt_id,
            "dependency_rule": dependency_rule,
        }

        if self.job_config_submit_args["accumulate"]:
            parent_args["param_type"] = "range"
            data_start = self.job_config_submit_args["data_start"]
            parent_args["param_value"] = str(data_start + 1) + "H~$NOWH"
        else:
            parent_args["param_type"] = "fixed"
            parent_submit_args = result_table_util.get_rt_job_submit_args_from_db(parent_rt_id)
            parent_schedule_period = parent_submit_args["schedule_period"]
            (
                parent_min_window_size,
                parent_min_window_size_unit,
            ) = result_table_util.get_batch_min_window_size(parent_submit_args, parent_rt_id)
            window_size = self.job_config_submit_args["result_tables"][parent_rt_id]["window_size"]
            window_delay = self.job_config_submit_args["result_tables"][parent_rt_id]["window_delay"]
            if parent_schedule_period == "hour" and self.job_config_submit_args["schedule_period"] == "hour":
                parent_args["param_value"] = str(window_size) + "H"
            elif parent_schedule_period == "hour" and self.job_config_submit_args["schedule_period"] == "day":
                parent_args["param_value"] = str(window_size * 24) + "H"
            elif parent_schedule_period == "day" and self.job_config_submit_args["schedule_period"] == "day":
                parent_args["param_value"] = str(window_size) + "d"
            elif self.job_config_submit_args["schedule_period"] == "week":
                parent_args["param_value"] = str(window_size) + "W"
            elif self.job_config_submit_args["schedule_period"] == "month":
                parent_args["param_value"] = str(window_size) + "M"
            parent_args["window_offset"] = JobnaviRegisterV1.calculate_jobnavi_window_offset(
                window_delay,
                self.job_config_submit_args["schedule_period"],  # 由于flow限制调度时间单位与窗口单位一致
                parent_min_window_size,
                parent_min_window_size_unit,
                parent_schedule_period,
            )
        return parent_args

    def gen_batch_import_upstream_args(self, parent_rt_id, created_by):
        default_args = {
            "schedule_id": parent_rt_id,
            "description": " Project {} submit by {}".format(self.data_processing_id, created_by),
            "type_id": "default",
        }
        parent_args = {"parent_id": parent_rt_id, "dependency_rule": "all_finished"}
        if self.job_config_submit_args["accumulate"]:
            parent_args["param_type"] = "range"
            data_start = self.job_config_submit_args["data_start"]
            parent_args["param_value"] = str(data_start + 1) + "H~$NOWH"
        else:
            parent_args["param_type"] = "fixed"
            if self.job_config_submit_args["schedule_period"] == "hour":
                parent_args["param_value"] = str(self.job_config_submit_args["count_freq"]) + "H"
            elif self.job_config_submit_args["schedule_period"] == "day":
                parent_args["param_value"] = str(self.job_config_submit_args["count_freq"] * 24) + "H"
            elif self.job_config_submit_args["schedule_period"] == "week":
                parent_args["param_value"] = str(self.job_config_submit_args["count_freq"] * 24 * 7) + "H"
            elif self.job_config_submit_args["schedule_period"] == "month":
                parent_args["param_value"] = str(self.job_config_submit_args["count_freq"]) + "M"

        batch_logger.info(default_args)

        parent_rtn = self.jobnavi.get_schedule_info(parent_rt_id)
        if parent_rtn:
            self.jobnavi.update_schedule_info(default_args)
        else:
            self.jobnavi.create_schedule_info(default_args)
        return parent_args

    @staticmethod
    def get_delay_minute(existed_jobnavi_job):
        if existed_jobnavi_job["period"] and existed_jobnavi_job["period"]["first_schedule_time"]:
            datetime = time_util.get_datetime(existed_jobnavi_job["period"]["first_schedule_time"] / 1000)
            return datetime.minute

    @staticmethod
    def is_rt_period_equal(old, new, prop):
        batch_logger.info(old)
        if old["period"]:
            if (
                old["period"]["frequency"]
                and old["period"]["frequency"] == new["period"]["frequency"]
                and old["period"]["period_unit"]
                and old["period"]["period_unit"] == prop["schedule_period"]
            ):
                return True
        return False

    @staticmethod
    def gen_jobnavi_node_label(job_info, jobnavi_response, geog_area_code):
        try:
            # 离线计算配置库中任务级别标签最高
            if job_info.deploy_config is not None:
                deploy_config = json.loads(job_info.deploy_config)
                if "node_label" in deploy_config:
                    node_label = deploy_config["node_label"]
                    return node_label
        except Exception as e:
            batch_logger.exception(e)

        default_node_label, _ = resource_util.get_default_jobnavi_label(geog_area_code, "batch", component_type="spark")
        node_label, _ = resource_util.get_jobnavi_label(geog_area_code, job_info.cluster_group, "batch")
        batch_logger.info(
            "Node label from resource center, default node label: {}, node_lable {}".format(
                default_node_label, node_label
            )
        )
        # 为了兼容jobnavi已有标签的任务,如果资源系统返回标签非默认标签使用资源系统返回标签，
        # 如果返回标签为默认标签则判断jobnavi任务是否已存在标签，如果已存在标签则使用已存在标签
        if (
            node_label == default_node_label
            and jobnavi_response is not None
            and jobnavi_response["node_label"] in settings.SPARK_SQL_NODE_LABEL_DEFAULT_LIST
        ):
            node_label = jobnavi_response["node_label"]
        return node_label

    @staticmethod
    def get_jobnavi_data_offset(submit_args, result_table_id):
        # 传入偏移值支持数据时间依赖
        data_time_offset, data_time_offset_unit = result_table_util.get_batch_min_window_size(
            submit_args, result_table_id
        )
        if data_time_offset_unit == "hour":
            data_time_offset_for_jobnavi = str(data_time_offset) + "H"
        elif data_time_offset_unit == "month":
            data_time_offset_for_jobnavi = str(data_time_offset) + "M"
        else:
            raise Exception("Window offset unit can only be hour or month")
        return data_time_offset_for_jobnavi

    @staticmethod
    def calculate_jobnavi_window_offset(
        window_delay,
        window_unit,
        parent_min_window_size,
        parent_min_window_size_unit,
        parent_freq_unit,
    ):
        """
        数据依赖中兼容离线计算v1，依照调度时间依赖计算窗口延迟规则
        :param window_delay: 当前窗口延迟
        :param window_unit:
        当前节点窗口单位,单位为hour, day, week, month, 需大于等于parent_freq_unit, parent_min_window_size_unit
        :param parent_min_window_size: 父表最小窗口
        :param parent_min_window_size_unit: 父表最小窗口大小,只有小时和月两种单位
        :param parent_freq_unit: 父表统计单位,单位为hour, day, week, month
        :return:
        """

        if window_unit == "hour":
            window_offset = str(window_delay + parent_min_window_size - 1) + "H"
        elif window_unit == "day":
            if parent_freq_unit == "hour":
                window_offset = str(window_delay * 24 + parent_min_window_size - 1) + "H"
            else:
                window_offset = str(window_delay * 24 + parent_min_window_size - 1 * 24) + "H"

        elif window_unit == "week":
            if parent_freq_unit == "hour":
                window_offset = str(window_delay * 24 * 7 + parent_min_window_size - 1) + "H"
            elif parent_freq_unit == "day":
                window_offset = str(window_delay * 24 * 7 + parent_min_window_size - 1 * 24) + "H"
            else:
                window_offset = str(window_delay * 24 * 7 + parent_min_window_size - 1 * 24 * 7) + "H"

        elif window_unit == "month" and parent_min_window_size == 0:
            window_offset = str(window_delay) + "M"
        # 由于flow的限制，最小窗口单位为月时，父表频率单位必定是月
        elif window_unit == "month" and parent_min_window_size_unit == "month":
            window_offset = str(window_delay + parent_min_window_size - 1) + "M"
        # else 条件应为 window_unit == 'month' and parent_min_window_size_unit != "month":
        else:
            window_offset = str(window_delay) + "M"
            if parent_freq_unit == "hour":
                hour_offset = str(parent_min_window_size - 1) + "H"
            elif parent_freq_unit == "day":
                hour_offset = str(parent_min_window_size - 1 * 24) + "H"
            else:
                hour_offset = str(parent_min_window_size - 1 * 24 * 7) + "H"
            if hour_offset != 0:
                window_offset = window_offset + "+" + str(hour_offset)
        return window_offset
