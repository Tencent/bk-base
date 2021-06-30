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

from common.log import sys_logger
from django.utils.translation import ugettext as _

from dataflow.pizza_settings import JOBNAVI_DEFAULT_CLUSTER_ID, JOBNAVI_STREAM_CLUSTER_ID
from dataflow.shared.api.modules.jobnavi import JobNaviApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class JobNaviHelper(object):
    def __init__(self, geog_area_code, cluster_id):
        self.cluster_id = cluster_id
        self.geog_area_code = geog_area_code

    @classmethod
    def get_jobnavi_cluster(cls, module):
        """
        通过配置获取 module 对应的 jobnavi cluster id
        先决条件：不同地域的jobnavi集群 cluster id 是一致的

        :param module: stream/batch
        :return:  cluster_id
        """
        if module == "stream":
            return JOBNAVI_STREAM_CLUSTER_ID
        elif module == "batch":
            return JOBNAVI_DEFAULT_CLUSTER_ID
        else:
            raise Exception(_("获取 jobnavi 集群 ID 失败 ，不支持的 %s" % module))

    @staticmethod
    def list_jobnavi_cluster_config(geog_area_code=None):
        request_params = {}
        if geog_area_code:
            request_params["tags"] = [geog_area_code]
        res = JobNaviApi.cluster_config.list(request_params)
        res_util.check_response(res)
        return res.data

    def get_schedule_info(self, schedule_id):
        res = JobNaviApi.schedule.retrieve(
            {
                "schedule_id": schedule_id,
                "tags": [self.geog_area_code],
                "cluster_id": self.cluster_id,
            }
        )
        res_util.check_response(res)
        return res.data

    def update_schedule_info(self, args):
        args["cluster_id"] = self.cluster_id
        args["tags"] = [self.geog_area_code]
        res = JobNaviApi.schedule.partial_update(args)
        res_util.check_response(res)
        return res.data

    def overwrite_schedule_info(self, args):
        args["cluster_id"] = self.cluster_id
        args["tags"] = self.geog_area_code
        res = JobNaviApi.schedule.update(args)
        res_util.check_response(res)
        return res.data

    def create_schedule_info(self, args, timeout_auto_retry=True):
        args["cluster_id"] = self.cluster_id
        args["tags"] = [self.geog_area_code]
        if timeout_auto_retry:
            res = JobNaviApi.schedule.create(args)
        else:
            res = JobNaviApi.schedule_without_retry.create(args)
        res_util.check_response(res)
        return res.data

    def create_execute_info(self, args):
        args["cluster_id"] = self.cluster_id
        args["tags"] = [self.geog_area_code]
        res = JobNaviApi.execute.create(args)
        res_util.check_response(res)
        return res.data

    def start_schedule(self, schedule_id):
        request_params = {
            "schedule_id": schedule_id,
            "tags": [self.geog_area_code],
            "cluster_id": self.cluster_id,
            "active": True,
        }
        res = JobNaviApi.schedule.partial_update(request_params)
        res_util.check_response(res)
        return res.data

    def start_schedule_with_exec_on_create(self, schedule_id):
        request_params = {
            "schedule_id": schedule_id,
            "tags": [self.geog_area_code],
            "cluster_id": self.cluster_id,
            "active": True,
            "exec_oncreate": True,
        }
        res = JobNaviApi.schedule.partial_update(request_params)
        res_util.check_response(res)
        return res.data

    def stop_schedule(self, schedule_id):
        request_params = {
            "schedule_id": schedule_id,
            "tags": [self.geog_area_code],
            "cluster_id": self.cluster_id,
            "active": False,
        }
        res = JobNaviApi.schedule.partial_update(request_params)
        res_util.check_response(res)
        return res.data

    def force_schedule(self, schedule_id):
        request_params = {
            "schedule_id": schedule_id,
            "cluster_id": self.cluster_id,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.schedule.force_schedule(request_params)
        res_util.check_response(res)
        return res.data

    def delete_schedule(self, schedule_id, auto_kill_execute=False):
        if auto_kill_execute:
            execute_list = self.get_execute_result_by_status(schedule_id, "running")
            for execute_info in execute_list:
                execute_id = execute_info["execute_info"]["id"]
                sys_logger.info("kill execute " + str(execute_id))
                try:
                    self.kill_execute(execute_id)
                except Exception:
                    pass
        res = JobNaviApi.schedule.delete(
            {
                "schedule_id": schedule_id,
                "cluster_id": self.cluster_id,
                "tags": [self.geog_area_code],
            }
        )
        res_util.check_response(res)
        return res.data

    def send_event(self, args, timeout_auto_retry=True):
        args["cluster_id"] = self.cluster_id
        args["tags"] = [self.geog_area_code]
        if timeout_auto_retry:
            res = JobNaviApi.event.create(args)
        else:
            res = JobNaviApi.event_without_retry.create(args)
        res_util.check_response(res)
        return res.data

    def get_event(self, event_id):
        res = JobNaviApi.event.retrieve(
            {
                "event_id": event_id,
                "cluster_id": self.cluster_id,
                "tags": [self.geog_area_code],
            }
        )
        res_util.check_response(res)
        return res.data

    def execute_schedule(self, args):
        args["cluster_id"] = self.cluster_id
        args["tags"] = [self.geog_area_code]
        res = JobNaviApi.execute.create(args)
        res_util.check_response(res)
        return res.data

    def get_execute_result_by_status(self, schedule_id, status):
        params = {
            "cluster_id": self.cluster_id,
            "schedule_id": schedule_id,
            "status": status,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.execute.list(params)
        res_util.check_response(res)
        return res.data

    def get_schedule_execute_result(self, schedule_id, limit):
        params = {
            "cluster_id": self.cluster_id,
            "schedule_id": schedule_id,
            "limit": limit,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.execute.list(params)
        res_util.check_response(res)
        return res.data

    def get_execute_status(self, execute_id):
        res = JobNaviApi.execute.retrieve(
            {
                "execute_id": execute_id,
                "cluster_id": self.cluster_id,
                "field": "status",
                "tags": [self.geog_area_code],
            }
        )
        res_util.check_response(res)
        return res.data

    def get_recovery_execute(self, schedule_id, limit):
        res = JobNaviApi.execute.list_recovery(
            {"schedule_id": schedule_id, "limit": limit, "tags": [self.geog_area_code]}
        )
        res_util.check_response(res)
        return res.data

    def get_submitted_history_by_time(self, schedule_id, start_time, end_time):
        res = JobNaviApi.execute.query_submitted_by_time(
            {
                "schedule_id": schedule_id,
                "cluster_id": self.cluster_id,
                "start_time": start_time,
                "end_time": end_time,
            }
        )
        res_util.check_response(res)
        return res.data

    def kill_execute(self, execute_id):
        args = {
            "event_name": "kill",
            "execute_id": execute_id,
            "change_status": "killed",
        }
        return self.send_event(args)

    def rerun(self, rerun_processings, rerun_model, start_time, end_time):
        args = {
            "rerun_processings": rerun_processings,
            "rerun_model": rerun_model,
            "start_time": start_time,
            "end_time": end_time,
            "cluster_id": self.cluster_id,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.execute.rerun(args)
        res_util.check_response(res)
        return res.data

    def run(self, rerun_processings, rerun_model, start_time, end_time):
        args = {
            "rerun_processings": rerun_processings,
            "rerun_model": rerun_model,
            "start_time": start_time,
            "end_time": end_time,
            "cluster_id": self.cluster_id,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.execute.run(args)
        res_util.check_response(res)
        return res.data

    def data_makeup(
        self,
        processing_id,
        rerun_processings,
        rerun_model,
        target_schedule_time,
        source_schedule_time,
        dispatch_to_storage,
    ):
        args = {
            "processing_id": processing_id,
            "rerun_processings": rerun_processings,
            "rerun_model": rerun_model,
            "target_schedule_time": target_schedule_time,
            "source_schedule_time": source_schedule_time,
            "dispatch_to_storage": dispatch_to_storage,
            "cluster_id": self.cluster_id,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.data_makeup.create(args)
        res_util.check_response(res)
        return res.data

    def data_makeup_status_list(self, schedule_id, start_time, end_time):
        args = {
            "schedule_id": schedule_id,
            "start_time": start_time,
            "end_time": end_time,
            "cluster_id": self.cluster_id,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.data_makeup.list(args)
        res_util.check_response(res)
        return res.data

    def data_makeup_check_allowed(self, schedule_id, schedule_time):
        args = {
            "schedule_id": schedule_id,
            "schedule_time": schedule_time,
            "cluster_id": self.cluster_id,
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.data_makeup.check_allowed(args)
        res_util.check_response(res)
        return res.data

    def upload_jar(self, args, file):
        args["cluster_id"] = self.cluster_id
        args["tags"] = [self.geog_area_code]
        res = JobNaviApi.task_type.upload(args, files={"file": file})

        res_util.check_response(res)
        return res.data

    def get_task_submit_log(self, execute_id, begin, end):
        res = JobNaviApi.task_log.retrieve(
            {
                "execute_id": execute_id,
                "cluster_id": self.cluster_id,
                "begin": begin,
                "end": end,
            }
        )
        res_util.check_response(res)
        return res.data

    def get_task_submit_log_file_size(self, execute_id):
        res = JobNaviApi.task_log.query_file_size({"execute_id": execute_id, "cluster_id": self.cluster_id})
        res_util.check_response(res)
        return res.data

    def get_application_id(self, execute_id):
        res = JobNaviApi.task_log.query_application_id({"execute_id": execute_id, "cluster_id": self.cluster_id})
        res_util.check_response(res)
        return res.data

    def calculate_schedule_task_time(self, rerun_processings, rerun_model, start_time, end_time):
        args = {
            "rerun_processings": rerun_processings,
            "rerun_model": rerun_model,
            "start_time": start_time,
            "end_time": end_time,
            "cluster_id": self.cluster_id,
            "admin_type": "schedule",
            "tags": [self.geog_area_code],
        }
        res = JobNaviApi.admin.calculate_schedule_task_time(args)
        res_util.check_response(res)
        return res.data
