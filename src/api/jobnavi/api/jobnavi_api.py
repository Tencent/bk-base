# coding=utf-8
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
from django.utils.translation import ugettext as _

from common.api.base import DataAPI
from common.exceptions import CommonCode
from jobnavi.exception.exceptions import JobNaviException
from jobnavi.config import jobnavi_config


class _JobNaviApi(object):
    def __init__(self, geog_area_code, cluster_name="default"):
        if not cluster_name:
            cluster_name = "default"
        self.geog_area_code = geog_area_code
        self.cluster_name = cluster_name
        _jobnavi_config = jobnavi_config.get_jobnavi_config()
        if not _jobnavi_config:
            raise JobNaviException(message=_("没有JobNavi集群信息."), code=CommonCode.HTTP_500)
        domain = _jobnavi_config.get((self.geog_area_code, self.cluster_name), None)
        if not domain:
            raise JobNaviException(
                message=_("没有JobNavi集群(geog_area_code: %s, cluster_name: %s)配置.") %
                         (self.geog_area_code, cluster_name),
                code=CommonCode.HTTP_500)
        self.jobnavi_url = "http://" + domain

    def healthz(self):
        healthz_api = DataAPI(
            url=self.jobnavi_url + "/healthz",
            method="GET",
            module="jobnavi",
            description="获取jobnavi的健康状态"
        )
        return healthz_api()

    def add_schedule(self, args):
        args["operate"] = "add"
        add_schedule_api = DataAPI(
            url=self.jobnavi_url + "/schedule",
            method="POST",
            module="jobnavi",
            description="添加调度任务"
        )
        return add_schedule_api(args)

    def del_schedule(self, schedule_id):
        args = {
            "operate": "delete",
            "schedule_id": schedule_id
        }
        del_schedule_api = DataAPI(
            url=self.jobnavi_url + "/schedule",
            method="GET",
            module="jobnavi",
            description="删除调度任务"
        )
        return del_schedule_api(args)

    def get_schedule(self, schedule_id):
        args = {
            "operate": "get",
            "schedule_id": schedule_id
        }
        get_schedule_api = DataAPI(
            url=self.jobnavi_url + "/schedule",
            method="GET",
            module="jobnavi",
            description="获取调度任务"
        )
        return get_schedule_api(args)

    def update_schedule(self, args):
        args["operate"] = "update"
        update_schedule_api = DataAPI(
            url=self.jobnavi_url + "/schedule",
            method="POST",
            module="jobnavi",
            description="更新部分调度任务元信息"
        )
        return update_schedule_api(args)

    def overwrite_schedule(self, args):
        args["operate"] = "overwrite"
        overwrite_schedule_api = DataAPI(
            url=self.jobnavi_url + "/schedule",
            method="POST",
            module="jobnavi",
            description="覆盖调度任务元信息"
        )
        return overwrite_schedule_api(args)

    def force_schedule(self, schedule_id):
        args = {
            "operate": "force_schedule",
            "schedule_id": schedule_id
        }
        force_schedule_api = DataAPI(
            url=self.jobnavi_url + "/schedule",
            method="GET",
            module="jobnavi",
            description="强制调度"
        )
        return force_schedule_api(args)

    def execute_schedule(self, schedule_id, schedule_time):
        args = {
            "schedule_id": schedule_id
        }
        if schedule_time:
            args["schedule_time"] = str(schedule_time)
        execute_schedule_api = DataAPI(
            url=self.jobnavi_url + "/execute",
            method="GET",
            module="jobnavi",
            description="执行调度任务"
        )
        return execute_schedule_api(args)

    def get_execute_lifespan(self, schedule_id):
        args = {
            "operate": "get_execute_lifespan",
            "schedule_id": schedule_id
        }
        get_execute_lifespan_api = DataAPI(
            url=self.jobnavi_url + "/schedule",
            method="GET",
            module="jobnavi",
            description="获取调度任务实例生命期限"
        )
        return get_execute_lifespan_api(args)

    def get_execute_status(self, execute_id):
        args = {
            "operate": "query_execute_status",
            "execute_id": execute_id
        }
        get_execute_status_api = DataAPI(
            url=self.jobnavi_url + "/result",
            method="GET",
            module="jobnavi",
            description="查询任务执行状态"
        )
        return get_execute_status_api(args)

    def query_execute(self, schedule_id, limit):
        args = {
            "operate": "query_execute",
            "schedule_id": schedule_id,
            "limit": limit
        }
        query_execute_api = DataAPI(
            url=self.jobnavi_url + "/result",
            method="GET",
            module="jobnavi",
            description="查询任务执行状态"
        )
        return query_execute_api(args)

    def query_execute_by_status(self, schedule_id, schedule_time, status):
        args = {
            "operate": "query_execute",
            "schedule_id": schedule_id,
            "schedule_time": schedule_time,
            "status": status
        }
        query_execute_by_status_api = DataAPI(
            url=self.jobnavi_url + "/result",
            method="GET",
            module="jobnavi",
            description="查询任务执行状态"
        )
        return query_execute_by_status_api(args)

    def query_execute_by_time_range(self, schedule_id, start_time, end_time):
        args = {
            "operate": "query_execute_by_time",
            "schedule_id": schedule_id,
            "start_time": start_time,
            "end_time": end_time,
        }
        query_execute_by_time_range_api = DataAPI(
            url=self.jobnavi_url + "/result",
            method="GET",
            module="jobnavi",
            description="按调度时间范围查询任务实例"
        )
        return query_execute_by_time_range_api(args)

    def fetch_today_job_status(self):
        args = {
            "operate": "fetch_today_job_status"
        }
        fetch_today_job_status_api = DataAPI(
            url=self.jobnavi_url + "/debug",
            method="GET",
            module="jobnavi",
            description="查询近24小时任务执行状态"
        )
        return fetch_today_job_status_api(args)

    def fetch_last_hour_job_status(self):
        args = {
            "operate": "fetch_last_hour_job_status"
        }
        fetch_last_hour_job_status_api = DataAPI(
            url=self.jobnavi_url + "/debug",
            method="GET",
            module="jobnavi",
            description="查询上一小时任务执行状态"
        )
        return fetch_last_hour_job_status_api(args)

    def fetch_job_status_by_create_time(self):
        args = {
            "operate": "fetch_job_status_by_create_time"
        }
        fetch_job_status_by_create_time_api = DataAPI(
            url=self.jobnavi_url + "/debug",
            method="GET",
            module="jobnavi",
            description="查询近24小时生成的任务执行状态"
        )
        return fetch_job_status_by_create_time_api(args)

    def send_event(self, execute_id, event_name, change_status, event_info):
        args = {
            "execute_id": execute_id,
            "event_name": event_name
        }
        if change_status:
            args["change_status"] = change_status
        if event_info:
            args["event_info"] = event_info
        send_event_api = DataAPI(
            url=self.jobnavi_url + "/event",
            method="POST",
            module="jobnavi",
            description="创建任务事件"
        )
        return send_event_api(args)

    def get_event_result(self, event_id):
        args = {
            "operate": "query",
            "event_id": event_id
        }

        get_event_api = DataAPI(
            url=self.jobnavi_url + "/event_result",
            method="GET",
            module="jobnavi",
            description="创建任务事件"
        )
        return get_event_api(args)

    def query_processing_event_amount(self):
        args = {
            "operate": "query_processing_event_amount"
        }

        get_event_api = DataAPI(
            url=self.jobnavi_url + "/debug",
            method="GET",
            module="jobnavi",
            description="查询正在处理任务事件总数"
        )
        return get_event_api(args)

    def rerun(self, rerun_processings, rerun_model, start_time, end_time, exclude_statuses, priority="low"):
        args = {
            "rerun_processings": rerun_processings,
            "rerun_model": rerun_model,
            "start_time": start_time,
            "end_time": end_time,
            "exclude_statuses": exclude_statuses,
            "priority": priority
        }

        get_event_api = DataAPI(
            url=self.jobnavi_url + "/rerun",
            method="POST",
            module="jobnavi",
            description="任务重运行"
        )
        return get_event_api(args)

    def redo(self, schedule_id, schedule_time, is_run_depend, force, exclude_statuses):
        args = {
            "schedule_id": schedule_id,
            "schedule_time": schedule_time,
            "is_run_depend": is_run_depend,
            "force": force,
            "exclude_statuses": exclude_statuses
        }

        get_event_api = DataAPI(
            url=self.jobnavi_url + "/redo",
            method="GET",
            module="jobnavi",
            description="任务重运行redo"
        )
        return get_event_api(args)

    def admin_schedule_calculate_task_time(self, rerun_processings, rerun_model, start_time, end_time):
        args = {
            "operate": "calculate_schedule_task_time",
            "rerun_processings": rerun_processings,
            "rerun_model": rerun_model,
            "start_time": start_time,
            "end_time": end_time
        }

        get_event_api = DataAPI(
            url=self.jobnavi_url + "/admin/schedule",
            method="POST",
            module="jobnavi",
            description="查询调度任务时间"
        )
        return get_event_api(args)

    def query_failed_executes(self, begin_time, end_time, type_id):
        args = {
            "operate": "query_failed_executes",
            "begin_time": begin_time,
            "end_time": end_time,
            "type_id": type_id
        }

        query_failed_executes_api = DataAPI(
            url=self.jobnavi_url + "/admin/schedule",
            method="GET",
            module="jobnavi",
            description="查询失败调度任务实例"
        )
        return query_failed_executes_api(args)

    def query_current_runners(self):
        args = {
            "operate": "get_current_runners"
        }

        query_current_runners = DataAPI(
            url=self.jobnavi_url + "/sys/runner",
            method="GET",
            module="jobnavi",
            description="查询当前的runner信息"
        )
        return query_current_runners(args)

    def list_data_makeup_execute(self, schedule_id, start_time, end_time, status):
        args = {
            "operate": "list",
            "schedule_id": schedule_id,
            "start_time": start_time,
            "end_time": end_time,
            "status": status
        }
        list_data_makeup_execute_api = DataAPI(
            url=self.jobnavi_url + "/patch",
            method="GET",
            module="jobnavi",
            description="查询补齐数据实例"
        )
        return list_data_makeup_execute_api(args)

    def check_data_makeup_allowed(self, schedule_id, schedule_time):
        args = {
            "operate": "check_allowed",
            "schedule_id": schedule_id,
            "schedule_time": schedule_time
        }
        check_data_makeup_allowed_api = DataAPI(
            url=self.jobnavi_url + "/patch",
            method="GET",
            module="jobnavi",
            description="检查执行记录是否允许补齐"
        )
        return check_data_makeup_allowed_api(args)

    def run_data_makeup(self, processing_id, rerun_processings, rerun_model, target_schedule_time, source_schedule_time,
                        dispatch_to_storage):
        args = {
            "operate": "run",
            "processing_id": processing_id,
            "rerun_processings": rerun_processings,
            "rerun_model": rerun_model,
            "target_schedule_time": target_schedule_time,
            "source_schedule_time": source_schedule_time,
            "dispatch_to_storage": dispatch_to_storage
        }
        run_data_makeup_api = DataAPI(
            url=self.jobnavi_url + "/patch",
            method="POST",
            module="jobnavi",
            description="提交补齐任务"
        )
        return run_data_makeup_api(args)

    def list_runner_digests(self):
        args = {
            "operate": "get_runner_digests"
        }
        list_runner_digests_api = DataAPI(
            url=self.jobnavi_url + "/sys/runner",
            method="GET",
            module="jobnavi",
            description="查询Runner摘要信息"
        )
        return list_runner_digests_api(args)

    def retrieve_task_log_info(self, execute_id):
        args = {
            "execute_id": execute_id
        }
        retrieve_task_log_info_api = DataAPI(
            url=self.jobnavi_url + "/task_log",
            method="GET",
            module="jobnavi",
            description="查询任务日志信息"
        )
        return retrieve_task_log_info_api(args)

    def retrieve_task_log(self, execute_id, begin, end, log_url, aggregate_time):
        args = {
            "operate": "content",
            "execute_id": execute_id,
            "aggregate_time": aggregate_time,
            "begin": begin,
            "end": end
        }
        retrieve_task_log_api = DataAPI(
            url=log_url,
            method="GET",
            module="jobnavi",
            description="查询任务日志"
        )
        return retrieve_task_log_api(args)

    def query_task_log_file_size(self, execute_id, log_url, aggregate_time):
        args = {
            "operate": "file_size",
            "execute_id": execute_id,
            "aggregate_time": aggregate_time
        }
        query_task_log_file_size_api = DataAPI(
            url=log_url,
            method="GET",
            module="jobnavi",
            description="查询任务日志文件大小"
        )
        return query_task_log_file_size_api(args)

    def query_application_id(self, execute_id, log_url, aggregate_time):
        args = {
            "operate": "extract",
            "execute_id": execute_id,
            "aggregate_time": aggregate_time,
            "regex": "application_\\w+_\\w+"
        }
        query_application_id_api = DataAPI(
            url=log_url,
            method="GET",
            module="jobnavi",
            description="查询任务Tracking URL"
        )
        return query_application_id_api(args)

    def list_runner_node_label(self):
        args = {
            "operate": "list_host_label"
        }
        list_runner_node_labe_api = DataAPI(
            url=self.jobnavi_url + "/label",
            method="GET",
            module="jobnavi",
            description="查询Runner节点标签列表"
        )
        return list_runner_node_labe_api(args)

    def retrieve_runner_node_label(self, runner_id):
        args = {
            "operate": "get_host_label",
            "host": runner_id
        }
        retrieve_runner_node_label_api = DataAPI(
            url=self.jobnavi_url + "/label",
            method="GET",
            module="jobnavi",
            description="查询指定Runner节点标签"
        )
        return retrieve_runner_node_label_api(args)

    def create_runner_node_label(self, runner_id, node_label, description):
        args = {
            "operate": "add_host_label",
            "host": runner_id,
            "label_name": node_label,
            "description": description
        }
        create_runner_node_label_api = DataAPI(
            url=self.jobnavi_url + "/label",
            method="POST",
            module="jobnavi",
            description="绑定Runner节点标签"
        )
        return create_runner_node_label_api(args)

    def delete_runner_node_label(self, runner_id, node_label):
        args = {
            "operate": "delete_host_label",
            "host": runner_id,
            "label_name": node_label
        }
        delete_runner_node_label_api = DataAPI(
            url=self.jobnavi_url + "/label",
            method="POST",
            module="jobnavi",
            description="删除指定Runner节点标签"
        )
        return delete_runner_node_label_api(args)

    def create_runner_node_label_definition(self, node_label, description):
        args = {
            "operate": "add_label",
            "label_name": node_label,
            "description": description
        }
        create_runner_node_label_definition_api = DataAPI(
            url=self.jobnavi_url + "/label",
            method="POST",
            module="jobnavi",
            description="添加Runner节点标签定义"
        )
        return create_runner_node_label_definition_api(args)

    def list_runner_node_label_definition(self):
        args = {
            "operate": "list_label",
        }
        create_runner_node_label_definition_api = DataAPI(
            url=self.jobnavi_url + "/label",
            method="GET",
            module="jobnavi",
            description="获取Runner节点标签定义列表"
        )
        return create_runner_node_label_definition_api(args)

    def delete_runner_node_label_definition(self, node_label):
        args = {
            "operate": "delete_label",
            "label_name": node_label
        }
        delete_runner_node_label_definition_api = DataAPI(
            url=self.jobnavi_url + "/label",
            method="POST",
            module="jobnavi",
            description="删除Runner节点标签定义"
        )
        return delete_runner_node_label_definition_api(args)

    def list_task_type(self):
        args = {
            "operate": "list_type"
        }
        list_task_type_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="GET",
            module="jobnavi",
            description="查询任务类型列表"
        )
        return list_task_type_api(args)

    def create_task_type(self, type_id, tag, main, env, sys_env, language, task_mode, recoverable, created_by,
                         description):
        args = {
            "operate": "create_type",
            "type_id": type_id,
            "tag": tag,
            "main": main,
            "env": env,
            "sys_env": sys_env,
            "language": language,
            "task_mode": task_mode,
            "recoverable": recoverable,
            "created_by": created_by,
            "description": description
        }
        create_task_type_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="POST",
            module="jobnavi",
            description="新增任务类型定义"
        )
        return create_task_type_api(args)

    def delete_task_type_tag(self, type_id, tag):
        args = {
            "operate": "delete_tag_alias",
            "type_id": type_id,
            "tag": tag
        }
        delete_task_type_tag_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="POST",
            module="jobnavi",
            description="删除任务类型tag定义"
        )
        return delete_task_type_tag_api(args)

    def retrieve_task_type_tag_alias(self, type_id, tag):
        args = {
            "operate": "retrieve_tag_alias",
            "type_id": type_id,
            "tag": tag
        }
        retrieve_task_type_tag_alias_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="GET",
            module="jobnavi",
            description="查询任务类型tag别名"
        )
        return retrieve_task_type_tag_alias_api(args)

    def create_task_type_tag_alias(self, type_id, tag, alias, description):
        args = {
            "operate": "create_tag_alias",
            "type_id": type_id,
            "tag": tag,
            "alias": alias,
            "description": description
        }
        create_task_type_tag_alias_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="POST",
            module="jobnavi",
            description="新增任务类型tag别名"
        )
        return create_task_type_tag_alias_api(args)

    def delete_task_type_tag_alias(self, type_id, tag, alias):
        args = {
            "operate": "delete_tag_alias",
            "type_id": type_id,
            "tag": tag,
            "alias": alias
        }
        delete_task_type_tag_alias_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="POST",
            module="jobnavi",
            description="删除任务类型tag别名"
        )
        return delete_task_type_tag_alias_api(args)

    def retrieve_task_type_default_tag(self, type_id, node_label):
        args = {
            "operate": "retrieve_default_tag",
            "type_id": type_id,
            "node_label": node_label
        }
        retrieve_task_type_default_tag_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="GET",
            module="jobnavi",
            description="查询任务类型默认tag"
        )
        return retrieve_task_type_default_tag_api(args)

    def create_task_type_default_tag(self, type_id, node_label, default_tag):
        args = {
            "operate": "create_default_tag",
            "type_id": type_id,
            "node_label": node_label,
            "default_tag": default_tag
        }
        create_task_type_default_tag_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="POST",
            module="jobnavi",
            description="新增任务类型默认tag"
        )
        return create_task_type_default_tag_api(args)

    def delete_task_type_default_tag(self, type_id, node_label):
        args = {
            "operate": "delete_default_tag",
            "type_id": type_id,
            "node_label": node_label
        }
        delete_task_type_default_tag_api = DataAPI(
            url=self.jobnavi_url + "/task_type",
            method="POST",
            module="jobnavi",
            description="删除任务类型默认tag"
        )
        return delete_task_type_default_tag_api(args)


JobNaviApi = _JobNaviApi
