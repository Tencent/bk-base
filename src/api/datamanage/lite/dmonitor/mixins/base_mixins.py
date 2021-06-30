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


import datetime
import json

import gevent
from datamanage.lite.dmonitor.constants import ALERT_CODES, ALERT_LEVELS, ALERT_TYPES
from datamanage.lite.dmonitor.models import AlertShield
from datamanage.utils.api import CCApi, DataflowApi, MetaApi
from datamanage.utils.time_tools import tznow
from django.utils.translation import ugettext_lazy as _

from common.api import AuthApi
from common.auth.exceptions import PermissionDeniedError
from common.decorators import trace_gevent
from common.local import get_request_username
from common.log import logger


class BaseMixin(object):
    MAX_COUNT = 200000

    def get_table_records(self, table_name, fields="*", max_record_count=5000, update_duration=0):
        sqls = []
        if update_duration:
            update_time = tznow() - datetime.timedelta(seconds=update_duration)
            sqls.append(
                """
                SELECT {fields} FROM {table_name} WHERE updated_at >= '{update_time}' OR created_at >= '{update_time}'
            """.format(
                    fields=fields if isinstance(fields, str) else ",".join(fields),
                    table_name=table_name,
                    update_time=update_time.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
        else:
            try:
                res = MetaApi.complex_search(
                    {
                        "statement": "SELECT count(*) as total_count FROM {table_name}".format(table_name=table_name),
                        "backend": "mysql",
                    },
                    raise_exception=True,
                )

                total_count = res.data[0].get("total_count", self.MAX_COUNT)
            except Exception as e:
                logger.error(_("Complex search meta data error: {error}").format(error=e))
                total_count = max_record_count

            index = 0
            query_step_count = 5000
            while index < total_count:
                sqls.append(
                    """
                    SELECT {fields} FROM {table_name} limit {index}, {step}
                """.format(
                        fields=fields if isinstance(fields, str) else ",".join(fields),
                        table_name=table_name,
                        index=index,
                        step=query_step_count,
                    )
                )
                index += query_step_count

        return self.multiple_complex_search(sqls)

    def multiple_complex_search(self, sqls):
        result_list = []
        gevent_tasks = []

        def multiple_complex_search_task(sql, result):
            try:
                res = MetaApi.complex_search(
                    {
                        "statement": sql,
                        "backend": "mysql",
                    }
                )
                result.extend(res.data or [])
            except Exception as e:
                logger.error("查询元数据Complex Search接口失败, ERROR: %s" % e)

        for sql in sqls:
            gevent_tasks.append(gevent.spawn(multiple_complex_search_task, sql, result_list))

        gevent.joinall(gevent_tasks)
        return result_list

    @trace_gevent()
    def fetch_dataflow_infos(self, dataflow_infos, dataflow_ids=None, bk_username=None):
        bk_username = bk_username or get_request_username()
        if len(dataflow_ids) == 0:
            return
        try:
            if dataflow_ids is not None:
                res = DataflowApi.flows.list({"flow_id": list(dataflow_ids), "bk_username": bk_username})
            else:
                res = DataflowApi.flows.list({"bk_username": bk_username})

            if res.is_success():
                for dataflow in res.data:
                    flow_id = str(dataflow.get("flow_id"))
                    dataflow_infos[flow_id] = dataflow
        except Exception as e:
            logger.error("无法获取告警对象的flow信息, ERROR: %s" % e)

    def fetch_dataflow_multiprocess(self, dataflow_infos, dataflow_ids=None, bk_username=None):
        from gevent import monkey

        monkey.patch_all()

        dataflow_ids = list(dataflow_ids)
        gevent_tasks = []
        for i in range(len(dataflow_ids) // 100 + 1):
            gevent_tasks.append(
                gevent.spawn(
                    self.fetch_dataflow_infos,
                    dataflow_infos,
                    dataflow_ids[i * 100 : (i + 1) * 100],
                    bk_username,
                )
            )
        gevent.joinall(gevent_tasks)

    @trace_gevent()
    def fetch_rawdata_infos(self, raw_data_infos, raw_data_ids=None):
        try:
            if raw_data_ids is not None:
                sql = """
                    SELECT id, raw_data_name, raw_data_alias, bk_biz_id FROM access_raw_data
                    WHERE id in ({rawdata_list})
                """.format(
                    rawdata_list=",".join(list(raw_data_ids))
                )
            else:
                sql = """
                    SELECT id, raw_data_name, raw_data_alias, bk_biz_id FROM access_raw_data
                """
            complex_search_result = self.multiple_complex_search([sql])
            for item in complex_search_result:
                id = str(item.get("id"))
                raw_data_infos[id] = item
        except Exception as e:
            logger.error("无法获取告警对象rawdata信息, ERROR: %s" % e)

    @trace_gevent()
    def fetch_biz_infos(self, biz_infos, biz_ids=None):
        try:
            res = CCApi.get_app_list()
            if res.is_success():
                for item in res.data:
                    bk_biz_id = int(item.get("ApplicationID"))
                    if biz_ids is None or bk_biz_id in biz_ids:
                        biz_infos[bk_biz_id] = {
                            "bk_biz_id": item.get("ApplicationID"),
                            "bk_biz_name": item.get("ApplicationName"),
                            "maintainers": item.get("Maintainers"),
                            "description": item.get("AppSummary"),
                        }
        except Exception as e:
            logger.error("获取业务信息失败, ERROR: %s" % e)

    def fetch_project_infos(self, project_infos, project_ids=None):
        try:
            if project_ids is not None:
                res = MetaApi.projects.list({"project_ids": project_ids})
            else:
                res = MetaApi.projects.list()

            if res.is_success():
                for item in res.data:
                    project_id = item.get("project_id")
                    project_infos[project_id] = item
        except Exception as e:
            logger.error("获取项目信息失败, ERROR: %s" % e)

    @trace_gevent()
    def fetch_alert_shields(self, alert_shields, shield_time=None):
        try:
            shield_time = shield_time or tznow().strftime("%Y-%m-%d %H:%M:%S")
            new_alert_shields = list(
                AlertShield.objects.filter(active=True, start_time__lte=shield_time, end_time__gte=shield_time).values()
            )
            for item in new_alert_shields:
                item["dimensions"] = json.loads(new_alert_shields)
            alert_shields.extend(new_alert_shields)
        except Exception as e:
            logger.error("获取告警屏蔽信息失败, ERROR: %s" % e)

    def get_bizs_by_username(self, username):
        try:
            bk_bizs = AuthApi.list_user_scope_dimensions(
                {
                    "action_id": "raw_data.update",
                    "dimension": "bk_biz_id",
                    "bk_username": username,
                }
            ).data
            return bk_bizs
        except Exception as e:
            logger.error("获取用户有权限的业务列表失败: %s" % e)
            return []

    def get_projects_by_username(self, username):
        try:
            projects = AuthApi.list_user_perm_scopes(
                {
                    "user_id": username,
                    "show_display": True,
                    "action_id": "project.manage_flow",
                }
            ).data
            return projects
        except Exception as e:
            logger.error("获取用户有权限的项目列表失败: %s" % e)
            return []

    def check_permission(self, action_id, object_id, bk_username):
        res = AuthApi.check_user_perm(
            {
                "user_id": get_request_username(),
                "action_id": action_id,
                "object_id": object_id,
            },
            raise_exception=True,
        )
        if res.data is False:
            raise PermissionDeniedError()

    def check_batch_permissions(self, action_id, object_ids, bk_username):
        res = AuthApi.batch_check(
            {
                "permissions": list(
                    map(
                        lambda object_id: {
                            "object_id": object_id,
                            "user_id": bk_username,
                            "action_id": action_id,
                        },
                        object_ids,
                    )
                )
            },
            raise_exception=True,
        )
        for item in res.data:
            if item.get("result") is False:
                raise PermissionDeniedError(_("权限不足({})").format(item.get("object_id")))

    def check_dimension_match(self, dimension_conditions, item_dimensions):
        for key, value in list(dimension_conditions.items()):
            if key in item_dimensions:
                if isinstance(value, list):
                    in_candidate = False
                    for candidate_value in value:
                        if str(item_dimensions[key]) == (candidate_value):
                            in_candidate = True

                    if in_candidate:
                        continue
                else:
                    if str(item_dimensions[key]) == str(value):
                        continue
                return False
            else:
                return False
        return True

    def summary_alerts(self, alert_list, group=None):
        response = self.get_default_summary()
        if group:
            response["groups"] = {}

        for alert_detail in alert_list:
            alert_code = alert_detail["alert_code"]
            alert_type = alert_detail["alert_type"]
            alert_level = alert_detail["alert_level"]

            self.statistics_alert(response, alert_code, alert_type, alert_level)
            if group and (group in alert_detail or group in alert_detail["dimensions"]):
                group_value = alert_detail.get(group, alert_detail["dimensions"].get(group, ""))
                if group_value not in response["groups"]:
                    response["groups"][group_value] = self.get_default_summary()
                self.statistics_alert(response["groups"][group_value], alert_code, alert_type, alert_level)

        return response

    def get_default_summary(self):
        template = {
            "alert_count": 0,
            "alert_codes": {},
            "alert_levels": {},
            "alert_types": {},
        }
        for alert_code in ALERT_CODES:
            template["alert_codes"][alert_code] = 0
        for alert_type in ALERT_TYPES:
            template["alert_types"][alert_type] = 0
        for alert_level in ALERT_LEVELS:
            template["alert_levels"][alert_level] = 0
        return template

    def statistics_alert(self, summary, alert_code, alert_type, alert_level):
        summary["alert_count"] += 1
        if alert_code in summary["alert_codes"]:
            summary["alert_codes"][alert_code] += 1
        if alert_type in summary["alert_types"]:
            summary["alert_types"][alert_type] += 1
        if alert_level in summary["alert_levels"]:
            summary["alert_levels"][alert_level] += 1
