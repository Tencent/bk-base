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

from django.utils.translation import ugettext as _
from expiringdict import ExpiringDict

from dataflow.batch.exceptions.comp_execptions import InnerYarnServerException
from dataflow.batch.utils.http_util import http
from dataflow.batch.utils.yarn_util import YARN

context_app_id_map = ExpiringDict(max_len=1000, max_age_seconds=3600)


def __load_context_app_map(url):
    """
    加载YARN APPNAME -> APPID 的映射
    """
    global context_app_id_map
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    api_rtn = http(url, headers=headers)
    # 如果请求返回的是dict说明访问的是active RM，如果是str访问的是standby RM
    if isinstance(api_rtn, dict):
        for app in api_rtn["apps"]["app"]:
            context_app_id_map[app["name"]] = app
    elif isinstance(api_rtn, str):
        # 是str，从返回中拿到active的url
        url = api_rtn.split("RM:")[-1].strip()
        __load_context_app_map(url)
    else:
        raise InnerYarnServerException(_("http请求异常%s") % api_rtn)


# def get_appid_by_name(args):
#     """
#     基于Yarn的AppName获取AppID
#     """
#     yarn = YARN()
#     context_name = args['context_name']
#     if context_name not in context_app_id_map:
#         __load_context_app_map(yarn.config.get_rm_apps_urls())
#     app_info = context_app_id_map.get(context_name)
#     return app_info


def judge_yarn_avaliable(yarn_id):
    yarn = YARN(yarn_id)
    yarn_metrics = yarn.get_yarn_metrics()
    if not yarn_metrics:
        raise InnerYarnServerException(_("yarn metrics 异常%s") % yarn_metrics)
    cluster_metrics = yarn_metrics.get("clusterMetrics")
    if not cluster_metrics:
        raise InnerYarnServerException("Yarn is unavailable.")
    apps_pending = cluster_metrics["appsPending"]
    apps_running = cluster_metrics["appsRunning"]
    if apps_pending - apps_running > 100 or (apps_pending > 300 and apps_running < 50):
        raise InnerYarnServerException("Yarn is unavailable.")
    if apps_pending + apps_running > 800:
        raise InnerYarnServerException("Yarn is unavailable.")
    return True


# def yarn_speed_control():
#     """
#     流控函数,控制yarn同时运行作业数
#     """
#     yarn = YARN()
#     yarn_metrics = yarn.get_yarn_metrics()
#     if not yarn_metrics:
#         raise YarnException(_(u'yarn metrics 异常%s') % yarn_metrics, views.INNER_SERVER_EX)
#     cluster_metrics = yarn_metrics.get('clusterMetrics')
#     if not cluster_metrics:
#         return False
#     apps_pending = cluster_metrics['appsPending']
#     apps_running = cluster_metrics['appsRunning']
#     yarn_jobs = apps_pending + apps_running
#     if apps_pending - apps_running > 50 and apps_pending > 50:
#         return False
#     if yarn_jobs > 500:
#         return False
#
#     # """ 统计即将提交到yarn上的作业 """
#     # with db.open_cursor("azkaban") as c:
#     #     sql = """SELECT tb.flow_id FROM `active_executing_flows` as ta left join execution_flows as tb
#     #     on ta.exec_id = tb.exec_id  and tb.flow_id like "batch_sql_exec%%" and tb.flow_id is not null"""
#     #     db_rtn = db.list(c, sql)
#     #     if db_rtn and yarn_jobs + len(db_rtn) > 600:
#     #         return False
#     return True


# todo
def restart_resourcemanager():
    """重启Resourcemanager"""
    # execute_flow('resourcemanager_rstart', 'resourcemanager_rstart')


# def get_live_nodemanager():
#     yarn = YARN()
#     metrics = yarn.get_yarn_metrics()
#     return metrics.get('clusterMetrics')['totalNodes']
