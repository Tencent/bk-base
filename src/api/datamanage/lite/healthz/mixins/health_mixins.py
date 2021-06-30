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


import os
from django.utils.translation import ugettext as _

from common.log import logger
from pizza.settings import PIZZA_ROOT, APP_NAME

from datamanage.utils.dbtools.influx_util import influx_query
from datamanage.utils.dbtools.redis_util import redis_ltrim


class HealthMixin(object):
    """Mixin to check dmonitor"""

    def datamanager_health_check(self, request):
        result = True
        detail = {}

        # 检查datamanager master
        try:
            detail['data-monitor'] = self.check_master()
        except Exception as e:
            result = False
            detail['data-monitor'] = {
                "status": False,
                "version": "",
                "message": _("检查DataManager模块Master健康状态失败，异常信息：%s") % str(e),
            }

        return result, detail

    def get_version(self):
        version_file = os.path.join(PIZZA_ROOT, APP_NAME, "VERSION")
        try:
            with open(version_file, 'r') as f:
                version = f.read()
        except Exception:
            version = ""
        version = version.replace("\n", "")
        version = version.replace(" ", "")
        version = version.replace("\r", "")
        return version

    def check_master(self):
        result = {"status": True, "message": "", "version": ""}
        # 查询influxdb中master的埋点信息
        sql = "select * from monitor_dmonitor_master " "where time > now() - 5m order by time desc limit 1"
        database = 'monitor_custom_metrics'
        try:
            result_set = influx_query(sql, database)
            if not result_set:
                result['status'] = False
                result['message'] = _("Master超过5分钟没有打点")
            else:
                try:
                    info = list(result_set.get_points())
                except Exception as e:
                    info = []
                    logger.warning(f'{str(e)}')

                if not info or len(info) < 1:
                    result['status'] = False
                    result['message'] = _("Master超过5分钟没有打点")
                else:
                    master_info = info[0]
                    result['detail'] = master_info
                    result['version'] = master_info.get("version", "")
                    if master_info.get('worker_cnt', 0) < 3:
                        result['status'] = False
                        result['message'] = _("当前运行中的Worker数量小于3个")
                    if master_info.get('task_mq_len', 0) > 50:
                        result['status'] = False
                        result['message'] = _("当前Task队列大于50，请检查Worker是否运行异常")
                    if master_info.get('task_mq_len', 0) > 500:
                        redis_ltrim('dmonitor_mq_tasks', 0, 1)
                        result['status'] = False
                        result['message'] = _("当前Task队列大于500，请检查Worker是否运行异常，清空队列")
                    if master_info.get('storage_mq_len', 0) > 50:
                        result['status'] = False
                        result['message'] = _("当前存储队列大于50，请检查Puller是否运行异常")
                    if master_info.get('storage_mq_len', 0) > 500:
                        redis_ltrim('dmonitor_mq_storage', 0, 1)
                        result['status'] = False
                        result['message'] = _("当前Task队列大于500，请检查Worker是否运行异常，清空队列")
                    if master_info.get('worker_mq_len', 0) > 50:
                        result['status'] = False
                        result['message'] = _("当前启动Worker队列大于50，" "请检查Master是否运行异常")
                    if master_info.get('worker_mq_len', 0) > 500:
                        redis_ltrim('dmonitor_mq_start_worker', 0, 1)
                        result['status'] = False
                        result['message'] = _("当前启动Worker队列大于500，" "请检查Master是否运行异常，清空队列")
        except Exception as e:
            result['status'] = False
            result['version'] = ""
            result['message'] = _("查询DataManager模块Master状态失败，异常信息：%s") % e
        return result
