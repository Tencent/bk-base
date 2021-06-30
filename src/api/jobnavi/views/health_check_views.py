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

from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.views import APIViewSet
from jobnavi.config import jobnavi_config
from jobnavi.api.jobnavi_api import JobNaviApi


class HealthCheckViewSet(APIViewSet):
    """
    @apiDefine health_check
    集群健康检查API
    """

    def healthz(self, request):
        """
        @api {get} /jobnavi/healthz 检测jobnavi集群是否健康
        @apiName healthz
        @apiGroup health_check
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        healthz = {}
        status = True
        message = {}
        for cluster_config in jobnavi_config.get_jobnavi_config():
            geog_area_code = cluster_config[0]
            cluster_name = cluster_config[1]
            message[cluster_name] = {
                "scheduler": {"message": "", "status": True},
                "runner": {"message": "", "status": True},
            }

            jobnavi = JobNaviApi(geog_area_code, cluster_name)
            res = jobnavi.healthz()
            if not res or not res.is_success():
                status = False
                message[cluster_name]["scheduler"]["status"] = False
                message[cluster_name]["scheduler"]["message"] = _("scheduler检查异常")
                message[cluster_name]["runner"]["status"] = False
            else:
                res = jobnavi.query_current_runners()
                if not res or not res.is_success():
                    message[cluster_name]["runner"]["status"] = False
                    message[cluster_name]["runner"]["message"] = _("获取runner信息异常")
                else:
                    runner_amount = len(list(json.loads(res.data).keys()))
                    if runner_amount <= 1:
                        message[cluster_name]["runner"]["status"] = False
                        message[cluster_name]["runner"]["message"] = _("当前可用的runner数小于2个，存活runner列表: %s") % (
                            list(json.loads(res.data).keys())
                        )
            healthz["status"] = status
            healthz["message"] = message
        return Response(healthz)
