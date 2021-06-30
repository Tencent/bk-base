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


from datamanage.lite.dmonitor.mixins.dmonitor_mixins import DmonitorMixin
from datamanage.lite.dmonitor.serializers import ResultTableListSerializer
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.views import APIViewSet


class DmonitorResultTableViewSet(DmonitorMixin, APIViewSet):
    @list_route(methods=["get"], url_path="status")
    @params_valid(serializer=ResultTableListSerializer)
    def rt_status(self, request, params):
        """
        @api {get} /datamanage/dmonitor/result_tables/status/ 查询结果表监控状态
        @apiVersion 1.0.0
        @apiGroup DmonitorResultTables
        @apiName rt_status

        @apiParam {string[]} result_table_ids 结果表列表

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "clean_and_realtime_table": {
                        "alerts": [
                            {
                                "start_time": "2018-10-29 00:00:00",
                                "msg": "alert!"
                            }
                        ],
                        "status": {
                            "input_cnt_10min": 100,
                            "input_cnt": 10,
                            "output_cnt": 50,
                            "drop_cnt": 1
                            "stat_time": "2018-10-29 00:00:00"
                        }
                    },
                    "offline_table": {
                        "alerts": [
                            {
                                "start_time": "2018-10-29 00:00:00",
                                "msg": "alert!"
                            }
                        ],
                        "status": {
                            "interval": 3600,
                            "execute_history": [
                                {
                                    "status": job_status_code,
                                    "execute_list": [{
                                        "status": "xx",
                                        "status_str": "xxx",
                                        "exec_id": "",
                                        "end_time": "2018-11-11 11:11:11",
                                        "flow_id": "batch_sql_{result_table_id}",
                                        "start_time": "2018-11-11 11:11:11",
                                        "err_msg":"",
                                        "err_code":"0"
                                    }],
                                    "period_start_time": "2018-11-11 11:11:11",
                                    "period_end_time": "2018-11-11 11:11:11",
                                    "status_str": "xxx"
                                }
                            ]
                        }
                    }
                }
            }
        """
        result = self.query_rt_status(params["result_table_ids"])

        return Response(result)
