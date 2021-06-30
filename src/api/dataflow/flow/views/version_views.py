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

from common.auth import perm_check
from common.decorators import list_route
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.flow.handlers.flow import FlowHandler


class VersionViewSet(APIViewSet):
    lookup_field = "version_id"
    lookup_value_regex = r"[\s\S]*"

    @list_route(methods=["get"], url_path="draft")
    @perm_check("flow.retrieve", detail=True, url_key="flow_id")
    def get_draft(self, request, flow_id):
        """
        @api {get}  /dataflow/flow/flows/:fid/versions/draft/ 获取指定 flow 草稿信息
        @apiName get_draft
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
                {
                    "version": "V20191121192756#giJUOi9Siql8PmhFo6tQ",
                    "lines": [
                        {
                            "to_node_id": 1111,
                            "from_node_id": 222,
                            "frontend_info": {
                                "source": {
                                    "node_id": 16929,
                                    "id": "ch_16929",
                                    "arrow": "Right"
                                },
                                "target": {
                                    "id": "ch_16930",
                                    "arrow": "Left"
                                }
                            }
                        },
                        {
                            "to_node_id": 1111,
                            "from_node_id": 222,
                            "frontend_info": {
                                "source": {
                                    "node_id": 16931,
                                    "id": "ch_16931",
                                    "arrow": "Right"
                                },
                                "target": {
                                    "id": "ch_16932",
                                    "arrow": "Left"
                                }
                            }
                        }
                    ],
                    "locations": [
                        {
                            "status": "running",
                            "node_id": 16929,
                            "id": "ch_16929",
                            "node_type": "tdw_source",
                            "node_name": "ss_tdw[tdw_12497_591_tdw_tdtw]",
                            "flow_id": "2707",
                            "frontend_info": {
                                "y": 258,
                                "x": 477
                            },
                            "related": false,
                            "custom_calculate_status" "running",
                            "result_table_ids"：[],
                            "bk_biz_id": 1,
                            "node_config": {
                                "fixed_deplay": 0,          # 前端需要基于这些值确定新节点的展示情况
                                "count_freq": 1,           # 前端需要基于这些值确定新节点的展示情况
                                "schedule_period": "hour"  # 前端需要基于这些值确定新节点的展示情况
                            }
                        }
                    ]
                }
        """
        flow = FlowHandler(flow_id=flow_id)
        return Response(flow.get_draft())
