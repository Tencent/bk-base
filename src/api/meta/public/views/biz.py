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


from common.exceptions import ApiRequestError, ApiResultError
from rest_framework.response import Response

from meta.basic.common import RPCViewSet


class BizViewSet(RPCViewSet):
    lookup_field = "bk_biz_id"

    def retrieve(self, request, bk_biz_id):
        """
        @api {get} /meta/bizs/:bk_biz_id/ 获取单个biz的信息
        @apiVersion 0.2.0
        @apiGroup BKB
        @apiName retrieve_biz

        @apiParam {Integer} [bk_biz_id] 业务id

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK 0
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "bk_biz_id": "591",
                    "maintainers": "maintainer1;maintainer2",
                    "bk_biz_name": "蓝鲸基础计算平台",
                    "description": "some description"
                },
                "result": true
            }
        """

        try:
            res = None
            statement = """{
                get_node(func: eq(BKBiz.id, %s)) {
                    uid
                    BKBiz.id
                    BKBiz.name
                    BKBiz.maintainer
                    BKBiz.biz_type
                    BKBiz.biz @filter(has(BizV1.ApplicationID)) {
                        uid
                        BizV1.AppSummary
                    }
                }
            }"""
            statement = statement % bk_biz_id
            ret = self.entity_complex_search(statement, backend_type="dgraph")
            result = ret.result
            if result["data"]:
                res = result["data"]

            if res is not None and len(res.get("get_node", [])) > 0:
                item = res.get("get_node")[0]
                res_content = {
                    "bk_biz_id": str(item.get("BKBiz.id")),
                    "bk_biz_name": item.get("BKBiz.name"),
                    "maintainers": item.get("BKBiz.maintainer"),
                    "description": None,
                }
                # 如果是biz是biz v1的节点,获取AppSummary作为description
                if item.get("BKBiz.biz_type") == "v1":
                    if "BKBiz.biz" in item and len(item.get("BKBiz.biz")) > 0:
                        res_content["description"] = item.get("BKBiz.biz")[0].get("BizV1.AppSummary", None)

                return Response(res_content)
            else:
                raise ApiResultError("调用接口查询业务{}信息失败: {}".format(bk_biz_id, "无效的业务id"))
        except ApiRequestError as e:
            raise ApiResultError("调用接口查询业务{}信息失败: {}".format(bk_biz_id, e.message))

    def list(self, request):
        """
        @api {get} /meta/bizs/ 获取biz信息列表
        @apiVersion 0.2.0
        @apiGroup BKB
        @apiName list_biz

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK 0
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "bk_biz_id": "1",
                        "maintainers": "maintainer1;maintainer2",
                        "bk_biz_name": "部门资源池",
                        "description": "部门资源池"
                    },
                    ......
                ],
                "result": true
            }
        """
        try:
            res = None
            statement = """{
                get_list(func: has(BKBiz.id)) {
                    uid
                    BKBiz.id
                    BKBiz.name
                    BKBiz.maintainer
                    BKBiz.biz_type
                    BKBiz.biz @filter(has(BizV1.ApplicationID)) {
                        uid
                        BizV1.AppSummary
                    }
                }
            }"""

            ret = self.entity_complex_search(statement, backend_type="dgraph")
            result = ret.result
            if "data" in result:
                res = result["data"]
            if res is not None:
                res_content = []
                for item in res.get("get_list", []):
                    biz_name = item.get("BKBiz.name", None)
                    if not biz_name:
                        continue
                    tmp_content = {
                        "bk_biz_id": str(item.get("BKBiz.id")),
                        "bk_biz_name": biz_name,
                        "maintainers": item.get("BKBiz.maintainer"),
                        "description": None,
                    }
                    # 如果是biz是biz v1的节点,获取AppSummary作为description
                    if item.get("BKBiz.biz_type") == "v1":
                        if "BKBiz.biz" in item and len(item.get("BKBiz.biz")) > 0:
                            tmp_content["description"] = item.get("BKBiz.biz")[0].get("BizV1.AppSummary", None)
                    res_content.append(tmp_content)

                res_content.sort(key=lambda unit: unit["bk_biz_id"])
                return Response(res_content)
            else:
                raise ApiResultError("调用接口查询业务列表失败: %s" % ret)
        except ApiRequestError as e:
            raise ApiRequestError("调用接口查询业务列表失败: %s" % e.message)
