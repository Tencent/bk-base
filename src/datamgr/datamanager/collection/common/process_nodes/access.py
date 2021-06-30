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
import logging

from api import access_api

from .base import ProcessNode, ProcessTemplate

logger = logging.getLogger()


class AccessCustomTemplate(ProcessTemplate):
    template = "access_of_common_custom.jinja"

    def get_bk_biz_id(self):
        return self.content["bk_biz_id"]

    def get_raw_data_name(self):
        return self.content["access_raw_data"]["raw_data_name"]

    def get_deploy_plan(self):
        return self.content


class AccessNode(ProcessNode):
    """
    数据接入节点
    """

    def build(self):
        bk_biz_id = self.params_template.get_bk_biz_id()
        raw_data_name = self.params_template.get_raw_data_name()
        deploy_plan = self.params_template.get_deploy_plan()

        raw_data_info = self.get_raw_data_info_by_name(bk_biz_id, raw_data_name)
        if raw_data_info is not None:
            raw_data_id = raw_data_info["id"]
            logger.info(
                f"[AccessNode] RawData({raw_data_name}) of business({bk_biz_id}) has exist: {raw_data_id}"
            )
            return {"raw_data_id": raw_data_id}

        response = access_api.deploy_plans.create(deploy_plan, raise_exception=True)
        return {"raw_data_id": response.data["raw_data_id"]}

    def get_raw_data_info_by_name(self, bk_biz_id, raw_data_name):
        """
        通过数据源名称查询详情
        """
        res = access_api.raw_datas.list(
            {"bk_biz_id": bk_biz_id, "raw_data_name__icontains": raw_data_name},
            raise_exception=True,
        )
        for d in res.data:
            if d["raw_data_name"] == raw_data_name:
                return d

        return None
