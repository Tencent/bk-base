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

from django.utils.translation import ugettext_noop

from ...collectors.utils.language import Bilingual
from ...handlers.raw_data import RawDataHandler
from ...models import AccessRawDataTask, AccessTask
from ...serializers import BaseSerializer
from ..base_collector import BaseAccess, BaseAccessTask


class CustomAccessTask(BaseAccessTask):
    def deploy(self):
        """
        自定义接入，无需部署操作
        """
        pass


class CustomAccess(BaseAccess):
    """
    自定义接入
    """

    data_scenario = "custom"
    access_task = CustomAccessTask
    access_serializer = BaseSerializer
    task_disable = True

    def create_after(self):
        # 创建执行记录
        task_id = BaseAccessTask.create(
            self.access_param["bk_biz_id"],
            self.access_param["bk_username"],
            self.data_scenario,
            {},
            AccessTask.ACTION.DEPLOY,
        )
        AccessRawDataTask.objects.create(
            task_id=task_id,
            raw_data_id=self.access_param["raw_data_id"],
            created_by=self.access_param["bk_username"],
            updated_by=self.access_param["bk_username"],
            description="",
        )
        task = BaseAccessTask(task_id=task_id)
        task.log(msg=Bilingual(ugettext_noop(u"成功接入自定义数据源")), level="INFO", task_log=True)

    def save_access_info(self, is_update=False):
        """
        没有scope, 只更新description
        :param is_update: 是否是更新操作
        """
        param = self.access_param
        raw_data_id = param["raw_data_id"]
        # 有则更新, 没有则创建
        objs = self.access_model.objects.filter(raw_data_id=raw_data_id)
        if objs:
            # 自定义只有1个resource_info
            objs[0].updated_by = param["bk_username"]
            objs[0].description = param.get("description")
            objs[0].save()
        else:
            self.access_model.objects.create(
                raw_data_id=raw_data_id,
                created_by=param["bk_username"],
                updated_by=param["bk_username"],
                description=param.get("description"),
            )

    def generate_access_param_by_data_id(self, raw_data_id):
        """
        获取接入信息
        :param raw_data_id: data id
        :return:
        """
        if not self.rawdata:
            self.rawdata = RawDataHandler(raw_data_id=raw_data_id)
        access_raw_data = self.rawdata.retrieve()

        return {
            "bk_app_code": access_raw_data["bk_app_code"],
            "bk_biz_id": access_raw_data["bk_biz_id"],
            "description": "",
            "data_scenario": access_raw_data["data_scenario"],
            "access_raw_data": access_raw_data,
            "access_conf_info": {},
        }
