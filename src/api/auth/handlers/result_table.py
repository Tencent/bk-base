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


from auth.api import MetaApi
from auth.exceptions import ObjectNotExistsErr
from cached_property import cached_property


class ResultTableHandler:
    """
    将对 RT 一些常用操作集合起来，统一维护
    """

    def __init__(self, result_table_id):
        self.result_table_id = result_table_id

        # 绑定 AuthResource 资源对象，后续期望数据均从 AuthResource 获取
        self.resource = None

        # 存储信息
        self.storages = None

    def set_storage(self, storages):
        """
        设置存储信息

        @paramExample storages
            {
                'hdfs': {},
                'tspider': {},
                'druid': {},
                'kafka': {},
                'mysql': {},
                'hermes': {},
                'es': {}
            }
        """
        self.storages = storages

    @classmethod
    def list_by_rt_ids_with_storages(cls, result_table_ids):
        """
        批量检索结果表信息，附带关联的存储信息
        """
        data = MetaApi.list_result_table(
            dict(result_table_ids=result_table_ids, related=["storages"], need_storage_detail=0)
        ).data
        objs = []
        for d in data:
            obj = ResultTableHandler(d["result_table_id"])
            obj.set_storage(d["storages"])
            objs.append(obj)

        return objs

    def has_queue(self):
        """
        是否有队列存储
        """
        return "queue" in self.storages or "queue_pulsar" in self.storages

    @property
    def geo_tags(self):
        """
        获取地域标签
        """
        return [tag["code"] for tag in self.basic_info.get("tags", {}).get("manage", {}).get("geog_area", [])]

    @cached_property
    def basic_info(self):
        """
        获取基项目信息
        """
        basic_info = MetaApi.get_result_table_info({"result_table_id": self.result_table_id}).data

        if basic_info is None:
            raise ObjectNotExistsErr("RT NotExist: {}".foramt(self.result_table_id))

        return basic_info

    def __str__(self):
        return "[ResultTableHandler id={}, resource={}, storages={}]".format(
            self.result_table_id, self.resource, self.storages
        )
