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
from typing import List, Optional

from api.base import DataAPI
from api.utils import add_esb_common_params
from conf.settings import CMDB_API_URL

logger = logging.getLogger(__name__)


class CMDBApi(object):
    MODULE = "CMDB"

    def __init__(self):
        self.list_hosts_without_biz = DataAPI(
            url=CMDB_API_URL + "list_hosts_without_biz/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="可无业务信息的主机信息查询",
        )

        self.list_biz_hosts = DataAPI(
            url=CMDB_API_URL + "list_biz_hosts/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="指定业务下的主机信息查询",
        )

        self.resource_watch = DataAPI(
            url=CMDB_API_URL + "resource_watch/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="事件监听",
        )

        self.search_business = DataAPI(
            url=CMDB_API_URL + "search_business/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="查询业务",
        )

        self.search_object_attribute = DataAPI(
            url=CMDB_API_URL + "search_object_attribute/",
            method="GET",
            module="cmdb",
            before_request=add_esb_common_params,
            description="查询对象模型属性",
        )

        self.get_host_base_info = DataAPI(
            url=CMDB_API_URL + "get_host_base_info",
            method="GET",
            module="cmdb",
            before_request=add_esb_common_params,
            description="获取主机基础信息详情",
        )

        self.search_set = DataAPI(
            url=CMDB_API_URL + "search_set/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="查询业务下集群详情",
        )

        self.search_module = DataAPI(
            url=CMDB_API_URL + "search_module/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="查询业务下模块详情",
        )

        self.list_biz_hosts_topo = DataAPI(
            url=CMDB_API_URL + "list_biz_hosts_topo/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="查询业务下的主机拓扑信息",
        )

        self.find_host_topo_relation = DataAPI(
            url=CMDB_API_URL + "find_host_topo_relation/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="查询业务下的主机与拓扑的关系(id)",
        )

        self.search_biz_inst_topo = DataAPI(
            url=CMDB_API_URL + "search_biz_inst_topo",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="查询业务实例拓扑",
        )

        self.find_host_biz_relation = DataAPI(
            url=CMDB_API_URL + "find_host_biz_relations/",
            method="POST",
            module="cmdb",
            before_request=add_esb_common_params,
            description="根据主机ID查询业务拓扑相关关系(id)",
        )

    def get_all_business_list(self, fields=None, raise_exception=False):
        """
        获取全部业务信息列表
        """
        params = {"fields": fields} if fields else {}
        r = self.search_business(params, raise_exception=raise_exception)
        return r.data["info"]

    def get_all_business_ids(self, raise_exception=False):
        """
        获取全部业务列表
        """
        business_info_list = self.get_all_business_list(
            fields=["bk_biz_id"], raise_exception=raise_exception
        )
        return [business_info["bk_biz_id"] for business_info in business_info_list]

    def get_object_fields_list(self, bk_obj_id, raise_exception=False):
        """
        获取对象下全部字段列表
        :param: bk_obj_id , str 可选 host/module/set
        """
        object_attr_list = self.search_object_attribute(
            params={"bk_obj_id": bk_obj_id}, raise_exception=raise_exception
        )
        object_fields_list = [
            object_attr["bk_property_id"] for object_attr in object_attr_list.data
        ]
        return object_fields_list

    def get_all_hosts_without_biz(self, params, raise_exception=False):
        """
        获取不限业务的全部 主机列表
        """
        return self.batch_load_by_page(
            func=self.list_hosts_without_biz,
            params=params,
            raise_exception=raise_exception,
        )

    def get_all_hosts_with_biz(self, bk_biz_id, fields=None, raise_exception=False):
        """
        获取指定业务下的全部 主机列表
        """
        params = {"bk_biz_id": bk_biz_id}
        if fields:
            params["fields"] = fields

        return self.batch_load_by_page(
            func=self.list_biz_hosts, params=params, raise_exception=raise_exception
        )

    def get_all_hosts_relations_in_biz(self, params, raise_exception=False):
        """
        自动分页查询业务下的全部主机拓扑信息
        """
        return self.batch_load_by_page(
            func=self.find_host_topo_relation,
            params=params,
            page_limit=200,
            data_key="data",
            raise_exception=raise_exception,
        )

    @staticmethod
    def batch_load_by_page(
        func,
        params: List,
        page_limit: int = 500,
        data_key: str = "info",
        retry_count: int = 5,
        raise_exception: bool = False,
    ) -> Optional[List]:
        """
        批量进行分页查询并merge, 可选对查询中的 count 进行检查，
        当查询过程中总 count 有变化则重新由第一页遍历(以避免数据源头变更导致的分页窗口偏移)
        :param func: 请求方法
        :param params: 原请求参数
        :param page_limit: 单页限制条数
        :param data_key: 获取实际数据的字段名
        :param retry_count: 重试次数
        :param raise_exception: 请求失败是否抛出异常
        :return: 拼接后的数据列表，当请求失败时，返回 None
        """
        start = 0
        info_list = []
        while 1:
            params["page"] = {"start": start, "limit": page_limit}
            response = func(
                params, raise_exception=raise_exception, retry_times=retry_count
            )
            if not response.is_success():
                return None

            data = response.data[data_key]

            # 当加载到的数据为空列表时，则表示已经拉取完所有数据了
            if len(data) == 0:
                break

            info_list.extend(data)

            # 下一页
            start += page_limit

        return info_list
