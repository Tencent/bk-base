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
from conf.settings import USERMANAGE_API_URL

logger = logging.getLogger(__name__)


class UsermanageApi(object):
    MODULE = "usermanage"

    def __init__(self):
        self.list_users = DataAPI(
            url=USERMANAGE_API_URL + "list_users/",
            method="GET",
            module="usermanage",
            before_request=add_esb_common_params,
            description="用户管理相关操作接口集",
        )

    def generate_all_user_list(self, params, raise_exception=False):
        """
        自动分页查询业务下的全部主机拓扑信息
        """
        return self.yield_load_by_page(
            func=self.list_users,
            params=params,
            page_limit=200,
            data_key="results",
            raise_exception=raise_exception,
        )

    @staticmethod
    def yield_load_by_page(
        func,
        params: dict,
        page_limit: int = 500,
        data_key: str = "results",
        retry_count: int = 5,
        raise_exception: bool = False,
    ) -> Optional[List]:
        """
        分页查询 生成器
        :param func: 请求方法
        :param params: 原请求参数
        :param page_limit: 单页限制条数
        :param data_key: 获取实际数据的字段名
        :param retry_count: 重试次数
        :param raise_exception: 请求失败是否抛出异常
        :yield: 查询结果的数据列表
        """
        page = 1
        total_page = 1
        while page <= total_page:
            params.update({"page": page, "page_size": page_limit})
            response = func(
                params, raise_exception=raise_exception, retry_times=retry_count
            )
            logger.info(response.data)
            if not response.is_success():
                logger.warning(response.message)
                break

            data = response.data[data_key]

            # 当加载到的数据为空列表时，则表示已经拉取完所有数据了
            if len(data) == 0:
                break
            count = response.data["count"]
            total_page = int(count / page_limit) + 1
            # 下一页
            page += 1
            yield data

    @staticmethod
    def batch_load_by_page(
        func,
        params: dict,
        page_limit: int = 500,
        data_key: str = "results",
        retry_count: int = 5,
        raise_exception: bool = False,
    ) -> Optional[List]:
        """
        批量获取分页查询结果
        :param func: 请求方法
        :param params: 原请求参数
        :param page_limit: 单页限制条数
        :param data_key: 获取实际数据的字段名
        :param retry_count: 重试次数
        :param raise_exception: 请求失败是否抛出异常
        :return: list 全部查询结果的数据列表
        """
        batch_content_list = []
        for page_list in UsermanageApi.yield_load_by_page(
            func, params, page_limit, data_key, retry_count, raise_exception
        ):
            batch_content_list.extend(page_list)
        return batch_content_list
