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


from math import ceil
import copy
from concurrent.futures import ThreadPoolExecutor, as_completed

from datamanage.utils.api.datamanage import DatamanageApi


def dataset_filter(params, request):
    bk_biz_id = params.get('bk_biz_id', None)
    project_id = params.get('project_id', None)
    tag_ids = params.get('tag_ids', [])
    keyword = params.get('keyword', "")
    # 高级搜索参数
    storage_type = params.get('storage_type')
    created_by = params.get('created_by')
    created_at_start = params.get('created_at_start')
    created_at_end = params.get('created_at_end')
    range_operate = params.get('range_operate')
    range_score = params.get('range_score')
    heat_operate = params.get('heat_operate')
    heat_score = params.get('heat_score')
    importance_operate = params.get('importance_operate')
    importance_score = params.get('importance_score')
    asset_value_operate = params.get('asset_value_operate')
    asset_value_score = params.get('asset_value_score')
    assetvalue_to_cost_operate = params.get('assetvalue_to_cost_operate')
    assetvalue_to_cost = params.get('assetvalue_to_cost')
    storage_capacity_operate = params.get('storage_capacity_operate')
    storage_capacity = params.get('storage_capacity')
    cal_type = params.get('cal_type')

    has_filter_cond = True
    # 如果没有过滤条件
    if (
        (not bk_biz_id)
        and (not project_id)
        and (not tag_ids)
        and (not keyword)
        and (not storage_type)
        and (not created_by)
        and (not created_at_start)
        and (not created_at_end)
        and (not range_operate)
        and (not range_score)
        and (not heat_operate)
        and (not heat_score)
        and (not importance_operate)
        and (not importance_score)
        and (not asset_value_operate)
        and (not asset_value_score)
        and (not assetvalue_to_cost_operate)
        and (not assetvalue_to_cost)
        and (not storage_capacity_operate)
        and (not storage_capacity)
        and len(cal_type) == 1
    ):
        has_filter_cond = False
        return has_filter_cond, {}

    # 首先拿到符合搜索条件的数据有几个
    count = DatamanageApi.get_data_dict_count_product(params).data.get('count', 0)

    # 然后根据count的多少并发请求接口
    filter_dataset_dict = {}
    list_size = 5000.0
    future_tasks = []

    def async_add_dataset(params, request):
        """
        并发查满足条件的数据集列表
        :param item:dict
        :return:
        """
        # 激活除了主线程以外的request
        from common.local import activate_request

        activate_request(request)
        dataset_list = DatamanageApi.get_data_dict_list_product(params).data
        if dataset_list:
            return dataset_list
        else:
            return []

    with ThreadPoolExecutor(max_workers=5) as ex:
        for i in range(int(ceil(count / list_size))):
            params_dict = copy.deepcopy(params)
            params_dict['page'] = i + 1
            params_dict['page_size'] = 5000
            future_tasks.append(ex.submit(async_add_dataset, params_dict, request))

        for future in as_completed(future_tasks):
            for each_dataset in future.result():
                filter_dataset_dict[each_dataset.get('data_set_id')] = each_dataset

    return has_filter_cond, filter_dataset_dict
