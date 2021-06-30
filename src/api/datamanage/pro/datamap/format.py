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
# -*- coding: UTF-8 -*-


def format_search_res(dict, page=None, page_size=None):
    res_list = []
    if dict:
        if len(dict.get('data_set_id_list', [])) > page_size and page and page_size:
            offset = (page - 1) * page_size
            dict['data_set_id_list'] = dict['data_set_id_list'][offset : offset + page_size]

        for each_ds in dict.get('data_set_id_list', []):
            if '~LifeCycle.heat' in each_ds:
                res_list.append(each_ds['~LifeCycle.heat'][0].get('LifeCycle.target')[0])
            elif '~LifeCycle.range' in each_ds:
                res_list.append(each_ds['~LifeCycle.range'][0].get('LifeCycle.target')[0])
            elif '~LifeCycle.importance' in each_ds:
                res_list.append(each_ds['~LifeCycle.importance'][0].get('LifeCycle.target')[0])
            elif '~LifeCycle.asset_value' in each_ds:
                res_list.append(each_ds['~LifeCycle.asset_value'][0].get('LifeCycle.target')[0])
            elif '~Cost.capacity' in each_ds:
                res_list.append(each_ds['~Cost.capacity'][0].get('~LifeCycle.cost')[0].get('LifeCycle.target')[0])
            elif 'LifeCycle.target' in each_ds:
                res_list.append(each_ds['LifeCycle.target'][0])
            else:
                return dict
        dict['data_set_id_list'] = res_list

        return dict
