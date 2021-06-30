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


from datamanage.pro.lifecycle.utils import time_format
from datamanage.pro.lifecycle.metrics.cost import hum_storage_unit


def storage_capacity_trend(data_trend_df, filter_dataset_dict):
    # 1）数据准备
    # 如果没有搜素条件
    if not filter_dataset_dict:
        trend_df = data_trend_df
    # 将符合搜索条件数据集命中到data_trend_df的部分拿出来
    else:
        trend_df = data_trend_df[data_trend_df['dataset_id'].isin(list(filter_dataset_dict.keys()))]

    if trend_df.empty:
        return {'tspider_capacity': [], 'hdfs_capacity': [], 'total_capacity': [], 'time': [], 'unit': 'B'}
    # 缺失值填充
    trend_df = trend_df.fillna(0)

    # 按照time排序
    sum_df = trend_df[['hdfs_capacity', 'tspider_capacity', 'total_capacity', 'time']].groupby(['time']).sum()
    sum_df = sum_df.reset_index().sort_values(['time'], axis=0, ascending=True)
    hdfs_list = sum_df['hdfs_capacity'].values.tolist()
    tspider_list = sum_df['tspider_capacity'].values.tolist()
    total_list = sum_df['total_capacity'].values.tolist()
    time_list = sum_df['time'].values.tolist()

    ret_dict = {
        'time': time_list,
        'hdfs_capacity': hdfs_list,
        'tspider_capacity': tspider_list,
        'total_capacity': total_list,
    }

    # 按照total_capacity确定最大单位量级
    max_capacity = max(ret_dict['total_capacity'])
    format_max_capacity, unit, power = hum_storage_unit(max_capacity, return_unit=True)

    # 将所有存储的数据转换为最大单位对应的数值
    for i in range(len(ret_dict['total_capacity'])):
        ret_dict['hdfs_capacity'][i] = round(ret_dict['hdfs_capacity'][i] / float(1024 ** power), 3)
        ret_dict['tspider_capacity'][i] = round(ret_dict['tspider_capacity'][i] / float(1024 ** power), 3)
        ret_dict['total_capacity'][i] = round(ret_dict['total_capacity'][i] / float(1024 ** power), 3)
    ret_dict['unit'] = unit

    # 将timestamp转化为可读时间格式
    ret_dict['time'] = time_format(ret_dict['time'])
    return ret_dict
