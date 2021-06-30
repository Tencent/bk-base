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


import pandas as pd

from datamanage.pro.datastocktake.settings import SCORE_DICT, LEVEL_DICT, DEFAULT_SCORE_DICT
from datamanage.pro.lifecycle.utils import time_format


def score_trend_pandas_groupby(data_trend_df, filter_dataset_dict, score_type):
    """
    评分趋势
    :param trend_list_df:
    :param filter_dataset_dict:
    :param score_type:
    :return:
    """

    def group_by_time_func(df):
        """
        按照time来聚合
        :param df:
        :return:
        """
        x = LEVEL_DICT[score_type]['x']
        bins = LEVEL_DICT[score_type]['bins']
        score_cat = pd.cut(df['score'], bins, right=False)
        bin_result_list = pd.value_counts(score_cat, sort=False).values.tolist()
        return pd.DataFrame({'bin_result_list': bin_result_list, "score_level": x})

    def group_by_label_func(df):
        """
        按照分箱的label来聚合
        :param df:
        :return:
        """
        return pd.DataFrame({'num': [df['bin_result_list'].values.tolist()], 'time': [df['time'].values.tolist()]})

    # 1）数据准备
    # 如果没有搜素条件
    if not filter_dataset_dict:
        trend_df = data_trend_df
    # 将符合搜索条件数据集命中到data_trend_df的部分拿出来
    else:
        trend_df = data_trend_df[data_trend_df['dataset_id'].isin(list(filter_dataset_dict.keys()))]

    if trend_df.empty:
        return []

    # 给socre赋值，并进行空值填充
    trend_df['score'] = trend_df[SCORE_DICT[score_type]]
    score_series = trend_df['score'].fillna(DEFAULT_SCORE_DICT[score_type])
    trend_df['score'] = score_series

    # 2）聚合
    # 按照时间聚合
    bin_result_df = trend_df.groupby('time').apply(group_by_time_func)
    bin_result_df = bin_result_df.reset_index()
    # 按分箱label和索引level_1聚合（level_1用于说明分箱顺序）
    ret_df = bin_result_df.groupby(['score_level', 'level_1']).apply(group_by_label_func).reset_index()
    ret_list = ret_df.to_dict(orient='records')

    # 3）数据格式化
    for each in ret_list:
        each['time'] = time_format(each['time'])
        each['index'] = each['level_1']
        each.pop('level_1')
        each.pop('level_2')
    ret_list.sort(key=lambda k: k['index'])

    return ret_list


def score_trend(trend_list, filter_dataset_dict, score_type):
    """
    评分趋势
    :param trend_list:
    :param filter_dataset_dict:
    :param score_type:
    :return:
    """
    score_dict = {}
    # 1）首先以时间来划分，对相同时间的分数进行分箱，得到不同分数段的数据量
    # time为key，不同数据的得分放在key对应的[]中
    for each in trend_list:
        if not filter_dataset_dict or each['dataset_id'] in filter_dataset_dict:
            if each['time'] not in list(score_dict.keys()):
                score_dict[each['time']] = []
            score_dict[each['time']].append(each[SCORE_DICT[score_type]]) if each.get(
                SCORE_DICT[score_type]
            ) else score_dict[each['time']].append(DEFAULT_SCORE_DICT[score_type])

    # 分箱label和列表
    x = LEVEL_DICT[score_type]['x']
    bins = LEVEL_DICT[score_type]['bins']
    ret_dict = {}
    # 将每个time对应的所有数据的分数进行分箱
    index = -1
    for key, item in list(score_dict.items()):
        score_cat = pd.cut(item, bins, right=False)
        bin_result_list = pd.value_counts(score_cat, sort=False).values.tolist()
        for i in range(len(x)):
            if x[i] not in list(ret_dict.keys()):
                index += 1
                ret_dict[x[i]] = {}
                ret_dict[x[i]]['time'] = []
                ret_dict[x[i]]['num'] = []
                ret_dict[x[i]]['index'] = index
            ret_dict[x[i]]['time'].append(key)
            ret_dict[x[i]]['num'].append(bin_result_list[i])

    # 将时间戳转化为可读的时间格式
    for key, item in list(ret_dict.items()):
        item['time'] = time_format(item['time'])

    # 最终返回的列表每个元素代表一个数据曲线
    ret_list = []
    for key, item in list(ret_dict.items()):
        ret_list.append({'time': item['time'], 'num': item['num'], 'score_level': key, 'index': item['index']})
    ret_list = sorted(ret_list, key=lambda item: item['index'])
    return ret_list
