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
import numpy as np
from pandas import DataFrame


def score_ranking(sorted_list):
    # 用pandas实现分组排序
    dataset_id_list = []
    score_list = []
    for each_dataset in sorted_list:
        dataset_id_list.append(each_dataset[0])
        score_list.append(each_dataset[1])
    range_score_dict = {'dataset_id': dataset_id_list, 'score': score_list}

    df = pd.DataFrame(data=range_score_dict)
    df['ranking'] = df['score'].rank(method='max')
    df['ranking_perct'] = (df['ranking']) / len(df)
    return df


def score_distribution(df):
    """
    散点图处理函数
    :param df:
    :return:
    """
    df_sorted = df.sort_values(by='score')
    sampled_df = df_sorted.sample(frac=0.4, random_state=100)
    sampled_df_sorted = sampled_df.sort_values(by='score')
    # 图例
    legend = ['0', '1-30', '30-60', '60-90', '>90']
    bins = [0, 1, 30, 60, 90, 100]
    score_cat = pd.cut(sampled_df_sorted['score'], bins, right=False)
    bin_result_list = pd.value_counts(score_cat, sort=False).values.tolist()
    sorted_list = sampled_df_sorted['score'].values.tolist()
    sorted_dict = {}
    sum = 0
    for i in range(len(bin_result_list)):
        index_start = sum
        index_end = sum + bin_result_list[i]
        sum += bin_result_list[i]
        sorted_dict[legend[i]] = {}
        if index_start == index_end:
            sorted_dict[legend[i]]['x'] = []
            sorted_dict[legend[i]]['y'] = []
        else:
            sorted_dict[legend[i]]['x'] = list(range(index_start + 1, index_end + 1))
            sorted_dict[legend[i]]['y'] = sorted_list[index_start:index_end]
    return sorted_dict


def score_aggregate(score_list, score_type=None):
    """
    score bubble process function
    :param score_list:
    :return:
    """
    df = DataFrame(score_list)
    t1 = df.groupby('score', as_index=False).count().sort_values(['score'], axis=0, ascending=True)

    if score_type != 'importance' and score_type != 'assetvalue_to_cost':
        # 取对数并修正0值
        t1['cnt'] = t1['count'].apply(lambda x: np.log(x))
        t1.loc[t1.cnt <= 1, 'cnt'] = 1
        t1 = t1.sort_values(by='count', ascending=False)

        # 取阈值进行二次修正
        threshold = int(t1['count'].mean() * 20)
        # 数据修正
        minx = t1[t1['count'] > threshold]['count'].min()
        # 按实际数据的倍数再扩大：
        tmp = np.around(np.array(sorted(t1[t1['count'] > threshold]['count'], reverse=True)) / minx / 2, 2)
        t1['ratio'] = list(tmp) + [1] * (len(t1) - len(tmp))
        t1['cnt'] = t1['cnt'] * t1['ratio']

    # >=当前分支的累积数量
    t1 = t1.sort_values(by='score', ascending=False)
    t1['des_cum_count'] = t1['count'].cumsum()
    # 按热度值排序
    t1 = t1.sort_values(by='score')
    t1['cum_count'] = t1['count'].cumsum()

    score_agg_list = t1.to_dict(orient='records')
    y = [each['score'] for each in score_agg_list]
    z = [each['count'] for each in score_agg_list]
    x = list(range(len(y), 0, -1))
    # 每个分值占比
    perc_list = [round(each['count'] / float(len(df)), 7) for each in score_agg_list]
    if score_type != 'importance' and score_type != 'assetvalue_to_cost':
        cnt_list = [each['cnt'] for each in score_agg_list]
    elif score_type == 'importance':
        cnt_list = [
            each * 300 * (len(score_list) / 50000.0) if each * 300 * (len(score_list) / 50000.0) > 1 else 1
            for each in perc_list
        ]
    elif score_type == 'assetvalue_to_cost':
        cnt_list = [
            each * 1000 * (len(score_list) / 10000.0) if each * 1000 * (len(score_list) / 10000.0) > 1 else 1
            for each in perc_list
        ]
    # 累积占比
    cum_perc_list = [round(each['des_cum_count'] / float(len(df)), 7) for each in score_agg_list]
    return {'x': x, 'y': y, 'z': z, 'cnt': cnt_list, 'cum_perc': cum_perc_list, 'perc': perc_list}
