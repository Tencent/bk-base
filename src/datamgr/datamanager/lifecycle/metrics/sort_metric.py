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
from __future__ import absolute_import, print_function, unicode_literals

import pandas as pd


def score_ranking(score_dict):
    """
    用pandas实现分组排序
    :param score_dict: dict {'591_sum_test_0601': 13.1, '591_b_tpg7': 13.1, '591_tdw_ltpg6': 14.14}
    :return: DataFrame
    pd.DataFrame([['591_sum_test_0601', 13.10, 2.0, 0.6667],
    ['591_b_tpg7', 13.10, 2.0, 0.6667],
    ['591_tdw_ltpg6', 14.14, 3.0, 1.0]],
    columns=['dataset_id', 'score', 'ranking', 'ranking_perct'])
    """
    sorted_list = sorted(score_dict.items(), key=lambda item: item[1])
    dataset_id_list = []
    score_list = []
    for each_dataset in sorted_list:
        dataset_id_list.append(each_dataset[0])
        score_list.append(each_dataset[1])
    score_dict = {"dataset_id": dataset_id_list, "score": score_list}

    df = pd.DataFrame(data=score_dict)
    df["ranking"] = df["score"].rank(method="max")
    df["ranking_perct"] = (df["ranking"]) / len(df)
    return df
