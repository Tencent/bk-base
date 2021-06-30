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

from datamanage.pro.datastocktake.settings import SCORE_DICT, LEVEL_DICT


def level_distribution(metric_list, filter_dataset_dict, score_type):
    score_list = []
    for each in metric_list:
        if not filter_dataset_dict or each['dataset_id'] in filter_dataset_dict:
            score_list.append(each[SCORE_DICT[score_type]])

    x = LEVEL_DICT[score_type]['x']
    bins = LEVEL_DICT[score_type]['bins']
    score_cat = pd.cut(score_list, bins, right=False)
    bin_result_list = pd.value_counts(score_cat, sort=False).values.tolist()
    sum_count = len(score_list)
    z = [round(each / float(sum_count), 7) for each in bin_result_list]
    return x, bin_result_list, z, sum_count
