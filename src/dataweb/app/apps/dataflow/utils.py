# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
import datetime
import threading
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait


def format_date_string(date_string):
    """将输入日期进行格式化处理"""
    date_time = datetime.datetime.strptime(date_string, "%Y%m%d")
    return date_time.strftime("%m/%d")


def fun_dup_remove(x, y):
    """
    去重函数
    如果y在x中，直接返回x，如果y不在x中，将y加入到x中
    :param x: list
    :param y: 元素
    :return:
    """
    if y in x:
        return x
    else:
        return x + [y]


def subdigit(str):
    """
    提取带数字字符串
    :param str:'3','3d'
    :return:'3'
    """
    if "d" in str and len(str) >= 2:
        digit = str[0:-1]
    else:
        digit = str
        # pattern = re.compile(r'\d')
        # re_list = pattern.findall(str)
        # digit = re_list[0] if re_list else ''
    return digit


def async_func(func, params_list, request, res=None):
    """
    并发函数
    :param res:
    :param params_list:
    :param func:
    :return:
    """
    t_list = []
    for params in params_list:
        if isinstance(res, list):
            t = threading.Thread(target=func, args=(params, request, res))
        else:
            t = threading.Thread(target=func, args=(params, request))
        t.start()
        t_list.append(t)

    for t in t_list:
        t.join()


def thread_pool_func(func, request, language, item_list, work_num=100):
    # 并发查询数据源和数据处理节点tooltip信息
    with ThreadPoolExecutor(max_workers=work_num) as ex:
        future_tasks = []
        for item in item_list:
            future_tasks.append(ex.submit(func, item, request, language))

        wait(future_tasks, return_when=ALL_COMPLETED)
    return item_list
