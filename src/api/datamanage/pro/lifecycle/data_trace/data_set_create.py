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


from copy import deepcopy

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.utils.api import MetaApi
from datamanage.pro.utils.time import utc_to_local, str_to_datetime
from datamanage.pro.lifecycle.models_dict import (
    DATASET_CREATE_MAPPINGS,
    DATASET_CREATE_EVENT_INFO_DICT,
    DataTraceShowType,
    ComplexSearchBackendType,
    DataTraceFinishStatus,
)


def get_dataset_create_info(dataset_id, dataset_type):
    """获取数据足迹中和数据创建相关信息

    :param dataset_id: 数据id
    :param dataset_type: 数据类型

    :return: 数据创建相关信息
    :rtype: list
    """
    # 1)从dgraph中获取数据创建相关信息
    data_set_create_info_statement = """
    {
        get_dataset_create_info(func: eq(%s, "%s")){created_by created_at}
    }
    """ % (
        DATASET_CREATE_MAPPINGS[dataset_type]['data_set_pk'],
        dataset_id,
    )
    query_result = MetaApi.complex_search(
        {"backend_type": ComplexSearchBackendType.DGRAPH.value, "statement": data_set_create_info_statement}, raw=True
    )
    create_info_ret = query_result['data']['data']['get_dataset_create_info']
    if not (isinstance(create_info_ret, list) and create_info_ret):
        raise dm_pro_errors.GetDataSetCreateInfoError(message_kv={'dataset_id': dataset_id})

    # 2)得到格式化创建信息
    create_trace_dict = deepcopy(DATASET_CREATE_EVENT_INFO_DICT)
    create_trace_dict.update(
        {
            "sub_type": dataset_type,
            "sub_type_alias": DATASET_CREATE_MAPPINGS[dataset_type]['data_set_create_alias'],
            "description": DATASET_CREATE_MAPPINGS[dataset_type]['data_set_create_alias'],
            "created_at": utc_to_local(create_info_ret[0]['created_at']),
            "created_by": create_info_ret[0]['created_by'],
            "show_type": DataTraceShowType.DISPLAY.value,
            "datetime": str_to_datetime(utc_to_local(create_info_ret[0]['created_at'])),
            "status": DataTraceFinishStatus.STATUS,
            "status_alias": DataTraceFinishStatus.STATUS_ALIAS,
        }
    )
    return [create_trace_dict]
