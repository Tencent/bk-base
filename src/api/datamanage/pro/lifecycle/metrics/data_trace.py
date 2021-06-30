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


from django.db import connections
from django.utils.translation import ugettext as _

from datamanage.pro.utils.time import utc_to_local
from datamanage.lite.tag import tagaction
from datamanage.utils.api.meta import MetaApi


def get_stan_list(dataset_id):
    """
    拿到数据标准化列表
    :return:
    """
    sql = """select a.data_set_id data_set_id, a.created_at created_at, a.created_by created_by,
                      b.standard_id standard_id, c.flow_id as flow_id
                      from dm_task_detail a, dm_standard_version_config b, dm_task_config c
                      where a.data_set_id = '{}' and a.standard_version_id=b.id and b.standard_version_status = 'online'
                      and a.task_id = c.id;""".format(
        dataset_id
    )
    stan_list = tagaction.query_direct_sql_to_map_list(connections['bkdata_basic_slave'], sql)
    for each_stan in stan_list:
        each_stan['status'] = 'finish'
        each_stan['created_at'] = utc_to_local(each_stan['created_at'])
    return stan_list


def get_data_create_dict(dataset_id, dataset_type):
    """
    数据创建详情
    :param dataset_id:
    :param dataset_type:
    :return:
    """
    statement = (
        """
    {
      dataset(func: eq($Dataset.id, %s)){
        created_at
        created_by
      }
    }
    """
        % dataset_id
    )

    if dataset_type == 'raw_data':
        statement = statement.replace('$Dataset.id', 'AccessRawData.id')
    elif dataset_type == 'result_table':
        statement = statement.replace('$Dataset.id', 'ResultTable.result_table_id')
    elif dataset_type == 'tdw_table':
        statement = statement.replace('$Dataset.id', 'TdwTable.table_id')
    search_dict = MetaApi.complex_search({'statement': statement, 'backend_type': 'dgraph'}).data
    dataset_dict = (
        search_dict['data']['dataset'][0] if search_dict and search_dict.get('data', {}).get('dataset', []) else {}
    )
    create_dict = {}
    create_dict['created_at'] = utc_to_local(dataset_dict.get('created_at', ''))
    create_dict['created_by'] = dataset_dict.get('created_by', '')
    create_dict['type'] = 'create'
    create_dict['type_alias'] = _('数据创建')
    create_dict['status'] = 'finish'
    return create_dict
