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


import json

from datamanage.pro.utils.time import str_to_datetime
from datamanage.pro.lifecycle.data_trace.data_trace_elasticsearch import DataTraceElasticSearchClient
from datamanage.pro.lifecycle.models_dict import (
    EventType,
    Migrate,
    DataTraceFinishStatus,
)


def get_data_trace_events_from_es(dataset_id):
    """获取写入es的数据足迹事件

    :param dataset_id: 数据id
    :return:
    """
    # 1)从es中获取数据足迹信息
    data_trace_es_client = DataTraceElasticSearchClient()
    resp = data_trace_es_client.search_data_trace(dataset_id)
    data_trace_event_list = resp['hits']['hits']

    # 2)对es中获取的数据足迹进行解析 & 格式化处理 & 按event_sub_type和action_id聚合
    return parse_convg_data_trace_events_in_es(data_trace_event_list)


def parse_convg_data_trace_events_in_es(data_trace_event_list):
    """对es中获取的数据足迹进行解析 & 格式化处理 & 按event_sub_type和action_id聚合

    :param data_trace_event_list: es中的数据足迹事件列表
    :return: 按event_sub_type和action_id进行聚合的es数据足迹事件列表
    """
    # 1)对es中获取的数据足迹进行解析&格式化处理
    data_trace_list = []
    for data_trace_event_dict in data_trace_event_list:
        data_trace_dict = data_trace_event_dict['_source']
        opr_info_dict = json.loads(data_trace_dict['opr_info'])
        data_trace_list.append(
            {
                "event_id": data_trace_dict['id'],
                "action_id": data_trace_dict['dispatch_id'],
                "type": data_trace_dict['opr_type'],
                "type_alias": opr_info_dict['alias'],
                "sub_type": data_trace_dict['opr_sub_type'],
                "sub_type_alias": opr_info_dict['sub_type_alias'],
                "description": data_trace_dict['description'],
                "created_at": data_trace_dict['created_at'],
                "created_by": data_trace_dict['created_by'],
                "show_type": opr_info_dict['show_type'],
                "details": opr_info_dict,
            }
        )

    # 2)对es中获取的数据足迹按照event_sub_type和action_id进行初步聚合
    merge_dict = {}
    for item in data_trace_list:
        event_params = item['details']['desc_params']
        # 对相同event_sub_type和action_id的足迹进行聚合
        merge_key = '{}_{}'.format(item['action_id'], item['sub_type'])
        if merge_key not in merge_dict:
            merge_dict[merge_key] = item
        else:
            merge_dict[merge_key]['details']['desc_params'].extend(event_params)
    return list(merge_dict.values())


def set_event_status_convg_mgr_events(event_list):
    """补充事件状态信息 & 对迁移事件按照action_id聚合

    :param event_list: 数据足迹事件列表

    :return:格式化的数据足迹事件列表
    """
    # 1) 补充事件状态信息 & 事件是否迁移事件
    ret_event_list = []
    migrate_dict = {}
    for event_dict in event_list:
        # 补充datetime字段，用于按照时间排序
        event_dict['datetime'] = str_to_datetime(event_dict['created_at'])
        # 事件类型是迁移
        if event_dict['type'] == EventType.MIGRATE.value:
            if event_dict['action_id'] not in migrate_dict:
                migrate_dict[event_dict['action_id']] = []
            migrate_dict[event_dict['action_id']].append(event_dict)
        # 事件类型:非迁移
        else:
            event_dict.update(
                {'status': DataTraceFinishStatus.STATUS, 'status_alias': DataTraceFinishStatus.STATUS_ALIAS}
            )
            ret_event_list.append(event_dict)

    # 2) 迁移事件按action_id聚合（迁移事件的状态是最后一个子事件的状态）
    for action_id, migrate_events in list(migrate_dict.items()):
        # 数据迁移子事件按照时间排序
        migrate_events.sort(key=lambda event: (event['datetime']), reverse=False)
        event_dict = {
            'datetime': migrate_events[0]['datetime'],
            'created_by': migrate_events[0]['created_by'],
            'created_at': migrate_events[0]['created_at'],
            'type': migrate_events[0]['type'],
            'type_alias': migrate_events[0]['type_alias'],
            'sub_events': migrate_events,
            'status_alias': Migrate.MIGRATE_STATUS[migrate_events[-1]['sub_type']],
            'status': migrate_events[-1]['sub_type'],
            'show_type': migrate_events[0]['show_type'],
        }
        ret_event_list.append(event_dict)
    return ret_event_list
