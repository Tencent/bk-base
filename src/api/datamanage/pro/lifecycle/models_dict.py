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


import enum

from django.utils.translation import ugettext_lazy as _


class ListValueMixin(object):
    @classmethod
    def get_enum_value_list(cls):
        return [m.value for m in list(cls.__members__.values())]


class EventType(ListValueMixin, enum.Enum):
    # 数据模型发布状态字典表
    MIGRATE = 'migrate'  # 数据迁移


class Migrate(object):
    # 数据迁移
    MIGRATE_START = 'migrate_start'
    MIGRATE_FINISH = 'migrate_finish'
    MIGRATE_FAILED = 'migrate_failed'
    MIGRATE_STATUS = {
        MIGRATE_START: _('进行中'),
        MIGRATE_FINISH: _('已完成'),
        MIGRATE_FAILED: _('失败'),
    }


class DataTraceFinishStatus(object):
    # 数据足迹成功状态
    STATUS = 'finish'
    STATUS_ALIAS = _('已完成')


class DataTraceShowType(ListValueMixin, enum.Enum):
    # 数据足迹展示形式
    DISPLAY = 'display'
    ADD_TIPS = 'add_tips'
    GROUP = 'group'


class DataSetType(ListValueMixin, enum.Enum):
    # 数据集类型
    RAW_DATA = 'raw_data'
    RESULT_TABLE = 'result_table'
    TDW_TABLE = 'tdw_table'


# 数据集创建相关内容
DATASET_CREATE_MAPPINGS = {
    DataSetType.RAW_DATA.value: {'data_set_pk': 'AccessRawData.id', 'data_set_create_alias': _('创建数据源')},
    DataSetType.RESULT_TABLE.value: {'data_set_pk': 'ResultTable.result_table_id', 'data_set_create_alias': _('创建结果表')},
    DataSetType.TDW_TABLE.value: {'data_set_pk': 'TdwTable.table_id', 'data_set_create_alias': _('创建TDW结果表')},
}

DATASET_CREATE_EVENT_INFO_DICT = {
    "event_id": "auto",
    "action_id": "",
    "type": "create",
    "type_alias": _('创建数据'),
    "details": {},
}


class ComplexSearchBackendType(ListValueMixin, enum.Enum):
    DGRAPH = 'dgraph'
    MYSQL = 'mysql'


DATA_TRACE_ES_INDEX_NAME = 'data_trace_log'
DATA_TRACE_ES_TYPE_NAME = 'data_trace'
DATA_TRACE_ES_MAX_RESULT_WINDOW = 10000
DATA_TRACE_ES_NAME = 'data_trace'
