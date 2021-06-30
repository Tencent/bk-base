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

import enum


class ListValueMixin(object):
    @classmethod
    def get_enum_value_list(cls, excludes=None):
        if excludes is None:
            excludes = []
        return [m.value for m in cls.__members__.values() if m.value not in excludes]

    @classmethod
    def get_enum_key_list(cls):
        return cls.__members__.keys()


class NodeType(ListValueMixin, enum.Enum):
    # dataflow节点类型
    SPLIT = "split"  # 数据迁移


class DataTraceShowType(ListValueMixin, enum.Enum):
    # 数据足迹展示形式
    DISPLAY = "display"
    ADD_TIPS = "add_tips"
    GROUP = "group"


DATA_TRACE_ES_INDEX_NAME = "data_trace_log"
DATA_TRACE_ES_TYPE_NAME = "data_trace"
DATA_TRACE_ES_NAME = "data_trace"
DATA_TRACE_SUBSCRIBE_QUEUE_NAME = "data_trace_listener"
DATA_TRACE_SUBSCRIBE_ROUTING_KEY = "mutation"
DATA_TRACE_SUBSCRIBE_REFER_NAME = "datamanage"
DATA_TRACE_SUBSCRIBE_CONFIG = dict(
    name=DATA_TRACE_SUBSCRIBE_QUEUE_NAME,
    key=DATA_TRACE_SUBSCRIBE_ROUTING_KEY,
    refer=DATA_TRACE_SUBSCRIBE_REFER_NAME,
)


class FieldUpdateType(ListValueMixin, enum.Enum):
    UPDATE_FIELD_INDEX = "update_field_index"
    UPDATE_FIELD_DESC = "update_field_desc"
    UPDATE_ALIAS_DESC = "update_alias_desc"
    CREATE_FIELD = "create_field"


DATA_TRACE_EVENT_DICT = dict(
    TraceTypeDelete=dict(
        name="delete",
        alias="销毁数据",
        sub_type_mapping=dict(
            delete_access_raw_data=dict(
                alias="销毁数据源",
                event_name="EventDeleteNode",
                scope=["AccessRawData.id"],
                desc_tpl="销毁数据源",
            ),
            delete_result_table=dict(
                alias="销毁结果表",
                event_name="EventDeleteNode",
                scope=["ResultTable.result_table_id"],
                desc_tpl="销毁结果表",
            ),
        ),
    ),
    TraceTypeUpdateAttr=dict(
        name="update_attr",
        alias="属性变化",
        sub_type_mapping=dict(
            update_value=dict(
                alias="属性值变化",
                event_name="EventUpdateAttr",
                scope=["ResultTable.sensitivity"],
                desc_tpl="属性值变化: {change_content}",
                kv_tpl="{pred_name}: {before} -> {after}",
                show_type="add_tips",
            )
        ),
    ),
    TraceTypeUpdateSchema=dict(
        name="update_schema",
        alias="结构变化",
        sub_type_mapping=dict(
            create_field=dict(
                alias="新增字段",
                event_name="EventCreateNode",
                scope=["ResultTableField.id"],
                desc_tpl="新增字段: {change_content}",
                kv_tpl="{field_name}",
                show_type="add_tips",
            ),
            delete_field=dict(
                alias="删除字段",
                event_name="EventDeleteNode",
                scope=["ResultTableField.id"],
                desc_tpl="删除字段: {change_content}",
                kv_tpl="{field_name}",
                show_type="add_tips",
            ),
            update_field_desc=dict(
                alias="字段描述变更",
                event_name="EventUpdateAttr",
                scope=["ResultTableField.description"],
                desc_tpl="字段描述变更: {change_content}",
                kv_tpl="{pred_name}: {before} -> {after}",
                show_type="add_tips",
            ),
            update_alias_desc=dict(
                alias="字段别名变更",
                event_name="EventUpdateAttr",
                scope=["ResultTableField.field_alias"],
                desc_tpl="字段别名变更: {change_content}",
                kv_tpl="{pred_name}: {before} -> {after}",
                show_type="add_tips",
            ),
        ),
    ),
    TraceTypeUpdateTaskStatus=dict(
        name="update_task_status",
        alias="任务状态变化",
        node_type=None,
        sub_type_mapping=dict(
            task_start=dict(
                alias="flow任务启动",
                event_name="EventUpdateAttr",
                scope=["DataflowNodeInfo.status"],
                desc_tpl="关联flow任务 {change_content} 启动",
                kv_tpl="{flow_id}",
                rule=dict(before="starting", after="running"),
                show_type="add_tips",
            ),
            task_stop=dict(
                alias="flow任务停止",
                event_name="EventUpdateAttr",
                scope=["DataflowNodeInfo.status"],
                desc_tpl="关联flow任务 {change_content} 停止",
                kv_tpl="{flow_id}",
                rule=dict(before="stopping", after="no-start"),
                show_type="add_tips",
            ),
        ),
    ),
    TraceTypeMigrate=dict(
        name="migrate",
        alias="数据迁移",
        sub_type_mapping=dict(
            migrate_start=dict(
                alias="开始数据迁移",
                event_name="EventCreateNode",
                scope=["DatabusMigrateTask.status"],
                desc_tpl="开始数据迁移任务",
                rule=dict(after="init"),
                show_type="group",
            ),
            migrate_finish=dict(
                alias="数据迁移成功",
                event_name="EventUpdateAttr",
                scope=["DatabusMigrateTask.status"],
                desc_tpl="数据迁移任务成功",
                rule=dict(after="finish"),
                show_type="group",
            ),
        ),
    ),
)
