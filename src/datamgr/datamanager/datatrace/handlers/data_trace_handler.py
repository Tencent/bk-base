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

import json
import logging

import cattr

from datatrace.data_trace_dict import (
    DATA_TRACE_EVENT_DICT,
    DataTraceShowType,
    FieldUpdateType,
    NodeType,
)
from models.meta import DataTraceInfo

logger = logging.getLogger(__name__)


class BaseTraceType(object):
    """数据足迹事件基类型"""

    def __init__(self, handle_info):
        self.cls_name = self.__class__.__name__
        self.name = ""
        self.alias = ""
        self.sub_type_mapping = dict()
        if self.cls_name in DATA_TRACE_EVENT_DICT:
            self.name = DATA_TRACE_EVENT_DICT[self.cls_name]["name"]
            self.alias = DATA_TRACE_EVENT_DICT[self.cls_name]["alias"]
            self.sub_type_mapping = DATA_TRACE_EVENT_DICT[self.cls_name][
                "sub_type_mapping"
            ]

        self.handle_info = handle_info
        self.identifier_key = handle_info["identifier_key"]
        self.identifier_value = handle_info["identifier_value"]
        self.changed_md = self.handle_info["changed_md"]
        self.event_name = self.handle_info["event_name"]
        self.differ = self.handle_info["differ"]
        self.origin_data = self.handle_info["origin_data"]
        self.changed_data = self.handle_info["changed_data"]
        self.trace_info_list = list()
        self.remote_data = dict()

    def filling(self):
        """补充数据足迹事件相关信息

        :return:
        """
        # 数据足迹跳转相关信息
        jump_to_info = self.get_jump_to_info()
        # 数据足迹相关内容和相关描述
        desc_item_list = self.get_opr_desc_list()
        i = 0
        for sub_trace_type, desc_item in desc_item_list.items():
            # 获取数据足迹事件对应的data_set_id
            data_set_ids = self.get_data_set_ids()
            for data_set_id in data_set_ids:
                if not data_set_id:
                    continue
                desc_params = desc_item.get("params", [])
                desc_tpl = desc_item.get("desc_tpl", "")
                kv_tpl = desc_item.get("kv_tpl", "")
                show_type = desc_item.get("show_type", DataTraceShowType.DISPLAY.value)
                sub_type_alias = desc_item.get("alias", "")
                opr_info = dict(
                    alias=self.alias,
                    sub_type_alias=sub_type_alias,
                    jump_to=jump_to_info,
                    desc_params=desc_params,
                    desc_tpl=desc_tpl,
                    kv_tpl=kv_tpl,
                    show_type=show_type,
                )

                trace_info_obj = DataTraceInfo(
                    # 修改id,避免es写入的时候会发生覆盖(用户一次操作导致多个足迹中定义的子事件变更，例如同时修改字段中文名和描述)
                    id=f'{self.handle_info["event_id"]}-{i}',
                    dispatch_id=self.handle_info["dispatch_id"],
                    data_set_id=data_set_id,
                    opr_type=self.name,
                    opr_sub_type=sub_trace_type,
                    opr_info=json.dumps(opr_info),
                    description=(
                        desc_tpl.format(
                            change_content="; ".join(
                                [
                                    kv_tpl.format(**desc_param)
                                    for desc_param in desc_params
                                ]
                            )
                        )
                        if desc_params and kv_tpl
                        else desc_tpl
                    ),
                    created_by=self.handle_info["created_by"],
                    created_at=self.handle_info["created_at"],
                )
                trace_info = cattr.unstructure(trace_info_obj)
                self.trace_info_list.append(trace_info)
                i += 1
            logger.info("filling, self.trace_info_list:{}".format(self.trace_info_list))

    def get_opr_desc_list(self):
        """获取数据足迹相关内容和相关描述"""
        desc_item_list = dict()
        for sub_trace_type, sub_trace_attr in self.sub_type_mapping.items():
            sub_trace_dict = dict(
                alias=sub_trace_attr.get("alias", ""),
                desc_tpl=sub_trace_attr.get("desc_tpl", ""),
                kv_tpl=sub_trace_attr.get("kv_tpl", ""),
                show_type=sub_trace_attr.get(
                    "show_type", DataTraceShowType.DISPLAY.value
                ),
            )
            if "params" not in sub_trace_dict:
                sub_trace_dict["params"] = list()
            for pred_name, diff_item in self.differ.items():
                if self.check_sub_trace_scope(pred_name, sub_trace_attr):
                    desc_params = self.get_sub_type_desc(
                        sub_trace_type, pred_name, diff_item
                    )
                    if desc_params:
                        sub_trace_dict["params"].append(desc_params)
                    desc_item_list[sub_trace_type] = sub_trace_dict
                    break
        return desc_item_list

    def check_sub_trace_scope(self, pred_name, sub_trace_attr):
        """判断事件是否符合定义的数据足迹子类型

        :param pred_name: predicate name，dgraph类中的属性名称，for example: 'field_alias'
        :param sub_trace_attr: 子类型变更情况和描述，对应DATA_TRACE_EVENT_DICT中定义的数据足迹子事件内容，for example:
            {
                'alias': '字段描述变更',
                'event_name': 'EventUpdateAttr',
                'scope': ['ResultTableField.description'],
                'desc_tpl': '字段描述变更: {change_content}',
                'kv_tpl': '{pred_name}: {before} -> {after}',
                'show_type': 'add_tips'
            }
        :return:
        """
        # dgraph类.predicate name
        scope_key = "{}.{}".format(self.changed_md, pred_name)
        return self.event_name == sub_trace_attr["event_name"] and (
            sub_trace_attr["scope"] is True or scope_key in sub_trace_attr["scope"]
        )

    def get_data_set_ids(self):
        """返回事件对应的data_set_ids列表

        :return:
        """
        if getattr(self, "identifier_value"):
            return [self.identifier_value]
        return []

    def get_jump_to_info(self):
        """返回数据足迹跳转相关内容"""
        return dict()

    def get_sub_type_desc(self, sub_trace_type, pred_name, diff_item):
        """返回子类型相关描述

        :param sub_trace_type:
        :param pred_name:
        :param diff_item:
        :return:
        """
        return dict()


class TraceTypeDelete(BaseTraceType):
    """删除数据"""


class TraceTypeUpdateAttr(BaseTraceType):
    """数据属性变更"""

    def get_sub_type_desc(self, sub_trace_type, pred_name, diff_item):
        return dict(pred_name=pred_name, before=diff_item["b"], after=diff_item["a"])


class TraceTypeUpdateSchema(BaseTraceType):
    """数据结构变更"""

    def get_data_set_ids(self):
        if "result_table_id" in self.origin_data:
            return [self.origin_data["result_table_id"]]
        if "result_table_id" in self.changed_data:
            return [self.changed_data["result_table_id"]]
        return []

    def get_sub_type_desc(self, sub_trace_type, pred_name, diff_item):
        if sub_trace_type in FieldUpdateType.get_enum_value_list(
            excludes=[FieldUpdateType.CREATE_FIELD.value]
        ):
            return dict(
                pred_name=pred_name, before=diff_item["b"], after=diff_item["a"]
            )
        elif sub_trace_type == FieldUpdateType.CREATE_FIELD.value:
            return dict(field_name=self.changed_data.get("field_name", ""))
        else:
            return dict(field_name=self.origin_data.get("field_name", ""))


class TraceTypeUpdateTaskStatus(BaseTraceType):
    """任务状态变更"""

    def __init__(self, handle_info):
        super(TraceTypeUpdateTaskStatus, self).__init__(handle_info)
        self.node_type = None
        if self.cls_name in DATA_TRACE_EVENT_DICT:
            self.node_type = DATA_TRACE_EVENT_DICT[self.cls_name]["node_type"]

    def check_sub_trace_scope(self, pred_name, sub_trace_attr):
        # 判断对应的事件是否符合
        in_scope = super(TraceTypeUpdateTaskStatus, self).check_sub_trace_scope(
            pred_name, sub_trace_attr
        )
        node_type = self.origin_data.get("node_type", None)
        self.node_type = node_type
        is_source = True if node_type and node_type.endswith("source") else False
        is_storage = True if node_type and node_type.endswith("storage") else False
        in_scope &= not is_source and not is_storage

        # 判断事件前后的状态是否符合
        before = self.differ[pred_name].get("b", None)
        after = self.differ[pred_name].get("a", None)
        in_pattern = False
        if in_scope:
            rule_before = sub_trace_attr.get("rule", {}).get("before", None)
            rule_after = sub_trace_attr.get("rule", {}).get("after", None)
            in_pattern = before == rule_before and after == rule_after
        return in_pattern

    def get_data_set_ids(self):
        """任务启停获取不同的data_set_ids

        :return:
        """
        node_config = json.loads(self.origin_data.get("node_config", "{}"))
        biz = node_config.get("bk_biz_id", "")
        table_name = node_config.get("table_name", None)
        # 分流节点
        if self.node_type == NodeType.SPLIT.value:
            data_set_ids = (
                [
                    "{}_{}".format(config_dict["bk_biz_id"], table_name)
                    for config_dict in node_config["config"]
                    if "bk_biz_id" in config_dict
                ]
                if "config" in node_config
                else []
            )
            return data_set_ids
        # 数据模型明细数据、指标、tdw jar包节点获取rt_id逻辑
        elif table_name is None and node_config.get("outputs", []):
            data_set_ids = (
                [
                    "{}_{}".format(biz, output_dict["table_name"])
                    for output_dict in node_config["outputs"]
                    if "table_name" in output_dict
                ]
                if "outputs" in node_config
                else []
            )
            return data_set_ids
        # 其他node_type类型
        if biz and table_name:
            return ["{}_{}".format(biz, table_name)]
        return []

    def get_jump_to_info(self):
        node_config = json.loads(self.origin_data.get("node_config", "{}"))
        biz_id = node_config.get("bk_biz_id", "")
        return dict(
            jump_to="flow",
            flow_id=self.origin_data.get("flow_id", None),
            bk_biz_id=biz_id,
        )

    def get_sub_type_desc(self, sub_trace_type, pred_name, diff_item):
        return dict(flow_id=self.origin_data["flow_id"])


class TraceTypeMigrate(BaseTraceType):
    """数据迁移"""

    def check_sub_trace_scope(self, pred_name, sub_trace_attr):
        # 判断对应的事件是否符合
        in_scope = super(TraceTypeMigrate, self).check_sub_trace_scope(
            pred_name, sub_trace_attr
        )
        task_type = (
            self.changed_data.get("task_type", None)
            if sub_trace_attr["event_name"] == "EventCreateNode"
            else self.origin_data.get("task_type", None)
        )
        in_scope &= task_type == "overall"

        # 判断事件前后的状态是否符合
        after = self.differ[pred_name].get("a", None)
        in_pattern = False
        if in_scope:
            rule_after = sub_trace_attr.get("rule", {}).get("after", None)
            in_pattern = after == rule_after
        return in_scope and in_pattern

    def get_data_set_ids(self):
        if "result_table_id" in self.origin_data:
            return [self.origin_data["result_table_id"]]
        if "result_table_id" in self.changed_data:
            return [self.changed_data["result_table_id"]]
        return []

    def get_jump_to_info(self):
        remote_data = (
            self.origin_data if "source" in self.origin_data else self.changed_data
        )
        # 源存储 目标存储 启止时间
        return dict(
            jump_to="migrate",
            id=self.identifier_value,
            source=remote_data.get("source", ""),
            dest=remote_data.get("dest", ""),
            start=remote_data.get("start", ""),
            end=remote_data.get("end", ""),
        )


# dgraph model和对应的变更事件mappings
model_hook_mapping = dict(
    AccessRawData=[TraceTypeUpdateAttr],
    ResultTable=[TraceTypeUpdateAttr],
    ResultTableField=[TraceTypeUpdateSchema],
    DataflowNodeInfo=[TraceTypeUpdateTaskStatus],
    DatabusMigrateTask=[TraceTypeMigrate],
)


class DataTraceHandler(object):
    def __init__(self, changed_model, data_trace_info_dict):
        self.changed_model = changed_model
        self.data_trace_info_dict = data_trace_info_dict

    def get_data_trace_event_list(self):
        """获取数据足迹事件信息"""
        data_trace_list = []
        # 变更的dgraph model对应的数据足迹事件
        hook_trace_types = model_hook_mapping.get(self.changed_model, [])
        # 针对不同事件类型的body内容，生成足迹信息
        for trace_type in hook_trace_types:
            trace_obj = trace_type(self.data_trace_info_dict)
            trace_obj.filling()
            if trace_obj.trace_info_list:
                data_trace_list.extend(trace_obj.trace_info_list)

        logger.info(
            f"get_data_trace_event_list return data_trace_list: {data_trace_list}"
        )
        return data_trace_list
