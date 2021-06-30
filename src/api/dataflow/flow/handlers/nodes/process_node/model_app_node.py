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

import time
from datetime import datetime

from django.utils.translation import ugettext as _

from dataflow.flow.exceptions import NodeError
from dataflow.flow.handlers import forms
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.process_node.base_batch_node import BaseBatchNode
from dataflow.flow.handlers.offline_utils import valid_start_time, validate_batch_count_freq_and_delay
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.utils.count_freq import CountFreq
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.modeling.modeling_helper import ModelingHelper


class ModelAppNode(BaseBatchNode):
    default_storage_type = "hdfs"
    node_type = NodeTypes.MODEL_APP
    node_form = forms.model_app_node_form
    config_attrs = [
        "bk_biz_id",
        "input_model",
        "model_params",
        "model_config",
        "model_version",
        "enable_auto_update",
        "update_policy",
        "update_time",
        "processing_name",
        "table_name",
        "name",
        "window_type",
        "accumulate",
        "parent_tables",
        "schedule_period",
        "count_freq",
        "fallback_window",
        "fixed_delay",
        "delay_period",
        "data_start",
        "data_end",
        "delay",
        "from_nodes",
        "output_name",
        "outputs",
        "from_result_table_ids",
        "dependency_config_type",
        "unified_config",
        "custom_config",
        "advanced",
    ]

    def add_after(self, username, from_node_ids, form_data):
        valid_start_time(None, form_data, True)
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(self.node_type, from_node_ids, form_data)
        self.check_upstream_default_expires(form_data, upstream_nodes_info)
        # 2. 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        response_data = ModelingHelper.create_multi_processings(**rt_params)
        self.processing_id = response_data["processing_id"]
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(self.node_type, from_node_ids, form_data)
        upstream_system_storage_rts = self.check_upstream_default_expires(form_data, upstream_nodes_info)
        # 校验当前节点的数据窗口长度是否小于下游 HDFS 存储节点的过期时间
        self.check_downstream_expires(form_data)
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params.update({"processing_id": self.processing_id})
        response_data = ModelingHelper.update_multi_processings(**rt_params)
        # 校验是否需要更新上游离线节点过期时间
        self.update_upstream_default_expires(form_data, upstream_system_storage_rts, username)
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def remove_before(self, username):
        super(ModelAppNode, self).remove_before(username)
        self.delete_dp_with_rollback(ModelingHelper.delete_multi_processings)

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(ModelAppNode, self).build_before(from_node_ids, form_data, is_create)
        from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)
        # from_batch_and_model_app_nodes = NodeUtils.filter_batch(from_nodes)
        validate_batch_count_freq_and_delay(
            CountFreq(form_data["count_freq"], form_data["schedule_period"]), from_nodes
        )
        return form_data

    def build_rt_params(self, form_data, from_node_ids, username):
        """
        生成 RT 参数
        """
        rt_dict = form_data
        # # 使用 output_name 作为最终结果表的中文名
        # rt_dict['name'] = rt_dict['output_name']
        if not rt_dict["from_nodes"]:
            logger.warning(
                "from_nodes(%s) is not null, replace the parent nodes from node links(%s)"
                % (from_node_ids, list(rt_dict["from_nodes"].keys()))
            )
            from_node_ids = list(rt_dict["from_nodes"].keys())

        from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)

        dependence_parent_tables = {}

        # 将启动时间固定下来，默认从当前0点开始
        rt_dict["schedule_time"] = datetime.now().strftime("%Y%m%d000000")

        # 数据开始时间结束时间设置
        data_start = rt_dict["data_start"] if rt_dict["data_start"] is not None else -1
        data_end = rt_dict["data_end"] if rt_dict["data_end"] is not None else -1
        delay = 0

        is_custom_dependency_config = False
        static_rt_ids = []
        for _from_node in from_nodes:
            # 取出父表
            _table_ids = NodeUtils.get_from_result_table_ids(
                self.node_type,
                self.get_config(False),
                specified_from_node_id=_from_node.node_id,
            )
            if _from_node.node_type in NodeTypes.BATCH_KV_SOURCE_CATEGORY:
                static_rt_ids.extend(_table_ids)
                continue
            for _table_id in _table_ids:
                # _type = self.get_table_type_from_node(_from_node)
                if rt_dict["accumulate"]:
                    delay = 0 if rt_dict["delay"] is None else rt_dict["delay"]
                    dependence_parent_tables[_table_id] = rt_dict["unified_config"]
                    dependence_parent_tables[_table_id]["window_type"] = "accumulate"
                else:
                    # 是否启用父表差异化配置
                    delay = rt_dict["fixed_delay"] * 24 if rt_dict["delay_period"] == "day" else rt_dict["fixed_delay"]
                    if rt_dict["dependency_config_type"] == "custom":
                        is_custom_dependency_config = True
                        dependence_parent_tables = rt_dict["custom_config"]
                    else:
                        dependence_parent_tables[_table_id] = rt_dict["unified_config"]
                        dependence_parent_tables[_table_id]["window_delay"] = 0
                    dependence_parent_tables[_table_id]["window_type"] = "fixed"
        # 自定义配置的父表需要确保每个父 RT 都可归属于非关联数据源节点的输出 RT，否则就是不合法的 RT
        if is_custom_dependency_config:
            invalid_from_rts = set(rt_dict["custom_config"].keys()) - set(self.from_result_table_ids)
            if invalid_from_rts:
                raise NodeError(_("自定义配置父表不可使用静态关联表：%s") % ",".join(invalid_from_rts))

        output_params = {}
        for one_output in rt_dict["outputs"]:
            # 拼接output result table id
            output_result_table_id = "{}_{}".format(
                one_output["bk_biz_id"],
                one_output["table_name"],
            )

            # 拼接字段列表
            all_field_list = []
            field_index = 0
            for one_field in one_output["fields"]:
                one_field_dict = {
                    "is_enabled": 1,
                    "field_type": one_field["field_type"],
                    "field_alias": one_field["field_alias"],
                    "is_dimension": False,
                    "field_name": one_field["field_name"],
                    "field_index": field_index,
                }
                all_field_list.append(one_field_dict)
                field_index = field_index + 1

            # 拼接output_params
            output_params[output_result_table_id] = {
                "table_alias": one_output["output_name"],
                "fields": all_field_list,
            }

        # 组装schedule_info
        schedule_info = {
            "accumulate": rt_dict["accumulate"],
            "count_freq": rt_dict["count_freq"],
            "schedule_period": rt_dict["schedule_period"],
            "delay": 0 if delay is None else delay,  # 固定窗口非窗口差异化配置不会取这个值作为延迟时间
            "data_start": data_start,
            "data_end": data_end,
        }
        # 若有高级配置
        if rt_dict["advanced"]:
            # 允许启动时间为空
            if "start_time" in rt_dict["advanced"] and rt_dict["advanced"]["start_time"]:
                start_time = rt_dict["advanced"]["start_time"]
                time_array = time.strptime(start_time, "%Y-%m-%d %H:%M")
                _start_time = int(time.mktime(time_array) * 1000)
                rt_dict["advanced"]["start_time"] = _start_time
            else:
                rt_dict["advanced"]["start_time"] = None
            schedule_info["advanced"] = rt_dict["advanced"]

        # 获取cluster_name
        # TODO: 目前只有一个输出
        result_table_id = "{}_{}".format(
            rt_dict["bk_biz_id"],
            rt_dict["outputs"][0]["table_name"],
        )
        cluster_name = (
            NodeUtils.get_storage_cluster_name(
                self.project_id,
                self.default_storage_type,
                result_table_id=result_table_id,
            ),
        )
        # 组装 extra_info
        extra_info = {
            "static_data": static_rt_ids,
            "cluster": cluster_name,
            "batch_type": self.batch_type,
        }

        deployed_model_info = {}
        if "model_params" in form_data:
            deployed_model_info["model_params"] = form_data["model_params"]
        if "input_model" in form_data:
            deployed_model_info["input_model"] = form_data["input_model"]
        if "model_version" in form_data:
            deployed_model_info["model_version"] = form_data["model_version"]
        if "enable_auto_update" in form_data:
            deployed_model_info["enable_auto_update"] = form_data["enable_auto_update"]
            if form_data["enable_auto_update"]:
                deployed_model_info["auto_update"] = {}
                if "update_policy" in form_data:
                    deployed_model_info["auto_update"]["update_policy"] = form_data["update_policy"]
                if "update_time" in form_data:
                    deployed_model_info["auto_update"]["update_time"] = form_data["update_time"]

        # 组装 submit_args
        submit_args = {
            "dependence": dependence_parent_tables,
            "node": {
                "schedule_info": schedule_info,
                "deployed_model_info": deployed_model_info,
                # 'model_params': form_data['model_params'],
                "output": output_params,
                "extra_info": extra_info,
            },
        }

        rt_params = {
            "bk_biz_id": rt_dict["bk_biz_id"],
            "project_id": self.flow.project_id,
            "model_config": rt_dict["model_config"],
            "submit_args": submit_args,
            "use_scenario": "dataflow",
            "tags": self.geog_area_codes,
        }
        return rt_params

    @property
    def batch_type(self):
        return "spark_mllib"

    @property
    def processing_type(self):
        return "model_app"
