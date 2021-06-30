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
import time
from datetime import datetime

from django.utils.translation import ugettext as _

from dataflow.flow.exceptions import NodeError
from dataflow.flow.handlers import forms
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.node_handler import call_func_with_rollback
from dataflow.flow.handlers.nodes.process_node.base_batch_node import BaseBatchNode
from dataflow.flow.handlers.offline_utils import valid_start_time, validate_batch_count_freq_and_delay
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.utils.count_freq import CountFreq
from dataflow.shared.batch.batch_helper import BatchHelper


class DataModelIndicatorOfflineNode(BaseBatchNode):
    default_storage_type = "hdfs"
    node_type = NodeTypes.DATA_MODEL_BATCH_INDICATOR
    node_form = forms.FlowProcessingNodeStandardForm
    config_attrs = [
        "bk_biz_id",
        "processing_name",
        "name",
        "outputs",
        "from_nodes",
        "window_config",
        "dedicated_config",
    ]

    def add_after(self, username, from_node_ids, form_data):
        # 参数对应
        window_config = form_data["window_config"]
        valid_start_time(None, window_config, True)
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(self.node_type, from_node_ids, form_data)
        self.check_upstream_default_expires(window_config, upstream_nodes_info)

        # 创建模型实例指标
        model_indicator_params = self.build_create_model_indicator_params(form_data, from_node_ids)
        model_indicator_response = self.create_data_model_indicator(model_indicator_params)
        form_data["dedicated_config"]["data_model_sql"] = model_indicator_response["data_model_sql"]
        form_data["window_config"] = model_indicator_response["scheduling_content"]

        # 2. 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params["processing_id"] = self.build_processing_id(
            form_data["bk_biz_id"], form_data["outputs"][0]["table_name"]
        )
        result_table_id = "{}_{}".format(
            form_data["bk_biz_id"],
            form_data["outputs"][0]["table_name"],
        )

        # 创建processing，失败则回滚
        create_dp_output_rollbacks = [
            {
                "func": self.delete_dp_with_rollback,
                "params": {"delete_func": BatchHelper.delete_processing},
            },
            {
                "func": self.rollback_data_model_indicator,
                "params": {
                    "model_instance_id": model_indicator_response["model_instance_id"],
                    "rollback_id": model_indicator_response["rollback_id"],
                    "result_table_id": result_table_id,
                },
            },
        ]
        response_data = call_func_with_rollback(BatchHelper.create_processing, rt_params, create_dp_output_rollbacks)

        current_node_config = self.get_config(False)
        current_node_config["dedicated_config"]["model_instance_id"] = model_indicator_response["model_instance_id"]
        current_node_config["dedicated_config"]["data_model_sql"] = model_indicator_response["data_model_sql"]
        current_node_config["dedicated_config"]["result_table_id"] = result_table_id
        self.node_info.node_config = json.dumps(current_node_config)
        self.node_info.save()

        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        window_config = form_data["window_config"]
        upstream_nodes_info = NodeUtils.get_upstream_nodes_info(self.node_type, from_node_ids, form_data)
        upstream_system_storage_rts = self.check_upstream_default_expires(window_config, upstream_nodes_info)
        # 校验当前节点的数据窗口长度是否小于下游 HDFS 存储节点的过期时间
        self.check_downstream_expires(window_config)
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params.update({"processing_id": self.processing_id})
        result_table_id = ("{}_{}".format(form_data["bk_biz_id"], form_data["outputs"][0]["table_name"]),)

        update_dp_rollbacks = [
            {
                "func": self.rollback_data_model_indicator,
                "params": {
                    "model_instance_id": form_data["dedicated_config"]["model_instance_id"],
                    "rollback_id": form_data["dedicated_config"]["rollback_id"],
                    "result_table_id": result_table_id,
                },
            }
        ]
        response_data = call_func_with_rollback(BatchHelper.update_processing, rt_params, update_dp_rollbacks)

        # 校验是否需要更新上游离线节点过期时间
        self.update_upstream_default_expires(window_config, upstream_system_storage_rts, username)
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def remove_before(self, username):
        super(DataModelIndicatorOfflineNode, self).remove_before(username)
        current_config = self.get_config(True)
        result_table_id = "{}_{}".format(
            current_config["bk_biz_id"],
            current_config["outputs"][0]["table_name"],
        )
        del_data_model_response = self.del_data_model_indicator(
            current_config.get("dedicated_config").get("model_instance_id"),
            result_table_id,
        )
        delete_dp_rollbacks = [
            {
                "func": self.rollback_data_model_indicator,
                "params": {
                    "model_instance_id": del_data_model_response["model_instance_id"],
                    "result_table_id": del_data_model_response["result_table_id"],
                    "rollback_id": del_data_model_response["rollback_id"],
                },
            }
        ]
        call_func_with_rollback(
            self.delete_dp_with_rollback,
            BatchHelper.delete_processing,
            delete_dp_rollbacks,
        )

    def build_before(self, from_node_ids, form_data, is_create=True):
        form_data = super(DataModelIndicatorOfflineNode, self).build_before(from_node_ids, form_data, is_create)
        from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)
        # from_batch_nodes = NodeUtils.filter_batch(from_nodes)
        validate_batch_count_freq_and_delay(
            CountFreq(
                form_data["window_config"]["count_freq"],
                form_data["window_config"]["schedule_period"],
            ),
            from_nodes,
        )

        if not is_create:
            # 如果是更新操作，则需取到原有的 node_config 中的 model_instance_id 参数
            # 然后填充新的更新参数并更新，返回新的 data_model_sql 和 rollback_id
            current_node_config = self.get_config(False)
            if (
                "dedicated_config" in current_node_config
                and "model_instance_id" in current_node_config["dedicated_config"]
                and current_node_config["dedicated_config"]["model_instance_id"]
            ):
                # 更新数据模型指标
                model_indicator_params = self.build_update_model_indicator_params(form_data, from_node_ids)
                model_indicator_params["model_instance_id"] = current_node_config["dedicated_config"][
                    "model_instance_id"
                ]
                model_indicator_response = self.update_data_model_indicator(model_indicator_params)
                form_data["dedicated_config"]["model_instance_id"] = model_indicator_response["model_instance_id"]
                form_data["dedicated_config"]["data_model_sql"] = model_indicator_response["data_model_sql"]
                form_data["dedicated_config"]["rollback_id"] = model_indicator_response["rollback_id"]

        return form_data

    def build_rt_params(self, form_data, from_node_ids, username):
        """
        生成 RT 参数
        """
        rt_dict = form_data
        # 使用 output_name 作为最终结果表的中文名
        rt_dict["name"] = rt_dict["outputs"][0]["output_name"]

        from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)

        tables = {}

        # 将启动时间固定下来，默认从当前0点开始
        rt_dict["schedule_time"] = datetime.now().strftime("%Y%m%d000000")

        window_config = rt_dict["window_config"]
        # 参数补全，和之前离线节点的差异
        if window_config["window_type"] == "accumulate":
            window_config["accumulate"] = True
        else:
            window_config["accumulate"] = False
        window_config["advanced"]["self_dependency"] = False
        window_config["advanced"]["self_dependency_config"] = {}
        # 数据开始时间结束时间设置
        data_start = window_config["data_start"] if window_config["data_start"] is not None else -1
        data_end = window_config["data_end"] if window_config["data_end"] is not None else -1
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
            if _from_node.node_type == NodeTypes.BATCH_KV_SOURCE:
                static_rt_ids.extend(_table_ids)
                continue
            for _table_id in _table_ids:
                # _type = self.get_table_type_from_node(_from_node)
                if window_config["accumulate"]:
                    delay = 0 if window_config["delay"] is None else window_config["delay"]
                    tables[_table_id] = window_config["unified_config"]
                else:
                    # 是否启用父表差异化配置
                    delay = (
                        window_config["fixed_delay"] * 24
                        if window_config["delay_period"] == "day"
                        else window_config["fixed_delay"]
                    )
                    if window_config["dependency_config_type"] == "custom":
                        is_custom_dependency_config = True
                        tables = window_config["custom_config"]
                    else:
                        tables[_table_id] = window_config["unified_config"]
                        tables[_table_id]["window_delay"] = 0
        # 自定义配置的父表需要确保每个父 RT 都可归属于非关联数据源节点的输出 RT，否则就是不合法的 RT
        if is_custom_dependency_config:
            invalid_from_rts = set(window_config["custom_config"].keys()) - set(self.from_result_table_ids)
            if invalid_from_rts:
                raise NodeError(_("自定义配置父表不可使用静态关联表：%s") % ",".join(invalid_from_rts))

        # TODO：目前一个processing只有一个输出
        outputs = [
            {
                "bk_biz_id": rt_dict["bk_biz_id"],
                "table_name": rt_dict["outputs"][0]["table_name"],
            }
        ]
        # 获取cluster_name
        # TODO: 目前只有一个输出
        result_table_id = "{}_{}".format(
            rt_dict["bk_biz_id"],
            rt_dict["outputs"][0]["table_name"],
        )
        cluster_name = NodeUtils.get_storage_cluster_name(
            self.project_id, self.default_storage_type, result_table_id=result_table_id
        )
        send_dict = {
            "description": rt_dict["outputs"][0]["output_name"],
            "accumulate": window_config["accumulate"],
            "count_freq": window_config["count_freq"],
            "schedule_period": window_config["schedule_period"],
            "delay": 0 if delay is None else delay,  # 固定窗口非窗口差异化配置不会取这个值作为延迟时间
            "data_start": data_start,
            "data_end": data_end,
            "cluster": cluster_name,
            "batch_type": self.batch_type,
        }
        # 若有高级配置
        if window_config["advanced"]:
            # 允许启动时间为空
            if "start_time" in window_config["advanced"] and window_config["advanced"]["start_time"]:
                start_time = window_config["advanced"]["start_time"]
                time_array = time.strptime(start_time, "%Y-%m-%d %H:%M")
                _start_time = int(time.mktime(time_array) * 1000)
                window_config["advanced"]["start_time"] = _start_time
            else:
                window_config["advanced"]["start_time"] = None
            send_dict["advanced"] = window_config["advanced"]
        # 增加数据修正逻辑, 替换sql
        sql = form_data["dedicated_config"]["data_model_sql"]
        rt_params = {
            "project_id": self.flow.project_id,
            "sql": sql,
            "dict": send_dict,
            "result_tables": tables,
            "static_data": static_rt_ids,
            "outputs": outputs,
            "tags": self.geog_area_codes,
        }
        return rt_params

    def build_create_model_indicator_params(self, rt_dict, from_node_ids):
        """
        生成模型实例参数
        """
        rt_params = rt_dict["dedicated_config"]
        rt_params["scheduling_content"] = rt_dict["window_config"]
        # 加上额外的节点信息
        rt_params["bk_biz_id"] = rt_dict["bk_biz_id"]
        rt_params["result_table_id"] = "{}_{}".format(
            rt_dict["bk_biz_id"],
            rt_dict["outputs"][0]["table_name"],
        )
        rt_params["parent_result_table_id"] = self.from_result_table_ids[0]
        rt_params["flow_node_id"] = self.node_id
        return rt_params

    def build_update_model_indicator_params(self, rt_dict, from_node_ids):
        """
        生成模型实例参数
        """
        rt_params = rt_dict["dedicated_config"]
        rt_params["scheduling_content"] = rt_dict["window_config"]
        rt_params["result_table_id"] = "{}_{}".format(
            rt_dict["bk_biz_id"],
            rt_dict["outputs"][0]["table_name"],
        )
        rt_params["parent_result_table_id"] = self.from_result_table_ids[0]
        return rt_params

    @property
    def processing_type(self):
        return "batch"
