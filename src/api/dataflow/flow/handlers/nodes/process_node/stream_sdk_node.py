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

from common.local import get_request_username
from conf.dataapi_settings import BATCH_IP_RESULT_TABLE_ID
from django.utils.translation import ugettext as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import NodeError
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.handlers.nodes.base_node.base_rt_node import RTNode
from dataflow.flow.handlers.nodes.base_node.node_handler import call_func_with_rollback
from dataflow.flow.models import FlowFileUpload
from dataflow.flow.node_types import NodeTypes
from dataflow.pizza_settings import HDFS_UPLOADED_SDK_FILE_DIR
from dataflow.shared.component.component_helper import ComponentHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.shared.stream.stream_helper import StreamHelper


class StreamSDKNode(RTNode):
    default_storage_type = "hdfs"

    def add_after(self, username, from_node_ids, form_data):
        # 创建processing
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        self.processing_id = form_data["processing_name"]
        rt_params["processing_id"] = self.processing_id
        response_data = StreamHelper.create_processing(**rt_params)
        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        # 更新/删除 rt 与存储关联关系，主要进行删除操作，新增操作在第4步进行
        # 获取当前 processing 关联的 RT，与即将更新的 RT 进行比较，删除即将移除的 RT 关联关系(需检查是否被关联使用)
        rt_params = self.build_rt_params(form_data, from_node_ids, username)
        rt_params.update({"processing_id": self.processing_id})
        result_table_ids = []
        for output in rt_params["outputs"]:
            result_table_ids.append("{}_{}".format(output["bk_biz_id"], output["table_name"]))
        refresh_result_ret = self.refresh_result_table_storages(result_table_ids)
        rt_params["delete_result_tables"] = refresh_result_ret["result_table_ids_to_delete"]
        rollback_func_info_list = refresh_result_ret["rollback_func_info_list"]
        downstream_link_to_delete = refresh_result_ret["downstream_link_to_delete"]
        # 3. 更新 processing
        response_data = call_func_with_rollback(StreamHelper.update_processing, rt_params, rollback_func_info_list)

        # 移除与之相关的下游节点连线
        for to_node_id in downstream_link_to_delete:
            self.flow.delete_line(self.node_id, to_node_id)

        rts_heads_tails = {
            "result_table_ids": response_data["result_table_ids"],
            "heads": response_data["result_table_ids"],
            "tails": response_data["result_table_ids"],
        }
        return rts_heads_tails

    def remove_before(self, username):
        """
        先删processing再删存储，防止processing删除失败存储却删了
        @param username:
        @param kwargs:
        @return:
        """
        super(StreamSDKNode, self).remove_before(username)
        self.delete_multi_dp_with_rollback(StreamHelper.delete_processing, result_table_ids=self.result_table_ids)

    def build_before(self, from_node_ids, form_data, is_create=True):
        self.link_rules_check(from_node_ids, form_data, is_create)
        from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)
        if len(from_node_ids) > 0:
            source_bk_biz_ids = []
            for _n in from_nodes:
                if not (
                    (_n.node_type == NodeTypes.BATCH_SOURCE and _n.result_table_id in BATCH_IP_RESULT_TABLE_ID)
                    or (_n.node_type in NodeTypes.ALL_KV_SOURCE_CATEGORY and ResultTable(_n.result_table_id).is_public)
                ):
                    source_bk_biz_ids.append(_n.get_config(False)["bk_biz_id"])

            if len(set(source_bk_biz_ids)) > 1:
                raise Errors.NodeValidError(_("暂不支持不同业务的数据关联操作"))
        return form_data

    def _build_sdk_package(self, form_data):
        """
        校验上传待上传用户包，目前仅支持一个，若传多个则取最后一个
        @param form_data:
        @return:
        """
        file_info = None
        for jar_file_info in form_data["package"]:
            file_info = FlowFileUpload.get_uploaded_file_info(jar_file_info["id"])
            if file_info["flow_id"] != self.flow_id:
                raise NodeError(_("当前指定文件(%s)不属于当前任务，无权限使用") % file_info["name"])
            if self.component_type in ["spark_structured_streaming"] and file_info["suffix"] != "zip":
                raise Errors.NodeError(_("Spark Structured Streaming 节点仅支持上传 zip 包."))
            elif self.component_type in ["flink"] and file_info["suffix"] != "jar":
                raise Errors.NodeError(_("Flink Streaming 节点仅支持上传 jar 包."))
        if file_info:
            # 将 jar 包移到指定位置
            from_path = file_info["path"]
            to_path = "{}{}".format(
                HDFS_UPLOADED_SDK_FILE_DIR,
                "{}.{}".format(file_info["id"], file_info["suffix"]),
            )
            # move 临时文件到指定路径
            hdfs_ns_id = StorekitHelper.get_default_storage("hdfs", self.geog_area_codes[0])["cluster_group"]
            ComponentHelper.hdfs_check_to_path_before_move(hdfs_ns_id, from_path, to_path)
            # 更新文件信息
            FlowFileUpload.update_upload_file_info(file_info["id"], self.node_id, to_path, get_request_username())
            return {"path": to_path, "id": file_info["md5"], "name": file_info["name"]}
        else:
            return {}

    def build_rt_params(self, form_data, from_node_ids, username):
        """
        生成 RT 参数
        """
        (
            not_static_rt_ids,
            static_rt_ids,
            source_rt_ids,
        ) = NodeUtils.build_from_nodes_list(from_node_ids)

        # 保险起见，再校验一次
        rt_dict = self.clean_config(form_data, False)

        send_dict = {
            "programming_language": form_data["programming_language"],
            "user_args": form_data["user_args"],
            "advanced": form_data["advanced"],
        }
        if self.node_type == NodeTypes.SPARK_STRUCTURED_STREAMING:
            send_dict.update(
                {
                    "user_main_class": form_data["user_main_class"],
                    "package": self._build_sdk_package(rt_dict),
                    "code": form_data["code"],
                }
            )
        elif self.node_type == NodeTypes.FLINK_STREAMING:
            send_dict.update({"code": form_data["code"]})

        for output in form_data["outputs"]:
            output["result_table_name"] = output["table_name"]
            output["result_table_name_alias"] = output["output_name"]
        rt_params = {
            "project_id": self.flow.project_id,
            "bk_biz_id": form_data["bk_biz_id"],
            "component_type": self.component_type,
            "implement_type": "code",
            "processing_alias": form_data["name"],
            "input_result_tables": not_static_rt_ids,
            "static_data": static_rt_ids,
            "source_data": source_rt_ids,
            "dict": send_dict,
            "outputs": form_data["outputs"],
            "tags": self.geog_area_codes,
        }
        return rt_params

    @property
    def processing_type(self):
        return "stream"

    @property
    def component_type(self):
        """
        @return:
        """
        component_type = None
        if self.node_type == NodeTypes.SPARK_STRUCTURED_STREAMING:
            component_type = "spark_structured_streaming"
        elif self.node_type == NodeTypes.FLINK_STREAMING:
            component_type = "flink"
        return component_type
