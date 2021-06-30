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

from dataflow.shared.api.modules.databus import DatabusApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.send_message import send_system_message


class DatabusHelper(object):
    @staticmethod
    def get_task(result_table_id):
        request_params = {"result_table_id": result_table_id}
        res = DatabusApi.tasks.retrieve(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_task(result_table_id, storages=None):
        request_params = {"result_table_id": result_table_id}
        if storages:
            request_params["storages"] = storages
        res = DatabusApi.tasks.create(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def stop_task(result_table_id, storages=None):
        request_params = {"result_table_id": result_table_id}
        if storages:
            check_storages = []
            # 为保证任务可被正常停止和删除，这里再加一次验证，保证对应rt的确拥有该存储
            rt_storages = ResultTableHelper.get_result_table_storage(result_table_id)
            for storage in storages:
                if storage in rt_storages:
                    check_storages.append(storage)
                else:
                    logger.warning(
                        "[flow_databus] ignore non-existent storage({}) for rt_id({})".format(result_table_id, storage)
                    )
            if check_storages:
                request_params["storages"] = check_storages
            else:
                # 若没有，忽略之，发送告警消息
                content = "【警告】尝试停止分发进程失败，结果表 {} 关联存储不存在: {}, 操作人员: {}".format(
                    result_table_id,
                    ", ".join(storages),
                    get_request_username(),
                )
                logger.exception(content)
                send_system_message(content, False)
                return None
        res = DatabusApi.tasks.delete(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_inner_kafka_channel_info(result_table_id):
        """
        获取inner类型的kafka channel
        @return:
        """
        request_params = {"result_table_id": result_table_id}
        res = DatabusApi.channels.inner_to_use(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_result_table_data_tail(result_table_id):
        """
        获取rt最近的几条数据
        @return:
        """
        request_params = {"result_table_id": result_table_id}
        res = DatabusApi.result_tables.tail(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_channel_info(channel_id):
        res = DatabusApi.channels.retrieve({"channel_id": channel_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_channel_info_by_cluster_name(cluster_name):
        res = DatabusApi.channels_by_cluster_name.retrieve({"cluster_name": cluster_name})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def list_channel_info(tags):
        request_params = {"tags": tags}
        res = DatabusApi.channels.list(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_component_info_by_rt(result_table_ids):
        """
        根据rt列表获取component信息
        @param result_table_ids:
        @return:
        {
            "591_durant1115": {
                "hdfs": {
                    "component": "testinner",
                    "module": "hdfs"
                },
                "kafka": {
                    "component": "testouter",
                    "module": "clean"
                }
            },
            "591_node1": {
                "tspider": {
                    "component": "testinner",
                    "module": "tspider"
                }
            }
        }
        """
        request_params = {"result_table_id": result_table_ids}
        res = DatabusApi.tasks.component(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_datanode(
        project_id,
        bk_biz_id,
        processing_id,
        node_type,
        result_table_name,
        result_table_name_alias,
        source_result_table_ids,
        config,
        **kwargs
    ):
        kwargs.update(
            {
                "project_id": project_id,
                "bk_biz_id": bk_biz_id,
                "processing_id": processing_id,
                "node_type": node_type,
                "result_table_name": result_table_name,
                "result_table_name_alias": result_table_name_alias,
                "source_result_table_ids": source_result_table_ids,
                "config": config,
            }
        )
        res = DatabusApi.datanodes.create(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_datanode(
        project_id,
        bk_biz_id,
        processing_id,
        node_type,
        result_table_name,
        result_table_name_alias,
        source_result_table_ids,
        config,
        **kwargs
    ):
        kwargs.update(
            {
                "project_id": project_id,
                "bk_biz_id": bk_biz_id,
                "processing_id": processing_id,
                "node_type": node_type,
                "result_table_name": result_table_name,
                "result_table_name_alias": result_table_name_alias,
                "source_result_table_ids": source_result_table_ids,
                "config": config,
            }
        )
        res = DatabusApi.datanodes.partial_update(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_datanode(processing_id, with_data=False, delete_result_tables=[]):
        res = DatabusApi.datanodes.delete(
            {
                "processing_id": processing_id,
                "delete_result_tables": delete_result_tables,
                "with_data": with_data,
            }
        )
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_datanode_task(result_table_id):
        res = DatabusApi.tasks.datanode({"result_table_id": result_table_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def refresh_datanode(result_table_id):
        res = DatabusApi.datanodes.refresh({"processing_id": result_table_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_cleans(result_table_id):
        request_params = {"result_table_id": result_table_id}
        res = DatabusApi.cleans.retrieve(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_partition_num(result_table_id):
        res = DatabusApi.result_tables.partitions({"result_table_id": result_table_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_raw_data_id(processing_id):
        """
        获取clean(processing_id)的raw_data_id
        @return:
        """
        request_params = {"result_table_id": processing_id}
        res = DatabusApi.cleans.retrieve(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_clean_list(raw_data_id):
        """
        获取raw_data_id的clean任务列表
        @return:
        """
        request_params = {"raw_data_id": raw_data_id}

        # 根据raw_data_id获取清洗列表
        res = DatabusApi.cleans.list(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_task_components(result_table_id):
        """
        获取result_table_id的所有分发任务（含停止/删除过的）
        @return:
        """
        request_params = {"result_table_id": result_table_id}
        res = DatabusApi.tasks.component.retrieve(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_running_task_components(result_table_id):
        """
        获取result_table的正在运行中的分发任务类型
        @return:
        """
        request_params = {"result_table_id": result_table_id}
        res = DatabusApi.tasks.retrieve(request_params)
        res_util.check_response(res)
        ret = []
        for one_res in res.data:
            if one_res["status"] == "running":
                ret.append(one_res["sink_type"])
        return ret

    @staticmethod
    def stop_shipper(raw_data_id, result_table_id, cluster_type):
        """
        停止分发任务
        @return:
        """
        request_params = {
            "raw_data_id": raw_data_id,
            "result_table_id": result_table_id,
            "cluster_type": cluster_type,
        }
        res = DatabusApi.scenarios.stop_shipper(request_params)
        res_util.check_response(res)
        return res

    @staticmethod
    def get_raw_data_partitions(raw_data_id):
        """
        获取raw_data分区数
        @return:
        """
        request_params = {"raw_data_id": raw_data_id}
        res = DatabusApi.rawdatas.partitions(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def set_raw_data_partitions(raw_data_id, partitions):
        """
        设置raw_data分区数
        @return:
        """
        request_params = {"partitions": partitions, "raw_data_id": raw_data_id}
        res = DatabusApi.rawdatas.set_partitions(request_params)
        res_util.check_response(res)
        return res

    @staticmethod
    def get_result_table_partitions(result_table_id):
        """
        获取result_table分区数
        @return:
        """
        request_params = {"result_table_id": result_table_id}
        res = DatabusApi.result_tables.partitions(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def set_result_table_partitions(result_table_id, partitions):
        """
        设置result_table分区数
        @return:
        """
        request_params = {"partitions": partitions, "result_table_id": result_table_id}
        res = DatabusApi.result_tables.set_partitions(request_params)
        res_util.check_response(res)
        return res

    @staticmethod
    def get_json_conf_value(conf_key):
        res = DatabusApi.admin.json_conf_value({"conf_key": conf_key})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_data_storages(result_table_id, cluster_name, cluster_type, expires, storage_config, **kwargs):
        """
        @param result_table_id:
        @param cluster_name:
        @param cluster_type:
        @param expires:
        @param storage_config:
        @param kwargs:  可包含generate_type，可选user|system, 默认user
        @return:
        """
        kwargs.update(
            {
                "result_table_id": result_table_id,
                "cluster_name": cluster_name,
                "cluster_type": cluster_type,
                "expires": expires,
                "storage_config": storage_config,
                "description": "DataFlow增加存储",
            }
        )
        res = DatabusApi.data_storages_advanced.create(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_data_storages(result_table_id, cluster_name, cluster_type, **kwargs):
        """
        以下为必须参数
        @param result_table_id:
        @param cluster_name:
        @param cluster_type:
        @param kwargs:
        @return:
        """
        kwargs.update(
            {
                "result_table_id": result_table_id,
                "cluster_name": cluster_name,
                "cluster_type": cluster_type,
            }
        )
        res = DatabusApi.data_storages_advanced.update(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_data_storages(result_table_id, cluster_type, channel_mode, ignore_channel=False):
        """
        只有一类场景，ignore_channel=True
                    flink_code
        stream ->
                    tspider
        删除 tspider 的时候，kafka 有其它链路使用
        """
        res = DatabusApi.data_storages_advanced.delete(
            {
                "result_table_id": result_table_id,
                "cluster_type": cluster_type,
                "channel_mode": channel_mode,
                "ignore_channel": ignore_channel,
            }
        )
        res_util.check_response(res)
        return res.data
