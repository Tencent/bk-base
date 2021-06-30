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
import logging
import time

import gevent

from api import datamanage_api, meta_api


class MetaCacheMixin(object):
    def __init__(self, *args, **kwargs):
        super(MetaCacheMixin, self).__init__(*args, **kwargs)

    def refresh_metadata(
        self, metadata, func, args=None, default=None, update=True, callback=None
    ):
        """刷新缓存中的元数据

        :param metadata 目标缓存的元数据
        :param func 刷新缓存的函数
        :param args 刷新缓存函数的参数
        :param default 默认元数据
        :param update 是否只更新缓存的元数据
        :param callback 更新完元数据后的回调函数
        """
        args = args or {}
        default = default or {}

        try:
            result = func(**args)
            if not update:
                self.clear_metadata(metadata)

            self.update_metadata(metadata, result)
            logging.info("Finish to refresh metadata by function(%s)" % str(func))
        except Exception as e:
            logging.error(
                "Failed to refresh metadata by function(%s) and callback(%s), error: %s"
                % (
                    str(func),
                    str(callback),
                    str(e),
                )
            )
            if not update:
                self.clear_metadata(metadata)

        if callable(callback):
            callback(metadata)

    def clear_metadata(self, metadata):
        """清理元数据

        :param metadata 缓存的元数据
        """
        if isinstance(metadata, dict):
            metadata.clear()
        elif isinstance(metadata, list):
            del metadata[:]

    def update_metadata(self, metadata, value):
        """更新元数据

        :param metadata 缓存的元数据
        :param value 需要替代的元数据新值
        """
        if isinstance(metadata, dict):
            metadata.update(value)
        elif isinstance(metadata, list):
            metadata.extend(value)

    def check_metadata(self, metadata, timeout=60, name=None):
        """检测是否有缓存的元数据

        :param metadata 待检测的元数据
        :param timeout 超时时间
        :param name 缓存名称
        """
        start_time = None
        while len(metadata) == 0:
            if start_time is None:
                start_time = time.time()
            logging.info("Waiting to get metadata about {name}".format(name=name))
            gevent.sleep(1)
            if time.time() - start_time > timeout:
                raise Exception(
                    "Waiting to get metadata about {name} timed out".format(name=name)
                )

    def fetch_biz_infos(self):
        """获取业务信息"""
        biz_infos = {}
        try:
            res = meta_api.bizs.list()
            bizs = res.data if res.is_success() else []
            for biz_info in bizs:
                bk_biz_id = str(biz_info.get("bk_biz_id"))
                if bk_biz_id:
                    biz_infos[bk_biz_id] = biz_info
        except Exception as e:
            logging.error(e, exc_info=True)
        return biz_infos

    def fetch_project_infos(self):
        """获取项目信息"""
        project_infos = {}
        try:
            res = meta_api.projects.list()
            projects = res.data if res.is_success() else []
            for project_info in projects:
                project_id = str(project_info.get("project_id"))
                if project_id:
                    project_infos[project_id] = project_info
        except Exception as e:
            logging.error(e, exc_info=True)
        return project_infos

    def fetch_event_types(self):
        """获取事件类型信息"""
        try:
            res = datamanage_api.event_types.list()
            return res.data if res.is_success() else []
        except Exception as e:
            logging.error(e, exc_info=True)
            return []

    def fetch_notify_configs(self):
        """获取通知配置信息"""
        try:
            res = datamanage_api.notify_configs()
            return res.data if res.is_success() else []
        except Exception as e:
            logging.error(e, exc_info=True)
            return []

    def fetch_data_set_infos(self, update_duration=None):
        """获取数据集信息

        :param udpate_duration 最近有更新的数据集时间周期范围
        """
        try:
            params = {}
            if update_duration:
                params["update_duration"] = update_duration
            res = datamanage_api.dmonitor_data_sets.list(params)
            return res.data if res.is_success() else {}
        except Exception as e:
            logging.error(e, exc_info=True)
            return {}

    def fetch_data_set_by_id(self, data_set_id):
        """获取单个data_set信息"""
        data_set = {}
        try:
            res = datamanage_api.dmonitor_data_sets.retrieve(
                {"data_set_id": data_set_id}
            )
            data_set = res.data if res.is_success() else {}
        except Exception as e:
            logging.error(e, exc_info=True)
        return data_set

    def fetch_data_operations(
        self, with_status=True, with_relations=True, with_node=True
    ):
        """获取数据处理和数据传输的元数据信息"""
        data_operations = {}
        try:
            res = datamanage_api.dmonitor_data_operations.list(
                {
                    "with_relations": with_relations,
                    "with_status": with_status,
                    "with_node": with_node,
                }
            )
            data_operations = res.data if res.is_success() else {}
        except Exception as e:
            logging.error(e, exc_info=True)
        return data_operations

    def fetch_sampling_result_tables(self, heat_score, heat_rate, recent_data_time):
        """获取待采样的结果表信息

        :param heat_score: 热度评分阈值
        :param heat_rate: 热度顺序下数据集比例
        :param recent_data_time: 最近有数据的数据集的时间范围
        """
        try:
            res = datamanage_api.sampling_result_tables(
                {
                    "heat_score": heat_score,
                    "heat_rate": heat_rate,
                    "recent_data_time": recent_data_time,
                }
            )
            return res.data if res.is_success() else []
        except Exception as e:
            logging.error(e, exc_info=True)
            return []

    def fetch_flow_infos(self, with_nodes=False):
        """获取数据流相关的任务信息"""
        flow_infos = {}
        try:
            res = datamanage_api.dmonitor_flows.list({"with_nodes": with_nodes})
            flows = res.data if res.is_success() else []
            for flow_info in flows:
                flow_id = flow_info.get("flow_id")
                if flow_id:
                    flow_infos[str(flow_id)] = flow_info
        except Exception as e:
            logging.error(e, exc_info=True)
        return flow_infos

    def fetch_dataflow_infos(self):
        """获取运行中的dataflow任务配置及其节点信息"""
        flow_infos = {}
        try:
            res = datamanage_api.dmonitor_flows.dataflow(
                {"with_nodes": True, "only_running": True}
            )
            flows = res.data if res.is_success() else []
            for flow_info in flows:
                flow_id = flow_info.get("flow_id")
                if flow_id:
                    flow_infos[flow_id] = flow_info
        except Exception as e:
            logging.error(e, exc_info=True)
        return flow_infos
