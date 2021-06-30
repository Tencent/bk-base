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
import logging
import time
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, List

from api import cmdb_api
from collection.cmdb.base import CMDBBaseCollector, CMDBResourceWatcher, Signal
from collection.conf import constants

logger = logging.getLogger(__name__)


class CMDBHostInstCollector(CMDBBaseCollector):
    """
    cmdb 主机信息采集，将 relation 信息进行合并处理
    """

    object_type = "host"
    fields = constants.CMDB_HOST_FIELDS
    key_name = constants.CMDB_HOST_PK_NAME

    relation_object_type = "host_relation"
    relation_fields = constants.CMDB_RELATION_FIELDS

    def __init__(self, config: dict, init_producer=True):
        super(CMDBHostInstCollector, self).__init__(config, init_producer=init_producer)

    def batch_report(self):
        """
        全量获取上报
        """
        bk_biz_ids = self.bk_biz_ids
        biz_count = len(bk_biz_ids)
        logger.info(f"[CMDB HOST] Search {biz_count} business from CMDB.")

        # 并发度不能太大，太大会导致 API 调用超限
        pool = ThreadPoolExecutor(constants.POOL_SIZE)
        futures = [
            pool.submit(self.batch_report_one_by_one, bk_biz_id)
            for bk_biz_id in bk_biz_ids
        ]

        count = 0
        for future in as_completed(futures):
            # 目前该函数按照正常逻辑不会报错
            future.result()

            count += 1
            logger.info(f"Now, succeed to batch report {count}/{biz_count} business")

    def batch_report_one_by_one(self, bk_biz_id):
        try:
            success_count = 0
            failure_count = 0

            logger.info(f"[BIZ({bk_biz_id})] Start to load hosts with relations")
            hosts_list = self.collect_biz_content(bk_biz_id)
            logger.info(f"[BIZ({bk_biz_id})] Succeed to load {len(hosts_list)} hosts")

            for host_index, h in enumerate(hosts_list):
                host_info = self.filter_columns(h)
                try:
                    self.produce_message(message=host_info)
                except Exception as err:
                    logger.exception(
                        f"[BIZ({bk_biz_id})] Fail to write message({host_info}) to kafka, {err}"
                    )
                    failure_count += 1
                else:
                    success_count += 1

                if host_index % 1000 == 0:
                    logger.info(
                        f"[BIZ({bk_biz_id})] Now process {host_index + 1}th host."
                    )
        except Exception as err:
            logger.exception(f"[BIZ({bk_biz_id})] Fail to batch report hosts, {err}")

        logger.info(
            f"[BIZ({bk_biz_id})] Result: success_count={success_count}, failure_count={failure_count}"
        )

        # 每个业务处理完，适当添加延迟，避免单业务处理过快
        time.sleep(1)

    def exist_watcher(self):
        host_watcher = CMDBResourceWatcher(
            object_type=self.object_type, fields=self.fields, key_name=self.key_name
        )
        relation_watcher = CMDBResourceWatcher(
            self.relation_object_type, self.relation_fields
        )

        return bool(host_watcher.get_cursor_id()) and bool(
            relation_watcher.get_cursor_id()
        )

    def watch_report(self, start_time):
        """
        重载增量采集上报， 因需 hostinfo/relation 并行采集
        """
        signal = Signal(is_active=True)

        host_watcher = CMDBResourceWatcher(
            object_type=self.object_type, fields=self.fields, key_name=self.key_name
        )
        relation_watcher = CMDBResourceWatcher(
            self.relation_object_type, self.relation_fields
        )

        pool = ThreadPoolExecutor(2)
        futures = [
            pool.submit(
                host_watcher.watch_loop,
                start_time,
                self.handle_host_event,
                signal,
                True,
            ),
            pool.submit(
                relation_watcher.watch_loop,
                start_time,
                self.handle_relation_event,
                signal,
                False,
            ),
        ]
        for future in as_completed(futures):
            # 当有一个线程异常退出，则整个主逻辑可以中断
            try:
                future.result()
            except Exception as err:
                logger.exception(f"Watcher({future}) exit abnormally, {err}")
            finally:
                signal.is_active = False

    def handle_host_event(self, event):
        """
        处理主机事件 / 主机关系事件
        """
        event_info = event["bk_detail"]
        event_type = event["bk_event_type"]
        if event_info:
            bk_host_id = event_info["bk_host_id"]
            if event_type in ["update", "create"]:
                bk_biz_id, host_relations = self.get_host_relations(bk_host_id)
                event_info["bk_biz_id"] = bk_biz_id
                event_info["bk_relations"] = {"content": host_relations}

            # 删除事件需要置空处理
            if event_type in ["delete"]:
                event_info["bk_biz_id"] = 0
                event_info["bk_relations"] = {"content": {}}

            info = self.filter_columns(event_info)
            self.produce_message(
                message=info, event_type=event_type, collect_method="watch"
            )

    def filter_columns(self, event_info):
        """
        重载字段处理方式，
        """
        event_info = super(CMDBHostInstCollector, self).filter_columns(event_info)
        event_info["bk_relations"] = json.dumps(event_info["bk_relations"])
        return event_info

    def handle_relation_event(self, event):
        """
        监听主机关系事件
        """
        event_info = event["bk_detail"]
        event_type = event["bk_event_type"]
        if event_info:
            bk_host_id = event_info["bk_host_id"]
            if event_type in ["update", "create", "delete"]:
                host_info = self.get_host(bk_host_id)
                bk_biz_id, host_relations = self.get_host_relations(bk_host_id)

                host_info["bk_biz_id"] = bk_biz_id
                host_info["bk_relations"] = {"content": host_relations}

                info = self.filter_columns(host_info)
                self.produce_message(
                    message=info, event_type=event_type, collect_method="watch"
                )

    def collect_biz_content(self, bk_biz_id, only_pk_key=False) -> List:
        """
        采集业务下所有主机信息

        :param bk_biz_id:
        :param only_pk_key: 是否只需要返回主键数值
        """
        fields = [self.key_name] if only_pk_key else None
        hosts = cmdb_api.get_all_hosts_with_biz(
            bk_biz_id=bk_biz_id, fields=fields, raise_exception=True
        )

        if only_pk_key:
            return [h[self.key_name] for h in hosts]

        host_relations_mapping = CMDBHostInstCollector.get_biz_host_relations_mapping(
            bk_biz_id
        )

        none_relation_count = 0
        for h in hosts:
            bk_host_id = h["bk_host_id"]
            host_relations = host_relations_mapping.get(bk_host_id, None)
            # 当 bk_relations 为空时，则无业务属性，则该数据暂时舍弃
            if host_relations is None:
                none_relation_count += 1
                continue

            # 补充关系字段 bk_relations，这里的结构需要加上 content 字段，将整体规整为字典，便于在平台内进行清洗
            h["bk_relations"] = {"content": host_relations}
            h["bk_biz_id"] = bk_biz_id

        logger.info(
            f"[BIZ({bk_biz_id})] Filter {none_relation_count} hosts with none relations"
        )
        return hosts

    @staticmethod
    def get_biz_host_relations_mapping(bk_biz_id):
        """
        全量采集并聚合指定业务下的关系，将 biz 下的 host relation 按 host 为 key 聚合, value 为 [{"bk_set_id": "", "bk_module_id": ""}]
        """
        biz_relation_raw_list = cmdb_api.get_all_hosts_relations_in_biz(
            params={"bk_biz_id": bk_biz_id}, raise_exception=True
        )
        host_relation_mapping = {}
        for relation_info in biz_relation_raw_list:
            relation_item = {
                "bk_set_id": relation_info["bk_set_id"],
                "bk_module_id": relation_info["bk_module_id"],
            }
            bk_host_id = relation_info["bk_host_id"]
            host_relation_mapping.setdefault(bk_host_id, list()).append(relation_item)

        return host_relation_mapping

    @staticmethod
    def get_biz_count(biz_id):
        biz_r = cmdb_api.list_biz_hosts(
            {"bk_biz_id": biz_id, "page": {"start": 0, "limit": 1}},
            raise_exception=True,
        )
        count = biz_r.data["count"]
        return count

    @staticmethod
    def get_host(bk_host_id: int) -> Dict:
        """
        获取单个主机的基本信息
        """
        response = cmdb_api.get_host_base_info(
            {"bk_host_id": bk_host_id}, raise_exception=True
        )
        return {
            item["bk_property_id"]: item["bk_property_value"] for item in response.data
        }

    @staticmethod
    def get_host_relations(bk_host_id: int) -> List:
        """
        获取单个主机的关系
        """
        response = cmdb_api.find_host_biz_relation(
            {"bk_host_id": [bk_host_id]}, raise_exception=True
        )
        host_relations = response.data
        if len(host_relations) == 0:
            raise Exception(f"Host({bk_host_id}) has no relations")

        bk_biz_id = host_relations[0]["bk_biz_id"]
        return (
            bk_biz_id,
            [
                {
                    "bk_set_id": relation["bk_set_id"],
                    "bk_module_id": relation["bk_module_id"],
                }
                for relation in host_relations
            ],
        )


def collect_cmdb_host_info(params=None):
    if not params:
        params = {
            "bk_biz_id": constants.BKDATA_BIZ_ID,
            "raw_data_name": constants.CMDB_HOST_TABLE_NAME,
        }
    CMDBHostInstCollector(params).report()


def batch_collect_cmdb_host_info(params=None):
    if not params:
        params = {
            "bk_biz_id": constants.BKDATA_BIZ_ID,
            "raw_data_name": constants.CMDB_HOST_TABLE_NAME,
        }
        CMDBHostInstCollector(params).batch_report()


def collect_cmdb_host_info_by_one(bk_biz_id):
    params = {
        "bk_biz_id": constants.BKDATA_BIZ_ID,
        "raw_data_name": constants.CMDB_HOST_TABLE_NAME,
    }
    CMDBHostInstCollector(params).batch_report_one_by_one(bk_biz_id)
