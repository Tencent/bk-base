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
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from api import cmdb_api
from collection.cmdb.base import CMDBBaseCollector, CMDBResourceWatcher
from collection.conf import constants

logger = logging.getLogger(__name__)


class CMDBModuleInfoCollector(CMDBBaseCollector):
    """
    cmdb 模块信息采集
    """

    object_type = "module"
    fields = constants.CMDB_MODULE_FIELDS
    key_name = constants.CMDB_MODULE_PK_NAME

    def __init__(self, config: dict, init_producer=True):
        super(CMDBModuleInfoCollector, self).__init__(
            config, init_producer=init_producer
        )

    def batch_report(self):
        """
        全量获取上报
        """
        bk_biz_ids = self.bk_biz_ids
        biz_count = len(bk_biz_ids)
        logger.info(f"[CMDB MODULE] Search {biz_count} business from CMDB.")

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

            logger.info(f"[BIZ({bk_biz_id})] Start to load modules")
            modules_list = self.collect_biz_content(bk_biz_id)
            logger.info(
                f"[BIZ({bk_biz_id})] Succeed to load {len(modules_list)} modules"
            )

            for module_index, h in enumerate(modules_list):
                module_info = self.filter_columns(h)
                try:
                    self.produce_message(message=module_info)
                except Exception as err:
                    logger.exception(
                        f"[BIZ({bk_biz_id})] Fail to write message({module_info}) to kafka, {err}"
                    )
                    failure_count += 1
                else:
                    success_count += 1

                if module_index % 1000 == 0:
                    logger.info(
                        f"[BIZ({bk_biz_id})] Now process {module_index + 1}th module."
                    )
        except Exception as err:
            logger.exception(f"[BIZ({bk_biz_id})] Fail to batch report module, {err}")

        logger.info(
            f"[BIZ({bk_biz_id})] Result: success_count={success_count}, failure_count={failure_count}"
        )

        # 每个业务处理完，适当添加延迟，避免单业务处理过快
        time.sleep(1)

    def exist_watcher(self):
        module_watcher = CMDBResourceWatcher(self.object_type, self.fields)

        return bool(module_watcher.get_cursor_id())

    def handle_event(self, event):
        """
        对增量采集到的事件进行处理上报
        """
        self.produce_message(
            message=event["bk_detail"],
            event_type=event["bk_event_type"],
            collect_method="watch",
        )
        return True

    def collect_biz_content(self, biz_id, only_pk_key=False) -> list:
        """
        采集业务下全部集群信息
        """
        fields = [self.key_name] if only_pk_key else None

        modules = cmdb_api.search_module(
            params={"bk_biz_id": biz_id, "fields": fields}
        ).data["info"]
        if only_pk_key:
            return [h[self.key_name] for h in modules]
        else:
            return modules

    @staticmethod
    def biz_collect(biz_id):
        return cmdb_api.search_module(params={"bk_biz_id": biz_id}).data["info"]

    @staticmethod
    def get_biz_count(biz_id):
        """
        仅获取指定业务下 集群(module) 数量
        """
        biz_r = cmdb_api.search_module(
            {"bk_biz_id": biz_id, "page": {"start": 0, "limit": 1}}
        )
        count = biz_r.data["count"]
        return count


def collect_cmdb_module_info(params=None):
    if not params:
        params = {
            "bk_biz_id": constants.BKDATA_BIZ_ID,
            "raw_data_name": constants.CMDB_MODULE_TABLE_NAME,
        }
    CMDBModuleInfoCollector(params).report()


def batch_collect_cmdb_module_info(params=None):
    if not params:
        params = {
            "bk_biz_id": constants.BKDATA_BIZ_ID,
            "raw_data_name": constants.CMDB_MODULE_TABLE_NAME,
        }
        CMDBModuleInfoCollector(params).batch_report()


def collect_cmdb_module_info_by_one(bk_biz_id):
    params = {
        "bk_biz_id": constants.BKDATA_BIZ_ID,
        "raw_data_name": constants.CMDB_MODULE_TABLE_NAME,
    }
    CMDBModuleInfoCollector(params).batch_report_one_by_one(bk_biz_id)


if __name__ == "__main__":
    collect_cmdb_module_info()
