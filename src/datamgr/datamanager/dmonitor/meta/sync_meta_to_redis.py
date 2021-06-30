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
import copy
import json
import logging
import math
import time

import gevent
from common.mixins import MetaCacheMixin
from common.redis import connections
from dmonitor.settings import META_CACHE_CONFIGS
from gevent import Greenlet, monkey, pool

monkey.patch_all()


def sync_meta_to_redis():
    logging.info("Start to execute metadata sync to redis task")

    sync_task_config = {
        "meta_task_pool": 10,  # 刷新缓存任务协程池
    }

    try:
        task = SyncMetaToRedisTaskGreenlet(sync_task_config)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init metadata sync to redis task".format(
                error=e
            ),
            exc_info=True,
        )


class SyncMetaToRedisTaskGreenlet(MetaCacheMixin, Greenlet):
    """把接口获取的元数据同步到Redis，以供各个任务从redis中获取所需元数据"""

    def __init__(self, sync_task_config):
        """初始化刷新元数据到Redis任务的配置

        :param sync_task_config: 缓存同步任务配置
            {
                'task_pool': 10,          # 刷新缓存任务协程池
            }
        """
        super(SyncMetaToRedisTaskGreenlet, self).__init__()
        self._sync_task_config = sync_task_config

        # 任务配置
        self._refresh_interval = sync_task_config.get("refresh_interval", 300)
        self._meta_task_pool = sync_task_config.get("task_pool", 1000)
        self._redis_conn = connections["default"]

        self._metadata_configs = copy.deepcopy(META_CACHE_CONFIGS)

        self._metadata_configs.get("data_set", {}).update(
            {
                "refresh_func": self.fetch_data_set_infos,  # 获取元数据函数
                "inc_refresh_func": self.fetch_data_set_infos,  # 增量获取元数据的函数（请求参数统一用update_duration）
                "request_params": {},  # 请求参数
                "refresh_interval": 3600,  # 元数据刷新间隔
                "inc_refresh_interval": 60,  # 元数据增量刷新间隔
                "last_refresh_time": None,  # 上次刷新元数据的时间
                "last_inc_refresh_time": None,  # 上次增量刷新元数据的时间
                "meta_refresh_pool": 100,  # 元数据配置写Redis任务协程池
                "metadata": {},  # 当前元数据配置，用于增量更新
            }
        )
        self._metadata_configs.get("data_operation", {}).update(
            {
                "refresh_func": self.fetch_data_operations,
                "inc_refresh_func": None,
                "request_params": {
                    "with_relations": True,
                    "with_status": True,
                    "with_node": True,
                },
                "refresh_interval": 300,
                "last_refresh_time": None,
                "meta_refresh_pool": 100,
            }
        )
        self._metadata_configs.get("flow", {}).update(
            {
                "refresh_func": self.fetch_flow_infos,
                "inc_refresh_func": None,
                "request_params": {"with_nodes": True},
                "refresh_interval": 60,
                "last_refresh_time": None,
                "meta_refresh_pool": 100,
            }
        )
        self._metadata_configs.get("dataflow", {}).update(
            {
                "refresh_func": self.fetch_dataflow_infos,
                "inc_refresh_func": None,
                "refresh_interval": 60,
                "last_refresh_time": None,
                "meta_refresh_pool": 100,
            }
        )

    def _run(self):
        """按一定刷新间隔同步元数据到redis"""
        meta_task_pool = pool.Pool(self._meta_task_pool)

        while True:
            now = time.time()

            for meta_name, meta_task_config in self._metadata_configs.items():
                if not meta_task_config["last_refresh_time"]:
                    meta_task_pool.spawn(
                        self.refresh_metadata_by_config, meta_name, meta_task_config
                    )
                    meta_task_config["last_refresh_time"] = now
                    meta_task_config["last_inc_refresh_time"] = now
                    continue

                if (
                    now - meta_task_config["last_refresh_time"]
                    > meta_task_config["refresh_interval"]
                ):
                    meta_task_pool.spawn(
                        self.refresh_metadata_by_config, meta_name, meta_task_config
                    )
                    meta_task_config["last_refresh_time"] = now
                    meta_task_config["last_inc_refresh_time"] = now
                    continue

                if meta_task_config["inc_refresh_func"]:
                    now_inc_refresh_interval = (
                        now - meta_task_config["last_inc_refresh_time"]
                    )
                    if (
                        now_inc_refresh_interval
                        > meta_task_config["inc_refresh_interval"]
                    ):
                        meta_task_pool.spawn(
                            self.refresh_metadata_by_config,
                            meta_name,
                            meta_task_config,
                            True,
                            now_inc_refresh_interval,
                        )
                        meta_task_config["last_inc_refresh_time"] = now
                        continue

            gevent.sleep(1)

    def refresh_metadata_by_config(
        self, name, meta_task_config, update=False, interval=0
    ):
        """根据元数据刷新任务配置刷新元数据

        :param name: 元数据的名称
        :param meta_task_config: 元数据任务配置
        :param update: 更新周期时间
        :param interval: 上次到当前的刷新间隔
        """
        request_params = meta_task_config.get("request_params", {})

        if update and meta_task_config["inc_refresh_func"]:
            request_params.update({"update_duration": math.ceil(interval)})
            inc_metadata = meta_task_config["inc_refresh_func"](**request_params)
            self.update_metadata(meta_task_config["metadata"], inc_metadata)
            metadata = meta_task_config["metadata"]
        else:
            metadata = meta_task_config["refresh_func"](**request_params)
            if meta_task_config["inc_refresh_func"]:
                meta_task_config["metadata"] = metadata

        self.sync_to_redis(metadata, meta_task_config)

        logging.info("Finish to refresh metadata({})".format(name))

    def update_metadata(self, metadata, update_data):
        """增量更新元数据

        :param metadata: 原始元数据
        :param update_data: 待更新元数据
        """
        for meta_key in update_data.keys():
            if meta_key not in metadata:
                metadata[meta_key] = update_data[meta_key]
                continue

            for attr_key in metadata[meta_key].keys():
                if (
                    attr_key in update_data[meta_key]
                    and update_data[meta_key][attr_key]
                ):
                    metadata[meta_key][attr_key] = update_data[meta_key][attr_key]

    def sync_to_redis(self, metadata, meta_task_config):
        """同步更新后的元数据到Redis

        :param metadata: 刷新后的元数据
        :param meta_task_config: 元数据任务配置
        """
        task_pool = pool.Pool(meta_task_config["meta_refresh_pool"])
        key_sets = meta_task_config["key_sets"]

        for meta_key, meta_content in metadata.items():
            redis_meta_key = meta_task_config["key_template"].format(meta_key)
            task_pool.spawn(
                self._redis_conn.set, redis_meta_key, json.dumps(meta_content)
            )
            task_pool.spawn(self._redis_conn.sadd, key_sets, meta_key)

        while task_pool.free_count() != meta_task_config["meta_refresh_pool"]:
            gevent.sleep(1)
