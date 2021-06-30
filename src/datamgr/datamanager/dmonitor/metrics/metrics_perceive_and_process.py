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
import random
import time

import gevent
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import (
    BIG_DIMENSIONS,
    DMONITOR_TOPICS,
    STORAGE_COMPONENT_NODE_TYPES,
)
from gevent import monkey
from utils.time import strtotime

monkey.patch_all()


def metrics_perceive_and_process(params):
    logging.info("Start to execute metrics perceive and associate task")

    perceive_task_config = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": params.get("topic", "bkdata_data_monitor_metrics591"),
            "partition": params.get("partition", False),
            "group_id": "dmonitor",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "heartbeat_distribute_components": ["storm"],
        "task_pool_size": 50,
    }

    try:
        task = MetricsPerceiveAndProcessTaskGreenlet(configs=perceive_task_config)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init metrics clean task".format(error=e),
            exc_info=True,
        )


class MetricsPerceiveAndProcessTaskGreenlet(BaseDmonitorTaskGreenlet):
    DATA_SET_REFRESH_INTERVAL = 1800
    REFRESH_ERROR_INTERVAL = 180

    def __init__(self, *args, **kwargs):
        """初始化刷新元数据到Redis任务的配置

        :param perceive_task_config: 缓存同步任务配置
            {
                'consumer_configs': {
                    'type': 'kafka',
                    'alias': 'op',
                    'topic': 'bkdata_data_monitor_metrics591',
                    'partition': False,
                    'group_id': 'dmonitor',
                    'batch_message_max_count': 5000,
                    'batch_message_timeout': 0.1,
                },
                'task_pool_size': 100,
            }
        """
        configs = kwargs.pop("configs", {})

        super(MetricsPerceiveAndProcessTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._logical_subjects = {}
        self._physical_subjects = {}
        self._cache_last_refresh_time = None
        self._data_sets = {}
        self._heartbeat_distribute_components = configs.get(
            "heartbeat_distribute_components", []
        )

        self.refresh_metadata_cache(now)

    def refresh_metadata_cache(self, now):
        """刷新埋点解析依赖的元数据信息

        :param now: 当前刷新缓存的时间
        """
        if (
            self._cache_last_refresh_time
            and now - self._cache_last_refresh_time < self.DATA_SET_REFRESH_INTERVAL
        ):
            return

        gevent.spawn(
            self.refresh_metadata,
            self._data_sets,
            self.fetch_data_set_infos_from_redis,
            update=False,
        )

        self._cache_last_refresh_time = now

    def check_data_set_info(self, data_set_id, now):
        """检查单个数据集的信息是否已获取，否则从Cache中刷新

        :param data_set_id: 数据集ID
        :param now: 当前时间
        """
        data_set_info = {"last_refresh_error_time": now}
        if data_set_id not in self._data_sets:
            data_set_info_from_redis = self.fetch_data_set_by_id(
                data_set_id=data_set_id
            )
            if data_set_info_from_redis:
                data_set_info = data_set_info_from_redis
        elif "last_refresh_error_time" in self._data_sets[data_set_id]:
            if (
                now - self._data_sets[data_set_id]["last_refresh_error_time"]
                > self.REFRESH_ERROR_INTERVAL
            ):
                data_set_info = self.fetch_data_set_by_id(data_set_id)
        else:
            return

        self._data_sets[data_set_id] = data_set_info

    def handle_monitor_value(self, value, now):
        """处理各个模块上报的任务埋点

        :param value: 任务埋点的内容
        :param now: 当前处理数据的时间
        """
        if type(value) == list:
            for msg in value:
                self.parse_message(msg, now)
        elif type(value) == dict:
            self.parse_message(value, now)

    def parse_message(self, message, now):
        """解析埋点信息内容

        :param message: 埋点消息主体
        :param now: 解析时间，这里需要对当前所以埋点的解析时间进行统一，防止因为解析的耗时对解析时间造成误差
        """
        try:
            if not message:
                return
            current_logical_tag, current_physical_tag = self.parse_info_data(
                message, now
            )
            self.parse_metrics_data(message, current_logical_tag, now)
        except Exception as e:
            logging.error(
                "Parse message error: {}, the message is {}".format(
                    e, json.dumps(message)
                ),
                exc_info=True,
            )

    def parse_info_data(self, message, now):
        """解析埋点数据INFO部分的信息

        :param message: 埋点消息内容
            {
                "info": {
                    "logical_tag": {
                        "desc": {
                            "result_table_id": "591_mna_normal_p_011"
                        },
                        "tag": "591_mna_normal_p_011"
                    },
                    "physical_tag": {
                        "desc": {
                            "result_table_id": "591_mna_normal_p_011",
                            "task_id": "13",
                            "topology_id": "topology-44699"
                        },
                        "tag": "topology-44699|591_mna_normal_p_011|13"
                    },
                    "custom_tags": {
                        "port": "6706",
                        "task": "13",
                        "ip": "x.x.x.x"
                    },
                    "module": "stream",
                    "component": "flink",
                    "cluster": "",
                    "logical_type": "stream"
                }
            }
        :param now: 当前解析埋点的时间
        """
        module = message.get("info", {}).get("module", "")
        component = message.get("info", {}).get("component", "")
        cluster = message.get("info", {}).get("cluster", "default")
        storage = message.get("info", {}).get("storage", {})
        logical_tag_info = message.get("info", {}).get("logical_tag", {})
        physical_tag_info = message.get("info", {}).get("physical_tag", {})
        logical_tag = logical_tag_info.get("tag", "")
        physical_tag = physical_tag_info.get("tag", "")
        custom_tags = message.get("info", {}).get("custom_tags", {}) or {}
        version = message.get("version", "1.0")

        # 防止埋点缺少必要维度信息
        try:
            self.check_metadata(
                self._data_sets, self.REFRESH_ERROR_INTERVAL, "datasets"
            )
        except Exception:
            self.refresh_metadata_cache(now)
            self._cache_last_refresh_time = now

        # 旧版本埋点不实时更新元数据
        if version != "1.0":
            self.check_data_set_info(logical_tag, now)

        # 补全存储维度信息
        storage_key, storage_cluster_type = self.parse_storage(
            storage, module, component, logical_tag, version
        )
        base_tags = {
            "module": module,
            "component": component,
            "cluster": cluster,
            "storage": storage_key,
            "logical_tag": logical_tag,
            "version": version,
            "storage_cluster_type": storage_cluster_type,
            "data_set_id": logical_tag,
        }
        custom_tags.update(base_tags)
        custom_tags["physical_tag"] = physical_tag

        # 补全结果表或者源数据的其他的维度信息
        if logical_tag in self._data_sets and self._data_sets[logical_tag]:
            if "last_refresh_error_time" not in self._data_sets[logical_tag]:
                data_set_info = self._data_sets[logical_tag]
                if data_set_info["data_set_type"] == "result_table":
                    base_tags["flow_id"] = data_set_info.get("flow_id")
                    base_tags["node_id"] = self.get_flow_node_id(
                        data_set_info, module, component, storage_key, version
                    )
                    base_tags["bk_biz_id"] = self._data_sets[logical_tag].get(
                        "bk_biz_id"
                    )
                    base_tags["project_id"] = self._data_sets[logical_tag].get(
                        "project_id"
                    )
                elif data_set_info["data_set_type"] == "raw_data":
                    base_tags["flow_id"] = data_set_info.get("flow_id")
                    base_tags["bk_biz_id"] = self._data_sets[logical_tag].get(
                        "bk_biz_id"
                    )
                    base_tags["topic"] = self._data_sets[logical_tag].get("topic")
                    base_tags["data_category"] = self._data_sets[logical_tag].get(
                        "data_category"
                    )
                    base_tags["data_scenario"] = self._data_sets[logical_tag].get(
                        "data_scenario"
                    )

        logical_tag_key = "{}_{}_{}_{}_{}".format(
            module, component, cluster, storage_key, logical_tag
        )
        physical_tag_key = "{}_{}_{}_{}_{}".format(
            module, component, cluster, storage_key, physical_tag
        )

        if logical_tag_key not in self._logical_subjects:
            self._logical_subjects[logical_tag_key] = {}
        self._logical_subjects[logical_tag_key] = {
            "desc": logical_tag_info.get("desc", {}),
            "base_tags": base_tags,
            "custom_tags": custom_tags,
        }

        if physical_tag_key not in self._physical_subjects:
            self._physical_subjects[physical_tag_key] = {}
        self._physical_subjects[physical_tag_key] = {
            "desc": physical_tag_info.get("desc", {}),
            "base_tags": base_tags,
            "custom_tags": custom_tags,
        }

        return logical_tag_key, physical_tag_key

    def get_flow_node_id(
        self, data_set_info, module, component, storage, version="1.0"
    ):
        # 1.0版本埋点获取node_id
        if version == "1.0":
            if module == "realtime":
                return data_set_info.get("nodes", {}).get("realtime", None)
            elif module == "databus":
                if "puller" in component:
                    return None
                component_parts = component.split("-")
                if len(component_parts) > 0:
                    node_type = STORAGE_COMPONENT_NODE_TYPES.get(
                        component, "%s_storage" % component_parts[0]
                    )
                    return data_set_info.get("nodes", {}).get(node_type, None)

        # 2.0版本埋点获取node_id
        if module == "puller":
            return None
        elif module in ("stream", "realtime") and "realtime" in data_set_info.get(
            "nodes", {}
        ):
            return data_set_info.get("nodes", {}).get("realtime", None)
        elif module == "batch" and "offline" in data_set_info.get("nodes", {}):
            return data_set_info.get("nodes", {}).get("offline", None)
        elif storage.startswith("storage"):
            node_type = STORAGE_COMPONENT_NODE_TYPES.get(
                component, "%s_storage" % component
            )
            if node_type in data_set_info.get("nodes", {}):
                return data_set_info.get("nodes", {}).get(node_type, None)

        non_storage_node_count = 0
        cur_node_type = None
        for node_type, node_id in data_set_info.get("nodes", {}).items():
            if node_type.find("storage") < 0:
                non_storage_node_count += 1
                cur_node_type = node_type

        if non_storage_node_count == 1:
            return data_set_info.get("nodes", {}).get(cur_node_type, None)
        else:
            return None

    def parse_metrics_data(self, message, current_logical_tag, now):
        """
        处理埋点信息中的指标数据
        {
            "metrics": {
                "date_monitor": {
                    "data_loss": {
                        "input": {
                            "tags": {
                                "topology-44699_591_mna_normal_p_009_13_1493016820": 99114
                            },
                            "total_cnt": 83599444,
                            "total_cnt_increment": 99114
                        },
                        "output": {
                            "tags": {
                                "topology-44699_591_mna_normal_p_011_13_1493016820": 13732
                            },
                            "total_cnt": 9350234,
                            "total_cnt_increment": 13732
                        },
                        "data_drop": {
                            "data_drop1": {
                                "cnt": 85382,
                                "reason": "数据transform丢弃。"
                            }
                        }
                    },
                    "data_delay": {
                        "window_time": 0,
                        "min_delay": {
                            "data_time": 1493009045,
                            "delay_time": 7836,
                            "output_time": 1493016881
                        },
                        "waiting_time": 0,
                        "max_delay": {
                            "data_time": 1493008820,
                            "delay_time": 8002,
                            "output_time": 1493016822
                        }
                    }
                },
                "resource_monitor": {
                    "module_component_mem": {
                        "tags": "topology-44699|591_mna_normal_p_011|13",
                        "total": 4719443968,
                        "used": 1339845968
                    }
                }
            }
        }
        """
        metric_time = message.get("time", now) or now
        if isinstance(metric_time, str):
            try:
                metric_time = strtotime(metric_time)
            except Exception:
                metric_time = now

        # 增加随机值防止埋点数据维度一致而相互覆盖
        metric_time += 1e-10 * random.randint(0, 100000)
        base_tags = self._logical_subjects.get(current_logical_tag, {}).get(
            "base_tags", {}
        )
        custom_tags = self._logical_subjects.get(current_logical_tag, {}).get(
            "custom_tags", {}
        )

        # 兼容采集器埋点错误key
        if "metrics" in message and "date_monitor" in message["metrics"]:
            message["metrics"]["data_monitor"] = message["metrics"]["date_monitor"]

        try:
            self.handle_time_report(message, metric_time, base_tags, now)
            self.handle_custom_metrics(message, metric_time, custom_tags)
            self.handle_resource_metrics(message, metric_time, custom_tags)
            self.handle_data_monitor_metrics(message, metric_time, base_tags)
            self.handle_data_profiling_metrics(message, metric_time, base_tags)
        except Exception as e:
            logging.error(
                "Metric format error: {}, message: {}".format(e, json.dumps(message))
            )

    def parse_storage(self, storage, module, component, logical_tag, version="1.0"):
        """根据埋点上报的相关信息获取当前埋点对应的存储

        :param storage: 埋点上报的存储信息
        :param module: 模块
        :param component: 组件
        :param logical_tag: 逻辑标识，即数据集ID
        :param version: 版本ID

        :return: 存储集群类型与存储ID组成的key
        """
        if logical_tag not in self._data_sets:
            return "_".join(map(str, storage.values())) or "None", storage.get(
                "cluster_type", None
            )
        if not self._data_sets[logical_tag]:
            return "_".join(map(str, storage.values())) or "None", storage.get(
                "cluster_type", None
            )
        if "last_refresh_error_time" in self._data_sets[logical_tag]:
            return "_".join(map(str, storage.values())) or "None", storage.get(
                "cluster_type", None
            )
        data_set = self._data_sets[logical_tag]

        if version == "1.0":
            # 兼容1.0版本的打点
            return self.parse_version1_storage(data_set, module, component, logical_tag)
        else:
            # 2.0版本及以上存储信息
            return self.parse_new_version_storage(data_set, storage)

    def parse_version1_storage(self, data_set, module, component, logical_tag):
        """解析1.0埋点的存储信息"""
        if data_set["data_set_type"] == "raw_data":
            return (
                "{}_{}".format(data_set["storage_type"], data_set["storage_id"]),
                "kafka",
            )
        elif data_set["data_set_type"] == "result_table":
            storage_id, storage_type, cluster_type = "None", "None", None
            if module == "realtime":
                if "StormSql" not in logical_tag and "FlinkSql" not in logical_tag:
                    storage_type, storage_id = self.parse_storage_by_type(
                        data_set["storages"], "kafka", storage_id, storage_type
                    )
                    cluster_type = "kafka"
            elif module == "databus":
                if "puller" in component or component.startswith("clean"):
                    storage_type, storage_id = self.parse_storage_by_type(
                        data_set["storages"], "kafka"
                    )
                    cluster_type = "kafka"
                elif component == "puller-tredis":
                    storage_type, storage_id = self.parse_storage_by_type(
                        data_set["storages"], "tredis"
                    )
                    cluster_type = "tredis"
                else:
                    component_parts = component.split("-")
                    if len(component_parts):
                        storage_type, storage_id = self.parse_storage_by_type(
                            data_set["storages"], component_parts[0]
                        )
                    cluster_type = component_parts[0]
            return "{}_{}".format(storage_type, storage_id), cluster_type if (
                storage_type and storage_id
            ) else (
                "None",
                None,
            )

    def parse_new_version_storage(self, data_set, storage):
        if data_set["data_set_type"] == "raw_data":
            return (
                "{}_{}".format(data_set["storage_type"], data_set["storage_id"]),
                "kafka",
            )
        elif data_set["data_set_type"] == "result_table":
            storage_type, storage_id, cluster_type = None, None, None
            if "storage_id" in storage and "storage_type" in storage:
                storage_type = storage["storage_type"]
                storage_id = storage["storage_id"]
                cluster_type = self.get_cluster_type(
                    data_set["storages"], storage_type, storage_id
                )
                return "{}_{}".format(storage_type, storage_id), cluster_type
            # 由于一个RT不会有两个相同类型的存储，因此可以以cluster_type来确认RT的存储配置
            elif "cluster_type" in storage:
                cluster_type = storage["cluster_type"]
                storage_type, storage_id = self.parse_storage_by_type(
                    data_set["storages"], storage["cluster_type"]
                )
            elif len(storage) > 0:
                (
                    storage_type,
                    storage_id,
                    cluster_type,
                ) = self.parse_storage_by_connection(data_set["storages"], storage)
            elif len(data_set["storages"]) > 0:
                storage_type, storage_id, cluster_type = self.parse_storage_by_content(
                    data_set["storages"], storage
                )
            else:
                return "None", None
            return "{}_{}".format(storage_type, storage_id), cluster_type if (
                storage_type and storage_id
            ) else (
                "None",
                None,
            )

    def parse_storage_by_type(
        self,
        data_set_storages,
        cluster_type,
        default_storage_id="",
        default_storage_type="",
    ):
        """根据存储类型解析存储信息

        :param data_set_storages: 数据集存储信息
        :param cluster_type: 集群类型
        :param default_storage_id: 默认存储ID
        :param default_storage_type:

        :return: 存储集群类型与存储ID组成的key
        """
        storage_id, storage_type = default_storage_id, default_storage_type
        if cluster_type in data_set_storages:
            if data_set_storages[cluster_type]["storage_cluster"]:
                storage_type = "storage"
                storage_id = data_set_storages[cluster_type]["storage_cluster"][
                    "storage_cluster_config_id"
                ]
            elif data_set_storages[cluster_type]["storage_channel"]:
                storage_type = "channel"
                storage_id = data_set_storages[cluster_type]["storage_channel"][
                    "channel_cluster_config_id"
                ]
        return storage_type, storage_id

    def parse_storage_by_connection(self, data_set_storages, storage):
        """根据连接信息解析存储信息

        :param data_set_storages: 数据集存储信息
        :param storage: 包含连接信息的埋点存储

        :return: 存储集群类型与存储ID组成的key
        """
        storage_id, storage_type, target_cluster_type = "", "", None
        for cluster_type, storage_config in data_set_storages.items():
            if (
                storage_config["storage_channel"]
                and "host" in storage
                and "port" in storage
            ):
                if (
                    storage["host"]
                    != storage_config["storage_channel"]["cluster_domain"]
                ):
                    continue
                if storage["port"] != storage_config["storage_channel"]["cluster_port"]:
                    continue
                storage_id = storage_config["storage_channel"][
                    "channel_cluster_config_id"
                ]
                storage_type = "channel"
                target_cluster_type = cluster_type
                break
            elif storage_config["storage_cluster"]:
                connection_info = json.loads(
                    storage_config["storage_cluster"]["connection_info"]
                )
                match = True
                for key in storage.keys():
                    if connection_info.get(key, "") != storage[key]:
                        match = False
                        break
                if not match:
                    continue
                storage_id = storage_config["storage_cluster"][
                    "storage_cluster_config_id"
                ]
                storage_type = "storage"
                target_cluster_type = cluster_type
                break
        return storage_type, storage_id, target_cluster_type

    def parse_storage_by_content(self, data_set_storages, storage):
        """根据埋点上报的存储类型和ID获取埋点的存储信息

        :param data_set_storages: 数据集存储信息
        :param storage: 包含存储信息的埋点存储维度

        :return: 存储集群类型与存储ID组成的key
        """
        storage_id, storage_type, target_cluster_type = "", "", None
        for cluster_type, storage_config in data_set_storages.items():
            if storage_config["active"]:
                if storage_config["storage_channel"]:
                    storage_id = storage_config["storage_channel"][
                        "channel_cluster_config_id"
                    ]
                    storage_type = "channel"
                    target_cluster_type = cluster_type
                    break
                elif storage_config["storage_cluster"]:
                    storage_id = storage_config["storage_cluster"][
                        "storage_cluster_config_id"
                    ]
                    storage_type = "storage"
                    target_cluster_type = cluster_type
                    break
        return storage_type, storage_id, target_cluster_type

    def get_cluster_type(self, data_set_storages, storage_type, storage_id):
        """根据存储信息获取存储类型

        :param data_set_storages: 数据集存储信息
        :param storage_type: 存储集群类型，channel or storage
        :param storage_id: 存储ID

        :return: 存储类型
        """
        for cluster_type, cluster_content in data_set_storages.items():
            if storage_type == "storage":
                if storage_id == cluster_content.get("storage_cluster_config_id"):
                    return cluster_type
            elif storage_type == "channel":
                if storage_id == cluster_content.get("storage_channel_id"):
                    return cluster_type
        return None

    def handle_time_report(self, message, metric_time, base_tags, now):
        """处理埋点上报的时间

        :param message: 埋点内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        :param now: 当前时间
        """
        message_time = message.get("time", now)
        if type(message_time) not in [int, float]:
            try:
                message_time = strtotime(message_time)
            except Exception:
                message_time = now
        msg = {
            "time": metric_time,
            "database": "monitor_data_metrics",
            "logical_heartbeat": {
                "last_report_time": int(message_time),
                "tags": base_tags,
            },
        }
        metric_message = json.dumps(msg)
        self.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            metric_message,
        )

        component = base_tags.get("component")
        if component in self._heartbeat_distribute_components:
            self.produce_metric(
                DMONITOR_TOPICS["dmonitor_component_heartbeat"].format(
                    component=component
                ),
                metric_message,
            )

    def handle_resource_metrics(self, message, metric_time, base_tags):
        """处理资源相关的埋点信息

        :param message: 埋点内容
        :param metric_time: 埋点指标时间
        :param base_tags: 基本维度信息
        """
        resource_metrics = message.get("metrics", {}).get("resource_monitor", {}) or {}
        resource_metrics["time"] = message.get("time", "")
        resource_metrics["database"] = "monitor_performance_metrics"

        for metric_name in resource_metrics.keys():
            if type(resource_metrics[metric_name]) is not dict:
                continue

            resource_metrics[metric_name]["physical_tag"] = base_tags["physical_tag"]
            if (
                "tags" not in resource_metrics[metric_name].keys()
                or type(resource_metrics[metric_name]["tags"]) is not dict
            ):
                resource_metrics[metric_name]["tags"] = base_tags
            else:
                resource_metrics[metric_name]["tags"].update(base_tags)

            for big_tag in BIG_DIMENSIONS:
                if big_tag in resource_metrics[metric_name]["tags"]:
                    big_tag_value = resource_metrics[metric_name]["tags"][big_tag]
                    resource_metrics[metric_name][big_tag] = big_tag_value
                    del resource_metrics[metric_name]["tags"][big_tag]

        self.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            json.dumps(resource_metrics),
        )

    def handle_custom_metrics(self, message, metric_time, base_tags):
        """处理自定义指标

        :param message: 埋点内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        custom_metrics = message.get("metrics", {}).get("custom_metrics", {}) or {}
        if "time" in custom_metrics:
            custom_metrics["database"] = "monitor_custom_metrics"
            for big_tag in BIG_DIMENSIONS:
                if not custom_metrics.get("tags", False):
                    custom_metrics["tags"] = {}
                if big_tag in custom_metrics["tags"]:
                    big_tag_value = custom_metrics["tags"][big_tag]
                    custom_metrics[big_tag] = big_tag_value
                    del custom_metrics["tags"][big_tag]

            self.produce_metric(
                DMONITOR_TOPICS["data_cleaning"],
                json.dumps(custom_metrics),
            )

    def handle_data_monitor_metrics(self, message, metric_time, base_tags):
        """处理数据监控相关指标

        :param message: 埋点内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        self.handle_input_metrics(message, metric_time, base_tags)
        self.handle_output_metrics(message, metric_time, base_tags)
        self.handle_drop_metrics(message, metric_time, base_tags)
        self.handle_delay_metrics(message, metric_time, base_tags)

    def handle_input_metrics(self, message, metric_time, base_tags):
        """处理数据丢失中输入相关的指标

        :param message: 埋点内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        # 处理数据丢失中输入相关的指标
        data_loss_data = (
            message.get("metrics", {}).get("data_monitor", {}).get("data_loss", {})
            or {}
        )
        input_data = data_loss_data.get("input", {}) or {}
        input_tags = input_data.get("tags", {}) or {}
        version = message.get("version", "1.0")
        module = base_tags.get("module", "")
        component = base_tags.get("component", "")
        storage = base_tags.get("storage", "")

        # 所有输入tag的输入总和
        data_loss_input_total = {
            "time": metric_time,
            "database": "monitor_data_metrics",
            "data_loss_input_total": {
                "data_cnt": int(input_data.get("total_cnt", 0) or 0),
                # 临时兼容collector的错误格式，下周collector上线新版本后取消兼容
                "data_inc": int(input_data.get("total_cnt_increment", 0) or 0),
                "tags": base_tags,
            },
        }
        target = data_loss_input_total["data_loss_input_total"]
        for big_tag in BIG_DIMENSIONS:
            if big_tag in target.get("tags", {}):
                big_tag_value = target.get("tags", {}).get(big_tag, "")
                target[big_tag] = big_tag_value
                del target["tags"][big_tag]

        metric_message = json.dumps(data_loss_input_total)
        self.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            metric_message,
        )

        # 不使用collector埋点进行监控
        if module == "collector":
            return

        self.produce_metric(
            DMONITOR_TOPICS["data_io_total"],
            metric_message,
        )

        # shipper模块的埋点不参与无数据，数据波动跟数据丢弃的告警和指标任务
        if not self.check_is_shipper_or_puller_metric(version, module, component):
            self.produce_metric(
                DMONITOR_TOPICS["dmonitor_data_drop"],
                metric_message,
            )

        if (
            version == "1.0"
            and module == "databus"
            and storage.startswith("storage")
            and (not component.startswith("clean"))
        ):
            return

        self.handle_input_tag_data(input_tags, metric_time, base_tags)

    def handle_input_tag_data(self, input_tags, metric_time, base_tags):
        """按tags处理输入数据"""
        for index, (tag, value) in enumerate(input_tags.items()):
            # 不同input_tag之间如果维度相同，且时间相同，那么只会保存最后一个input_tag的信息，这样会造成数据丢失，而input_tag
            # 作为数据的维度也不合理，因此在时间上加一个index值来区分不同input_tag的打点数据
            data_loss_input = {
                "time": metric_time + 1e-6 * (index + 1),
                "database": "monitor_data_metrics",
                "data_loss_input": {
                    "data_cnt": int(value or 0),
                    "input_tag": "%s" % tag,
                    "tags": base_tags,
                },
            }

            target = data_loss_input["data_loss_input"]
            for big_tag in BIG_DIMENSIONS:
                if big_tag in target.get("tags", {}):
                    big_tag_value = target.get("tags", {}).get(big_tag, "")
                    target[big_tag] = big_tag_value
                    del target["tags"][big_tag]

            metric_message = json.dumps(data_loss_input)
            self.produce_metric(
                DMONITOR_TOPICS["data_cleaning"],
                metric_message,
            )

            # 目前input_tag要求必须带有时间戳，所以其长度若小于等于10，则认为该tag无效，不参与数据对账
            if len(tag) <= 10:
                return

            if self.check_is_batch_loss_metric("input", base_tags):
                topic = DMONITOR_TOPICS["dmonitor_batch_data_loss"]
            else:
                topic = DMONITOR_TOPICS["dmonitor_data_loss"]
            self.produce_metric(
                topic,
                metric_message,
            )

    def handle_output_metrics(self, message, metric_time, base_tags):
        """处理数据丢失中输出相关的指标

        :param message: 埋点内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        version = message.get("version", "1.0")
        # 处理数据丢失中输出相关的指标
        data_loss_data = (
            message.get("metrics", {}).get("data_monitor", {}).get("data_loss", {})
            or {}
        )
        output_data = data_loss_data.get("output", {}) or {}

        if isinstance(output_data, list):
            for output_data_item in output_data:
                self.handle_output_data(
                    output_data_item, version, metric_time, base_tags
                )
        else:
            self.handle_output_data(output_data, version, metric_time, base_tags)

    def handle_output_data(self, output_data, version, metric_time, base_tags):
        """处理每项输出数据内容

        :param output_data: 原始输出内容
        :param version: 埋点版本
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        output_tags = output_data.get("tags", {}) or {}
        module = base_tags.get("module", "")
        component = base_tags.get("component", "")

        # 所有输入tag的输入总和
        data_loss_output_total = {
            "time": metric_time,
            "database": "monitor_data_metrics",
            "data_loss_output_total": {
                "data_cnt": int(output_data.get("total_cnt", 0) or 0),
                "ckp_drop_cnt": int(output_data.get("ckp_drop", 0) or 0),
                # 临时兼容collector的错误格式，下周collector上线新版本后取消兼容
                "data_inc": int(output_data.get("total_cnt_increment", 0) or 0),
                "tags": base_tags,
            },
        }
        metric_message = json.dumps(data_loss_output_total)

        target = data_loss_output_total["data_loss_output_total"]
        for big_tag in BIG_DIMENSIONS:
            if big_tag in target.get("tags", {}):
                big_tag_value = target.get("tags", {}).get(big_tag, "")
                target[big_tag] = big_tag_value
                del target["tags"][big_tag]

        self.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            metric_message,
        )
        # 不使用collector埋点进行数据中断告警监控
        if module == "collector":
            return

        self.produce_metric(
            DMONITOR_TOPICS["data_io_total"],
            metric_message,
        )

        # shipper模块的埋点不参与无数据，数据波动跟数据丢弃的告警和指标任务
        if not self.check_is_shipper_or_puller_metric(version, module, component):
            self.produce_metric(
                DMONITOR_TOPICS["dmonitor_data_drop"],
                metric_message,
            )
            self.produce_metric(
                DMONITOR_TOPICS["dmonitor_output_total"],
                metric_message,
            )

        if self.check_is_batch_metric(base_tags):
            self.produce_metric(
                DMONITOR_TOPICS["dmonitor_batch_output_total"],
                metric_message,
            )

        self.handle_output_tag_data(output_data, output_tags, metric_time, base_tags)

    def handle_output_tag_data(self, output_data, output_tags, metric_time, base_tags):
        """处理按tag统计的输出数据内容

        :param output_data: 原始输出内容
        :param output_tags: 按tag统计的输出内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        output_ckp_drop_tags = output_data.get("ckp_drop_tags", {})
        for index, (tag, value) in enumerate(output_tags.items()):
            # 不同output_tag之间如果维度相同，且时间相同，那么只会保存最后一个output_tag的信息，这样会造成数据丢失，而output_tag
            # 作为数据的维度也不合理，因此在时间上加一个index值来区分不同output_tag的打点数据
            ckp_drop_cnt = int(output_ckp_drop_tags.get(tag, 0) or 0)
            data_loss_output = {
                "time": metric_time + 1e-6 * (index + 1),
                "database": "monitor_data_metrics",
                "data_loss_output": {
                    "data_cnt": int(value or 0),
                    "ckp_drop_cnt": ckp_drop_cnt,
                    "output_tag": "%s" % tag,
                    "tags": base_tags,
                },
            }

            target = data_loss_output["data_loss_output"]
            for big_tag in BIG_DIMENSIONS:
                if big_tag in target.get("tags", {}):
                    big_tag_value = target.get("tags", {}).get(big_tag, "")
                    target[big_tag] = big_tag_value
                    del target["tags"][big_tag]

            metric_message = json.dumps(data_loss_output)
            self.produce_metric(
                DMONITOR_TOPICS["data_cleaning"],
                metric_message,
            )

            # 目前output_tag要求必须带有时间戳，所以其长度若小于等于10，则认为该tag无效，不参与数据对账
            if len(tag) <= 10:
                return

            if self.check_is_batch_loss_metric("output", base_tags):
                topic = DMONITOR_TOPICS["dmonitor_batch_data_loss"]
            else:
                topic = DMONITOR_TOPICS["dmonitor_data_loss"]
            self.produce_metric(
                topic,
                metric_message,
            )

    def handle_drop_metrics(self, message, metric_time, base_tags):
        """处理数据丢失中主动丢弃相关的指标

        :param message: 埋点内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        data_loss_data = (
            message.get("metrics", {}).get("data_monitor", {}).get("data_loss", {})
            or {}
        )
        drop_data = data_loss_data.get("data_drop", {}) or {}
        for drop_key, drop_item in drop_data.items():
            tags = {"drop_tag": drop_key}
            tags.update(base_tags)
            data_loss_drop = {
                "time": metric_time,
                "database": "monitor_data_metrics",
                "data_loss_drop": {
                    "data_cnt": drop_item.get("cnt", 0),
                    "reason": drop_item.get("reason", ""),
                    "tags": tags,
                },
            }

            target = data_loss_drop["data_loss_drop"]
            for big_tag in BIG_DIMENSIONS:
                if big_tag in target.get("tags", {}):
                    big_tag_value = target.get("tags", {}).get(big_tag, "")
                    target[big_tag] = big_tag_value
                    del target["tags"][big_tag]

            metric_message = json.dumps(data_loss_drop)
            self.produce_metric(
                DMONITOR_TOPICS["data_cleaning"],
                metric_message,
            )
            self.produce_metric(
                DMONITOR_TOPICS["dmonitor_data_drop"],
                metric_message,
            )

    def handle_delay_metrics(self, message, metric_time, base_tags):
        """处理数据延迟相关的指标信息

        :param message: 埋点内容
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        delay_data = (
            message.get("metrics", {}).get("data_monitor", {}).get("data_delay", {})
            or {}
        )
        version = message.get("version", "1.0")
        module = base_tags.get("module", "")
        component = base_tags.get("component", "")
        if version == "1.0" and module == "collector" and component in ("agent",):
            return

        data_delay_max = {
            "time": metric_time,
            "database": "monitor_data_metrics",
            "data_delay_max": {
                "window_time": int(delay_data.get("window_time") or 0),
                "waiting_time": int(delay_data.get("waiting_time") or 0),
                "data_time": int(delay_data.get("max_delay", {}).get("data_time") or 0),
                "delay_time": int(
                    delay_data.get("max_delay", {}).get("delay_time") or 0
                ),
                "output_time": int(
                    delay_data.get("max_delay", {}).get("output_time") or 0
                ),
                "tags": base_tags,
            },
        }
        data_delay_min = {
            "time": metric_time,
            "database": "monitor_data_metrics",
            "data_delay_min": {
                "window_time": int(delay_data.get("window_time") or 0),
                "waiting_time": int(delay_data.get("waiting_time") or 0),
                "data_time": int(delay_data.get("min_delay", {}).get("data_time") or 0),
                "delay_time": int(
                    delay_data.get("min_delay", {}).get("delay_time") or 0
                ),
                "output_time": int(
                    delay_data.get("min_delay", {}).get("output_time") or 0
                ),
                "tags": base_tags,
            },
        }

        target = data_delay_max["data_delay_max"]
        for big_tag in BIG_DIMENSIONS:
            if big_tag in target.get("tags", {}):
                big_tag_value = target.get("tags", {}).get(big_tag, "")
                target[big_tag] = big_tag_value
                del target["tags"][big_tag]

        max_metric_message = json.dumps(data_delay_max)
        self.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            max_metric_message,
        )
        self.produce_metric(
            DMONITOR_TOPICS["dmonitor_data_delay"],
            max_metric_message,
        )

        target = data_delay_min["data_delay_min"]
        for big_tag in BIG_DIMENSIONS:
            if big_tag in target.get("tags", {}):
                big_tag_value = target.get("tags", {}).get(big_tag, "")
                target[big_tag] = big_tag_value
                del target["tags"][big_tag]

        min_metric_message = json.dumps(data_delay_min)
        self.produce_metric(
            DMONITOR_TOPICS["data_cleaning"],
            min_metric_message,
        )
        self.produce_metric(
            DMONITOR_TOPICS["dmonitor_data_delay"],
            min_metric_message,
        )

    def check_is_batch_loss_metric(self, data_directing, base_tags):
        """检查是否离线丢失指标

        :param data_directing: 数据流向，input or output
        :param base_tags: 基本维度信息

        :return: 是否离线丢失指标
        """
        if base_tags["module"] == "batch":
            return True

        if (
            base_tags["module"] == "puller"
            and base_tags["component"] == "bkhdfs"
            and data_directing == "input"
        ):
            return True

        if (
            base_tags["module"] == "shipper"
            and base_tags["component"] == "hdfs"
            and data_directing == "output"
        ):
            return True

        return False

    def check_is_batch_metric(self, base_tags):
        """检查是否是离线指标

        :param base_tags: 基本维度信息

        :return: 是否离线指标
        """
        if base_tags["module"] == "batch":
            return True
        return False

    def check_is_shipper_or_puller_metric(self, version, module, component):
        """检查是否是shipper或者puller的指标

        :param version: 版本
        :param module: 模块
        :param component: 组件

        :return: 是否是shipper或者puller的指标
        """
        if version.startswith("2"):
            if module in ("shipper", "puller"):
                return True
        else:
            if (
                module == "databus"
                and "clean" not in component
                and "eslog" not in component
            ):
                return True
        return False

    def handle_data_profiling_metrics(self, message, metric_time, base_tags):
        """处理数据剖析相关的指标信息

        :param message: 指标消息体
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        data_profiling_metrics = (
            message.get("metrics", {}).get("data_profiling", {}) or {}
        )
        if "data_structure" in data_profiling_metrics:
            self.handle_data_structure_metrics(
                data_profiling_metrics["data_structure"], metric_time, base_tags
            )

    def handle_data_structure_metrics(
        self, data_structure_metrics, metric_time, base_tags
    ):
        """处理数据结构相关的指标

        :param data_structure_metrics 数据结构相关的指标
        :param base_tags: 基本维度信息
        """
        if "data_malformed" in data_structure_metrics:
            self.handle_data_malformed_metrics(
                data_structure_metrics["data_malformed"], metric_time, base_tags
            )

    def handle_data_malformed_metrics(
        self, data_malformed_metrics, metric_time, base_tags
    ):
        """处理数据结构异常的指标

        :param data_malformed_metrics: 数据结构异常指标
        :param metric_time: 指标时间
        :param base_tags: 基本维度信息
        """
        for field, metric_info in data_malformed_metrics.items():
            tags = {"field": field}
            tags.update(base_tags)
            data_structure_mailformed = {
                "time": metric_time,
                "database": "monitor_data_metrics",
                "data_structure_malformed": {
                    "data_cnt": metric_info.get("cnt", 0),
                    "reason": metric_info.get("reason", ""),
                    "tags": tags,
                },
            }

            metric_message = json.dumps(data_structure_mailformed)
            self.produce_metric(
                DMONITOR_TOPICS["data_cleaning"],
                metric_message,
            )
            self.produce_metric(
                DMONITOR_TOPICS["dmonitor_data_drop"],
                metric_message,
            )
