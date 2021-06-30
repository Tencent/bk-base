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
import time
from collections import OrderedDict

import gevent
from gevent import monkey

from utils.time import floor_minute, timetostr

from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.metrics.base import (
    DataLossInputTotal,
    DataLossOutputTotal,
    KafkaTopicMessageCnt,
)
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def data_interrupt_alert():
    logging.info("Start to execute data interrupt monitor task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "data_io_total",
            "partition": False,
            "group_id": "dmonitor_interrupt",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
        "concurrency": 1,
        "raw_data_source": "KAFKA",
    }

    try:
        task = DataInterruptAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init data interrupt alert task".format(
                error=e
            ),
            exc_info=True,
        )


class DataInterruptAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    DETECT_INTERVAL = 60
    PENDING_TIME = 600
    CACHE_REFRESH_INTERVAL = 300

    CHECK_WINDOW_CORRECTOR_TIME = 300
    DOWNSTREAM_TIMEOUT_CORRECTOR_TIME = 180
    LOCAL_TIMEOUT = 1200

    ALERT_CODE = AlertCode.DATA_INTERRUPT.value
    # 示例1: 数据开发任务[flow_name]-数据处理节点[node_name]的数据发生中断
    # 示例2: 数据开发任务[flow_name]-数据传输到节点[result_table_id(cluster_type)]的过程中发生中断
    # 示例3: 数据集成数据源[raw_data_name]-清洗任务[clean_task_name]的数据发生中断
    # 示例4: 数据集成数据源[raw_data_name]-入库任务[result_table_id(cluster_type)]的数据发生中断
    ALERT_MESSAGE_TEMPLATE = {
        "dataflow": {
            "data_processing": "数据开发任务[{flow_name}]-数据处理节点[{node_name}]的数据发生中断",
            "data_transferring": "数据开发任务[{flow_name}]-数据传输到节点[{data_set_id}({cluster_type})]的过程中发生中断",
        },
        "rawdata": {
            "data_processing": "数据集成数据源[{flow_name}]-清洗任务[{node_name}]的数据发生中断",
            "data_transferring": "数据集成数据源[{flow_name}]-入库任务[{data_set_id}({cluster_type})]的数据发生中断",
        },
    }
    ALERT_MESSAGE_EN_TEMPLATE = {
        "dataflow": {
            "data_processing": (
                "The data in DataFlow Task[{flow_name}]-"
                "Data Processing Node[{node_name}] is interrupted."
            ),
            "data_transferring": (
                "The data transferred to DataFlow Task[{flow_name}]-"
                "Node[{data_set_id}({cluster_type})] is interrupted."
            ),
        },
        "rawdata": {
            "data_processing": "The data in Raw Data[{flow_name}]-Clean Task[{node_name}] is interrupted.",
            "data_transferring": (
                "The data in Raw Data[{flow_name}]-"
                "Shipper Task[{data_set_id}({cluster_type})] is interrupted."
            ),
        },
    }

    def __init__(self, *args, **kwargs):
        """初始化生成延迟指标的任务

        :param task_configs: 缓存同步任务配置
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

        super(DataInterruptAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._check_window = configs.get("check_window", 600)
        self._debug_data_sets = configs.get("debug_data_sets", [])
        # 数据源数据量对账来源，对于海外版，支持使用kafka数据量统计的指标来进行中断告警监控
        self._raw_data_source = configs.get("raw_data_source", "KAFKA")

        # 输入输出窗口时间范围
        self._upstream_output_max_time = -self.CHECK_WINDOW_CORRECTOR_TIME
        self._upstream_output_min_time = -(
            self.CHECK_WINDOW_CORRECTOR_TIME + self._check_window
        )
        self._downstream_input_max_time = 0
        self._downstream_input_min_time = -(
            self.CHECK_WINDOW_CORRECTOR_TIME
            + self._check_window
            + self.DOWNSTREAM_TIMEOUT_CORRECTOR_TIME
        )

        self._metric_cache = {}  # 缓存对比用的指标
        self._data_operations = {}  # 数据处理或者数据传输元数据，主要用于确定数据流中上下游关联关系
        self._data_sets = {}  # 数据集元数据，主要用于确定数据处理或者数据传输所在的数据流信息(flow_id)
        self._raw_data_topics = (
            {}
        )  # 数据源的Topic元数据，主要用于数据源数据量对账来源(_raw_data_source)为KAFKA的情况
        self._flow_infos = {}  # 数据流元数据，主要用于触发告警时补充数据流相关信息

        # 任务并行度
        self._concurrency = configs.get("concurrency", 2)
        self._task_hash_value = configs.get("hash_value", 0)
        self._hash_value_cache = {}

        self._last_detect_time = now + self.PENDING_TIME
        self._cache_last_refresh_time = None

        self.refresh_metadata_cache(now)

    def refresh_metadata_cache(self, now):
        """刷新任务依赖的元数据信息

        :param now: 刷新后用于重置刷新计数器的时间戳
        """

        if (
            self._cache_last_refresh_time
            and now - self._cache_last_refresh_time < self.CACHE_REFRESH_INTERVAL
        ):
            return

        gevent.spawn(
            self.refresh_metadata,
            self._data_operations,
            self.fetch_data_operations_from_redis,
            update=False,
        )
        gevent.spawn(
            self.refresh_metadata,
            self._data_sets,
            self.fetch_data_set_infos_from_redis,
            update=False,
            callback=self.refresh_topic_info,
        )
        gevent.spawn(
            self.refresh_metadata,
            self._flow_infos,
            self.fetch_flow_infos_from_redis,
            update=False,
        )

        self._cache_last_refresh_time = now

    def refresh_topic_info(self, data_sets):
        """刷新数据源的Topic元数据信息

        :param data_sets: 数据集列表
        """
        # 当且仅当数据源指标使用“kafka数据量统计”的指标时，才需要用到Topic的元数据信息
        if self._raw_data_source != "KAFKA":
            return

        self._raw_data_topics = {}
        for data_set_id, data_set_info in data_sets.items():
            if data_set_info.get("data_set_type") == "raw_data":
                topic = data_set_info.get("topic")
                self._raw_data_topics[topic] = data_set_info

    def handle_monitor_value(self, message, now):
        """处理各个模块上报的任务埋点

        :param message: 延迟原始指标
            {
                "time": 1542960360.000001,
                "database": "monitor_data_metrics",
                "data_delay_max": {
                    "waiting_time": 1542960360,
                    "data_time": 1542960360,
                    "delay_time": 1542960360,
                    "output_time": 1542960360,
                    "tags": {
                        "module": "stream",
                        "component": "flink",
                        "cluster": null,
                        "storage": "channel_11",
                        "logical_tag": "591_test1119str",
                        "physical_tag": "171_1fe25fadfef54a4899d781fc9d1e55d3|591_test1119str|0"
                    }
                }
            }
        :param now: 当前处理数据的时间
        """
        try:
            if "data_loss_input_total" in message:
                self.parse_input_data(message, now)
            elif "data_loss_output_total" in message:
                self.parse_output_data(message, now)
            elif "kafka_topic_message_cnt" in message:
                self.parse_kafka_data(message, now)
        except Exception as e:
            logging.error(
                "Combine data error: %s, message: %s" % (e, json.dumps(message)),
                exc_info=True,
            )

    def parse_input_data(self, message, now):
        """
        解析数据输入量指标

        :param message: 延迟原始指标
        :param now: 当前处理数据的时间
        """
        metric = DataLossInputTotal.from_message(message)
        logical_key = (metric.get_tag("logical_tag"), metric.get_tag("storage"))

        if self._raw_data_source != "GSE" and metric.get_tag("module") == "collector":
            return

        if metric.get_tag("logical_tag") in self._debug_data_sets:
            logging.info("Debug input info: %s" % json.dumps(message))

        if logical_key not in self._metric_cache:
            self._metric_cache[logical_key] = {
                "input": {},
                "output": {},
                "output_buffer": {},
            }

        self._metric_cache[logical_key]["tags"] = metric.tags
        minute = floor_minute(metric.timestamp)
        if minute not in self._metric_cache[logical_key]["input"]:
            self._metric_cache[logical_key]["input"][minute] = {
                "metrics": [],
                "first_metric_local_time": now,
            }
        self._metric_cache[logical_key]["input"][minute]["metrics"].append(
            metric.get_metric("data_inc", 0)
        )

    def parse_output_data(self, message, now):
        """
        解析数据输出量指标

        :param message: 延迟原始指标
        :param now: 当前处理数据的时间
        """
        metric = DataLossOutputTotal.from_message(message)
        logical_key = (metric.get_tag("logical_tag"), metric.get_tag("storage"))

        if self._raw_data_source != "GSE" and metric.get_tag("module") == "collector":
            return

        if metric.get_tag("logical_tag") in self._debug_data_sets:
            logging.info("Debug output info: %s" % json.dumps(message))

        if logical_key not in self._metric_cache:
            self._metric_cache[logical_key] = {
                "input": {},
                "output": {},
                "output_buffer": {},
            }

        self._metric_cache[logical_key]["tags"] = metric.tags
        minute = floor_minute(metric.timestamp)
        if minute not in self._metric_cache[logical_key]["output_buffer"]:
            self._metric_cache[logical_key]["output_buffer"][minute] = {
                "metrics": [],
                "first_metric_local_time": now,
            }
        self._metric_cache[logical_key]["output_buffer"][minute]["metrics"].append(
            metric.get_metric("data_inc", 0)
        )

    def parse_kafka_data(self, message, now):
        """
        解析kafka输出数据量指标

        :param message: 延迟原始指标
        :param now: 当前处理数据的时间
        """
        if self._raw_data_source != "KAFKA":
            return

        metric = KafkaTopicMessageCnt.from_message(message)
        topic = metric.get_tag("topic")
        if topic not in self._raw_data_topics:
            return

        raw_data_info = self._raw_data_topics[topic]
        logical_tag = str(raw_data_info.get("data_set_id"))
        logical_key = (
            logical_tag,
            "channel_{}".format(raw_data_info.get("storage_channel_id")),
        )

        if logical_tag in self._debug_data_sets:
            logging.info("Debug output info: %s" % json.dumps(message))

        if logical_key not in self._metric_cache:
            self._metric_cache[logical_key] = {
                "input": {},
                "output": {},
                "output_buffer": {},
            }

        self._metric_cache[logical_key]["tags"] = metric.tags
        minute = floor_minute(metric.timestamp)
        if minute not in self._metric_cache[logical_key]["output_buffer"]:
            self._metric_cache[logical_key]["output_buffer"][minute] = {
                "metrics": [],
                "first_metric_local_time": now,
            }
        self._metric_cache[logical_key]["output_buffer"][minute]["metrics"].append(
            metric.get_metric("cnt", 0)
        )

    def do_monitor(self, now, task_pool):
        """
        执行检测逻辑

        :param now: 当前时间
        :param task_pool: 任务协程池，用于并发检测逻辑
        """
        if now - self._last_detect_time > self.DETECT_INTERVAL:
            self.move_output_metrics(now)
            self.monitor_metrics(now)
            self.clear_metrics(now)
            self._last_detect_time = now

    def move_output_metrics(self, now):
        """
        把上游节点的输出数据量从output_buffer中挪到output中, 以让上游output与下游input的数据可以进行中断的监控

        :param now: 当前时间
        """
        # 防止埋点缺少必要维度信息
        try:
            self.check_metadata(
                self._data_operations, self.CACHE_REFRESH_INTERVAL, "data_operations"
            )
        except Exception:
            self.refresh_metadata_cache(now)
            self._cache_last_refresh_time = now

        for logical_key, metrics in self._metric_cache.items():
            deleted_minutes = []
            for minute in metrics["output_buffer"].keys():
                if minute < now + self._upstream_output_max_time:
                    if minute in metrics["output"]:
                        metrics["output"][minute]["metrics"].extend(
                            metrics["output_buffer"][minute]["metrics"]
                        )
                    else:
                        metrics["output"][minute] = metrics["output_buffer"][minute]

                    deleted_minutes.append(minute)

            for minute in deleted_minutes:
                del metrics["output_buffer"][minute]

    def monitor_metrics(self, now):
        """
        执行监控逻辑，对比上游数据输出量及下游数据输入量指标

        :param now: 当前时间
        """
        logging.info(
            "There are {} data operation for monitor".format(len(self._data_operations))
        )
        checked_count = 0
        for data_operation_id, data_operation in self._data_operations.items():
            # 如果是以下节点类型, 则不进行检测
            if data_operation[
                "data_operation_type"
            ] == "data_processing" and data_operation["processing_type"] in [
                "batch",
                "batch_model",
                "stream_model",
                "storage",
                "model",
            ]:
                continue

            # 如果节点不在运行，则不进行检测
            if data_operation.get("status") not in ["running", "started"]:
                continue

            hash_value = self.get_data_operation_hash_value(data_operation_id)
            if hash_value != self._task_hash_value:
                continue

            checked_count += 1

            # 检查上游是否有输出
            upstream_has_output = self.check_upstream_has_output(data_operation)
            if not upstream_has_output:
                continue

            # 检查下游是否有输入
            downstream_has_input = self.check_downstream_has_input(data_operation)

            if upstream_has_output and (not downstream_has_input):
                self.generate_alert(data_operation_id, data_operation, now)
        logging.info("{} data operations has been checked".format(checked_count))

    def get_data_operation_hash_value(self, data_operation_id):
        """
        获取数据处理唯一标识的hash值，用于在并发任务下，标识是否需要检测当前的数据处理

        :param data_operation_id: 数据处理或数据处理的唯一标识

        :return: 数据处理或数据传输的hash值
        """
        hash_value = self._hash_value_cache.get(data_operation_id, None)

        if not hash_value:
            hash_value = (
                self.get_data_set_hash_value(data_operation_id) % self._concurrency
            )
            self._hash_value_cache[data_operation_id] = hash_value

        return hash_value

    def check_upstream_has_output(self, data_operation):
        """
        检测指定data_operation的上游是否有数据输出量

        :param data_operation: 数据处理或数据传输
        """
        for input in data_operation.get("inputs", []):
            input_logical_key = (input["data_set_id"], input["storage_key"])
            if input_logical_key not in self._metric_cache:
                return False
            # 如果上游没有任何输出数据，则不进行检测
            if len(self._metric_cache[input_logical_key]["output"].keys()) == 0:
                False
            for minute, metric_info in self._metric_cache[input_logical_key][
                "output"
            ].items():
                for upstream_point in metric_info.get("metrics", []):
                    if upstream_point > 0:
                        return True
        return False

    def check_downstream_has_input(self, data_operation):
        """
        检测指定data_operation的下游是否有数据输入量

        :param data_operation: 数据处理或数据传输
        """
        outputs = data_operation.get("outputs", [])
        if (
            data_operation["data_operation_type"] == "data_processing"
            and data_operation["processing_type"] == "stream"
            and len(outputs) > 0
        ):
            output_item = copy.copy(outputs[0])
            output_item["storage_key"] = "None_None"
            if output_item not in outputs:
                outputs.append(output_item)

        for output in outputs:
            output_logical_key = (output["data_set_id"], output["storage_key"])

            if output_logical_key not in self._metric_cache:
                return False
            # 如果上游没有任何输出数据，则不进行检测
            if len(self._metric_cache[output_logical_key]["input"].keys()) == 0:
                return False
            for minute, metric_info in self._metric_cache[output_logical_key][
                "input"
            ].items():
                for downstream_point in metric_info.get("metrics", []):
                    if downstream_point > 0:
                        return True
        return False

    def generate_alert(self, data_operation_id, data_operation, now):
        """
        产生告警

        :param data_operation_id: 数据处理或数据处理的唯一标识
        :param data_operation: 数据处理或数据传输
        :param now: 当前检测时间
        """
        self.debug_log(data_operation)

        try:
            flow_id = self.get_flow_id_by_data_operation(data_operation)
            bk_biz_id = self.get_bk_biz_id_by_data_operation(data_operation)
            flow_info = self._flow_infos.get(str(flow_id), {})
            if not flow_info:
                return
        except Exception:
            logging.error(
                "Failed to get metadata about data_operation({})".format(
                    data_operation_id
                )
            )
            return

        message, message_en = self.generate_alert_message(flow_info, data_operation)

        upstreams = sorted(self.get_upstreams_by_data_operation(data_operation))
        downstreams = sorted(self.get_downstreams_by_data_operation(data_operation))

        alert_info = {
            "time": now,
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "message": message,
                "message_en": message_en,
                "full_message": message,
                "full_message_en": message_en,
                "alert_status": AlertStatus.INIT.value,
                "recover_time": 0,
                "tags": {
                    "alert_level": AlertLevel.WARNING.value,
                    "alert_code": AlertCode.DATA_INTERRUPT.value,
                    "alert_type": AlertType.DATA_MONITOR.value,
                    "data_operation_id": data_operation_id,
                    "data_operation_type": data_operation.get("data_operation_type"),
                    "project_id": data_operation.get("project_id"),
                    "processing_type": data_operation.get("processing_type"),
                    "transferring_type": data_operation.get("transferring_type"),
                    "flow_id": flow_id,
                    "bk_biz_id": bk_biz_id,
                    "upstreams": ",".join(upstreams),
                    "downstreams": ",".join(downstreams),
                },
            },
        }

        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_raw_alert"], alert_message)

    def debug_log(self, data_operation):
        """
        打印调试用数据集的日志

        :param data_operation: 数据处理或数据传输
        """
        for input in data_operation.get("inputs", []):
            if input["data_set_id"] in self._debug_data_sets:
                input_logical_key = (input["data_set_id"], input["storage_key"])
                self.display_metric_cache(input_logical_key)
        for output in data_operation.get("outputs", []):
            if output["data_set_id"] in self._debug_data_sets:
                output_logical_key = (output["data_set_id"], output["storage_key"])
                self.display_metric_cache(output_logical_key)

    def generate_alert_message(self, flow_info, data_operation):
        """
        生成告警信息

        :param flow_info: 数据流信息
        :param data_operation: 数据处理或数据传输
        """
        flow_type = flow_info.get("flow_type")
        data_operation_type = data_operation.get("data_operation_type")

        template = self.ALERT_MESSAGE_TEMPLATE.get(flow_type, {}).get(
            data_operation_type
        )
        template_en = self.ALERT_MESSAGE_EN_TEMPLATE.get(flow_type, {}).get(
            data_operation_type
        )
        if not template or not template_en:
            logging.error(
                "There is no template for the Flow({}) and DataOperation({})".format(
                    flow_type,
                    data_operation_type,
                )
            )
            return

        flow_name = flow_info.get("flow_name")
        if data_operation_type == "data_processing":
            message = template.format(
                flow_name=flow_name, node_name=data_operation.get("node_name")
            )
            message_en = template_en.format(
                flow_name=flow_name, node_name=data_operation.get("node_name")
            )
        else:
            output_info = data_operation.get("outputs")[0]
            params = {
                "flow_name": flow_name,
                "data_set_id": output_info.get("data_set_id"),
                "cluster_type": output_info.get("cluster_type"),
            }
            message = template.format(**params)
            message_en = template_en.format(**params)
        return message, message_en

    def clear_metrics(self, now):
        """
        清理缓存的指标

        :param now: 当前数据处理时间
        """
        for logical_key, metrics in self._metric_cache.items():
            # 清除当前logical_key的输入埋点
            clearing_input_minutes = []
            for minute, metric_info in metrics["input"].items():
                if minute < now + self._downstream_input_min_time:
                    clearing_input_minutes.append(minute)
                elif metric_info["first_metric_local_time"] < now - self.LOCAL_TIMEOUT:
                    clearing_input_minutes.append(minute)

            for minute in clearing_input_minutes:
                del metrics["input"][minute]

            # 清除当前logical_key的输出埋点
            clearing_output_minutes = []
            for minute, metric_info in metrics["output"].items():
                if minute < now + self._upstream_output_min_time:
                    clearing_output_minutes.append(minute)
                elif metric_info["first_metric_local_time"] < now - self.LOCAL_TIMEOUT:
                    clearing_output_minutes.append(minute)

            for minute in clearing_output_minutes:
                del metrics["output"][minute]

            # 清除当前logical_key的输出缓存埋点
            clearing_output_buffer_minutes = []
            for minute, metric_info in metrics["output_buffer"].items():
                if metric_info["first_metric_local_time"] < now - self.LOCAL_TIMEOUT:
                    clearing_output_buffer_minutes.append(minute)

            for minute in clearing_output_buffer_minutes:
                del metrics["output_buffer"][minute]

    def get_flow_id_by_data_operation(self, data_operation):
        """
        获取数据处理或数据传输的flow_id

        :param data_operation: 数据处理或数据传输
        """
        for output in data_operation["outputs"]:
            data_set_id = output["data_set_id"]
            if (
                data_set_id in self._data_sets
                and "flow_id" in self._data_sets[data_set_id]
            ):
                return self._data_sets[data_set_id]["flow_id"]

        return None

    def get_bk_biz_id_by_data_operation(self, data_operation):
        """
        获取数据处理或数据传输的业务ID

        :param data_operation: 数据处理或数据传输
        """
        bk_biz_id_sets = set()
        for output in data_operation["outputs"]:
            data_set_id = output["data_set_id"]
            if (
                data_set_id in self._data_sets
                and "bk_biz_id" in self._data_sets[data_set_id]
            ):
                bk_biz_id_sets.add(self._data_sets[data_set_id]["bk_biz_id"])
        if len(bk_biz_id_sets) == 1:
            return list(bk_biz_id_sets)[0]
        else:
            return None

    def get_upstreams_by_data_operation(self, data_operation):
        """
        获取数据处理或数据传输的上游信息

        :param data_operation: 数据处理或数据传输
        """
        upstreams = []
        for input in data_operation.get("inputs", []):
            upstreams.append(
                "{data_set_id}_{storage_key}".format(
                    data_set_id=input.get("data_set_id"),
                    storage_key=input.get("storage_key"),
                )
            )
        return upstreams

    def get_downstreams_by_data_operation(self, data_operation):
        """
        获取数据处理或数据传输的下游信息

        :param data_operation: 数据处理或数据传输
        """
        downstreams = []
        for output in data_operation.get("outputs", []):
            downstreams.append(
                "{data_set_id}_{storage_key}".format(
                    data_set_id=output.get("data_set_id"),
                    storage_key=output.get("storage_key"),
                )
            )
        return downstreams

    def display_metric_cache(self, logical_key):
        """
        展示当前逻辑标识缓存的用于检测的数据

        :param logical_key: 逻辑标识
        """
        if logical_key[0] not in self._debug_data_sets:
            return

        if logical_key not in self._metric_cache:
            logging.info("Logical_key: %s is empty" % str(logical_key))
            return

        cache_data = copy.deepcopy(self._metric_cache[logical_key])
        del cache_data["tags"]

        if logical_key[1].startswith("channel"):
            del cache_data["input"]
            for direct in cache_data.keys():
                temp_dict = OrderedDict({})
                for timestamp in sorted(cache_data[direct].keys()):
                    temp_dict[timetostr(int(timestamp))] = cache_data[direct][timestamp]
                cache_data[direct] = temp_dict
            logging.info(
                "Logical_key: %s, cache_data: %s"
                % (logical_key, json.dumps(cache_data, indent=4))
            )
        elif logical_key[1].startswith("storage"):
            del cache_data["output"]
            del cache_data["output_buffer"]
            for direct in cache_data.keys():
                temp_dict = OrderedDict({})
                for timestamp in sorted(cache_data[direct].keys()):
                    temp_dict[timetostr(int(timestamp))] = cache_data[direct][timestamp]
                cache_data[direct] = temp_dict
            logging.info(
                "Logical_key: %s, cache_data: %s"
                % (logical_key, json.dumps(cache_data, indent=4))
            )
