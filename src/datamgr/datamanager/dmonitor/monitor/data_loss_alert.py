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

from gevent import monkey

from utils.time import timetostr

from dmonitor.alert.alert_codes import AlertCode, AlertLevel, AlertStatus, AlertType
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def data_loss_alert():
    logging.info("Start to execute data loss monitor task")

    task_configs = {
        "consumer_configs": {
            "type": "kafka",
            "alias": "op",
            "topic": "dmonitor_loss_audit",
            "partition": False,
            "group_id": "dmonitor",
            "batch_message_max_count": 100000,
            "batch_message_timeout": 5,
        },
        "task_pool_size": 50,
    }

    try:
        task = DataLossAlertTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            "Raise exception({error}) when init data loss alert task".format(error=e),
            exc_info=True,
        )


class DataLossAlertTaskGreenlet(BaseDmonitorTaskGreenlet):
    DETECT_INTERVAL = 60
    PENDING_TIME = 600

    # 上下游超时时间不一致主要是为了防止检测过程中，本身埋点出现下游比上游先到的情况
    # 导致下游先超时，上游未超时，这种情况会造成丢数据的假象
    # 所以下游埋点增加一定的超时Buffer以允许出现下游埋点比上游埋点先到的情况
    UPSTREAM_TAG_TIMEOUT = 600
    DOWNSTREAM_TAG_TIMEOUT = 900

    ALERT_CODE = AlertCode.DATA_LOSS.value
    ALERT_MESSAGE = "{entity_display}在{time_str}丢失数据{loss_cnt}条"
    ALERT_MESSAGE_EN = "Data had lost {loss_cnt} at {time_str}"
    ALERT_FULL_MESSAGE = "{entity_display}({logical_tag})在{time_str}丢失数据{loss_cnt}条"
    ALERT_FULL_MESSAGE_EN = "Data about {entity_display_en}({logical_tag}) had lost {loss_cnt} at {time_str}"

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

        super(DataLossAlertTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get("consumer_configs"))
        self.init_task_pool(configs.get("task_pool_size"))

        now = time.time()

        self._debug_data_sets = configs.get("debug_data_sets", [])
        self._metric_cache = {"inputs": {}, "outputs": {}}
        self._last_detect_time = now + self.PENDING_TIME

        self.reset_opdata()

    def refresh_metadata_cache(self, now):
        """刷新数据延迟监控依赖的元数据信息

        :param now: 当前刷新缓存的时间
        """
        pass

    def reset_opdata(self):
        self._loss_opdata = {
            "total_count": 0,
            "fail_count": 0,
            "total_paths": set(),
            "fail_paths": set(),
        }

    def report_opdata(self, now):
        total_paths_count = len(self._loss_opdata["total_paths"])
        fail_paths_count = len(self._loss_opdata["fail_paths"])
        succ_path_count = total_paths_count - fail_paths_count
        total_audit_count = self._loss_opdata["total_count"]
        fail_audit_count = self._loss_opdata["fail_count"]
        succ_audit_count = total_audit_count - fail_audit_count
        metric_info = {
            "time": now,
            "database": "monitor_custom_metrics",
            "dmonitor_loss_audit_opdata": {
                "succ_audit_count": succ_audit_count,
                "fail_audit_count": fail_audit_count,
                "total_audit_count": total_audit_count,
                "succ_paths_count": succ_path_count,
                "fail_paths_count": fail_paths_count,
                "total_paths_count": total_paths_count,
                "tags": {"task": "dmonitor_loss_audit"},
            },
        }

        metric_message = json.dumps(metric_info)
        self.produce_metric(DMONITOR_TOPICS["data_cleaning"], metric_message)

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
            if (
                "data_loss_input" in message
                and "input_tag" in message["data_loss_input"]
            ):
                message_info = message["data_loss_input"]
                tags = message_info.get("tags", {})
                module = tags.get("module", "")
                component = tags.get("component", "")
                logical_tag = tags.get("logical_tag", "")
                input_tag = message_info.get("input_tag", "")
                point_key = "{}_{}_{}".format(module, component, logical_tag)

                if input_tag not in self._metric_cache["inputs"]:
                    self._metric_cache["inputs"][input_tag] = {}
                if point_key not in self._metric_cache["inputs"][input_tag]:
                    self._metric_cache["inputs"][input_tag][point_key] = {
                        "tags": tags,
                        "metrics": [],
                    }
                self._metric_cache["inputs"][input_tag][point_key]["metrics"].append(
                    {
                        "data_cnt": message_info["data_cnt"],
                        "tag_time": int(message["time"]),
                        "audit_time": now,
                    }
                )

                # 排查数据丢失任务日志
                if logical_tag in self._debug_data_sets:
                    logging.info("Debug input info: %s" % json.dumps(message))
            elif (
                "data_loss_output" in message
                and "output_tag" in message["data_loss_output"]
            ):
                message_info = message["data_loss_output"]
                tags = message_info.get("tags", {})
                module = tags.get("module", "")
                component = tags.get("component", "")
                logical_tag = tags.get("logical_tag", "")
                output_tag = message_info.get("output_tag", "")
                point_key = "{}_{}_{}".format(module, component, logical_tag)

                if point_key not in self._metric_cache["outputs"]:
                    self._metric_cache["outputs"][point_key] = {
                        "tags": tags,
                        "output_tags": {},
                    }
                if (
                    output_tag
                    not in self._metric_cache["outputs"][point_key]["output_tags"]
                ):
                    self._metric_cache["outputs"][point_key]["output_tags"][
                        output_tag
                    ] = {
                        "data_cnt": 0,
                        "ckp_drop_cnt": 0,
                        "tag_time": int(message["time"]),
                        "audit_time": now,
                    }

                self._metric_cache["outputs"][point_key]["output_tags"][output_tag][
                    "data_cnt"
                ] += message_info.get("data_cnt", 0)
                self._metric_cache["outputs"][point_key]["output_tags"][output_tag][
                    "ckp_drop_cnt"
                ] += message_info.get("ckp_drop_cnt", 0)
                self._metric_cache["outputs"][point_key]["output_tags"][output_tag][
                    "audit_time"
                ] = now

                # 排查数据丢失任务日志
                if logical_tag in self._debug_data_sets:
                    logging.info("Debug output info: %s" % json.dumps(message))
        except Exception as e:
            logging.error(
                "Combine data error: %s, message: %s" % (e, json.dumps(message)),
                exc_info=True,
            )

    def do_monitor(self, now, task_pool):
        if now - self._last_detect_time > self.DETECT_INTERVAL:
            self.reset_opdata()
            self.monitor_metrics(now)
            self.clear_input_metrics(now)
            self.report_opdata(now)
            self._last_detect_time = now

    def monitor_metrics(self, now):
        for point_key, point_metrics in self._metric_cache["outputs"].items():
            # 以output为主线，每个tag对账
            timeout_output_tags = []
            node = {
                "module": point_metrics["tags"].get("module"),
                "component": point_metrics["tags"].get("component"),
                "cluster": point_metrics["tags"].get("cluster"),
                "storage": point_metrics["tags"].get("storage"),
                "logical_tag": point_metrics["tags"].get("logical_tag"),
            }
            for output_tag, output_metrics in point_metrics["output_tags"].items():
                output_cnt = output_metrics["data_cnt"]
                ckp_drop_cnt = output_metrics["ckp_drop_cnt"]
                output_time = output_metrics["tag_time"]
                audit_time = output_metrics["audit_time"]

                # 对账是否超时
                if now <= audit_time + self.UPSTREAM_TAG_TIMEOUT:
                    continue
                timeout_output_tags.append(output_tag)

                input_metrics = self._metric_cache["inputs"].get(output_tag, {})
                correct_input_point_keys = []
                # 对每个消费者的输入进行对账
                for input_point_key, input_info in input_metrics.items():
                    input_cnt = 0
                    to_node = {
                        "module": input_info["tags"].get("module"),
                        "component": input_info["tags"].get("component"),
                        "cluster": input_info["tags"].get("cluster"),
                        "storage": input_info["tags"].get("storage"),
                        "logical_tag": input_info["tags"].get("logical_tag"),
                    }
                    for input_metric in input_info["metrics"]:
                        input_cnt += input_metric["data_cnt"]

                    path_key = self.gen_path_key(node, to_node)
                    self._loss_opdata["total_paths"].add(path_key)
                    self._loss_opdata["total_count"] += 1

                    # 如果对账准确，则清除当前的tag
                    if input_cnt == output_cnt:
                        correct_input_point_keys.append(input_point_key)
                        continue

                    # 如果上游输出为0，则记录异常并跳过当次对账
                    if output_cnt == 0:
                        continue

                    # 按不同的数据消费者写入数据丢失的指标中
                    self._loss_opdata["fail_count"] += 1
                    self._loss_opdata["fail_paths"].add(path_key)
                    loss_cnt = output_cnt - input_cnt
                    loss_rate = loss_cnt * 100.0 / output_cnt
                    metric_info = {
                        "time": output_time + 1e-9 * random.randint(0, 1000000),
                        "database": "monitor_data_metrics",
                        "data_loss_audit": {
                            "ckp_drop_cnt": ckp_drop_cnt,
                            "output_cnt": output_cnt,
                            "consume_cnt": input_cnt,
                            "loss_cnt": loss_cnt,
                            "loss_rate": loss_rate,
                            "loss_tag": output_tag,
                            "tags": {
                                "src_module": node.get("module", ""),
                                "src_component": node.get("component", ""),
                                "src_logical_tag": node.get("logical_tag", ""),
                                "src_storage": node.get("storage", ""),
                                "src_cluster": node.get("cluster", ""),
                                "dst_module": to_node.get("module", ""),
                                "dst_component": to_node.get("component", ""),
                                "dst_logical_tag": to_node.get("logical_tag", ""),
                                "dst_storage": to_node.get("storage", ""),
                                "dst_cluster": to_node.get("cluster", ""),
                                "data_set_id": to_node.get("logical_tag", ""),
                                "storage": to_node.get("storage", ""),
                                "path_key": path_key,
                            },
                        },
                    }
                    base_tags = input_info.get("tags", {})
                    metric_info["data_loss_audit"]["tags"].update(base_tags)

                    # 排查数据丢失任务日志
                    if (
                        node.get("logical_tag", "") in self._debug_data_sets
                        or to_node.get("logical_tag", "") in self._debug_data_sets
                    ):
                        logging.info("Data loss metric: %s" % json.dumps(metric_info))

                    metric_message = json.dumps(metric_info)
                    self.produce_metric(
                        DMONITOR_TOPICS["data_cleaning"], metric_message
                    )
                    self.produce_metric(
                        DMONITOR_TOPICS["data_loss_metric"], metric_message
                    )
                    self.generate_raw_alert(metric_info, base_tags, now)

                # 清理对账完成的input_tag
                for input_point_key in correct_input_point_keys:
                    del self._metric_cache["inputs"][output_tag][input_point_key]

            # 清理超时对账信息
            for output_tag in timeout_output_tags:
                if output_tag in self._metric_cache["inputs"]:
                    del self._metric_cache["inputs"][output_tag]
                del self._metric_cache["outputs"][point_key]["output_tags"][output_tag]

        empty_point_keys = []
        for point_key in self._metric_cache["outputs"].keys():
            if len(self._metric_cache["outputs"][point_key]["output_tags"]) == 0:
                empty_point_keys.append(point_key)
        for point_key in empty_point_keys:
            del self._metric_cache["outputs"][point_key]

        return True

    def generate_raw_alert(self, metric, base_tags, now):
        time_str = timetostr(metric["time"])
        loss_cnt = metric["data_loss_audit"].get("loss_cnt", 0)

        logical_tag = str(base_tags.get("logical_tag"))
        entity_display, entity_display_en = self.get_logical_tag_display(
            logical_tag, base_tags
        )

        message = self.ALERT_MESSAGE.format(
            entity_display=entity_display,
            time_str=time_str,
            loss_cnt=loss_cnt,
        )
        message_en = self.ALERT_MESSAGE_EN.format(
            time_str=time_str,
            loss_cnt=loss_cnt,
        )
        full_message = self.ALERT_FULL_MESSAGE.format(
            entity_display=entity_display,
            logical_tag=logical_tag,
            time_str=time_str,
            loss_cnt=loss_cnt,
        )
        full_message_en = self.ALERT_FULL_MESSAGE_EN.format(
            entity_display_en=entity_display_en,
            logical_tag=logical_tag,
            time_str=time_str,
            loss_cnt=loss_cnt,
        )

        alert_info = {
            "time": now,
            "database": "monitor_data_metrics",
            "dmonitor_alerts": {
                "message": message,
                "message_en": message_en,
                "full_message": full_message,
                "full_message_en": full_message_en,
                "alert_status": AlertStatus.INIT.value,
                "tags": {
                    "alert_level": AlertLevel.DANGER.value,
                    "alert_code": AlertCode.DATA_LOSS.value,
                    "alert_type": AlertType.DATA_MONITOR.value,
                },
            },
        }
        alert_info["dmonitor_alerts"]["tags"].update(metric["data_loss_audit"]["tags"])
        alert_message = json.dumps(alert_info)
        self.produce_metric(DMONITOR_TOPICS["dmonitor_raw_alert"], alert_message)

    def gen_path_key(self, from_node, to_node):
        if type(from_node) is dict:
            from_node_key = "{}_{}_{}_{}_{}".format(
                from_node["module"],
                from_node["component"],
                from_node["cluster"],
                from_node["storage"],
                from_node["logical_tag"],
            )
        else:
            from_node_key = "{}_{}_{}_{}_{}".format(
                from_node.module,
                from_node.component,
                from_node.cluster,
                from_node.storage,
                from_node.logical_tag,
            )
        if type(to_node) is dict:
            to_node_key = "{}_{}_{}_{}_{}".format(
                to_node["module"],
                to_node["component"],
                to_node["cluster"],
                to_node["storage"],
                to_node["logical_tag"],
            )
        else:
            to_node_key = "{}_{}_{}_{}_{}".format(
                to_node.module,
                to_node.component,
                to_node.cluster,
                to_node.storage,
                to_node.logical_tag,
            )
        return "{}->{}".format(from_node_key, to_node_key)

    def clear_input_metrics(self, now):
        timeout_tags = []
        for input_tag, tag_info in self._metric_cache["inputs"].items():
            tag_timeout = True
            for point_key, point_info in tag_info.items():
                for metric in point_info["metrics"]:
                    if now - metric["audit_time"] < self.DOWNSTREAM_TAG_TIMEOUT:
                        tag_timeout = False
                    if not tag_timeout:
                        break
                if not tag_timeout:
                    break
            if tag_timeout:
                for point_key, point_info in tag_info.items():
                    if point_info["tags"].get("logical_tag") in self._debug_data_sets:
                        logging.info(
                            "Input tag(%s) timeout, content: %s"
                            % (input_tag, json.dumps(point_info))
                        )
                timeout_tags.append(input_tag)
        for input_tag in timeout_tags:
            del self._metric_cache["inputs"][input_tag]
