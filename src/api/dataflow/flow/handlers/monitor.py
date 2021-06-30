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

import datetime
import time

from django.utils import translation
from django.utils.translation import ugettext as _

from dataflow.flow.exceptions import NodeValidError
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.models import FlowNodeRelation
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.utils.concurrency import concurrent_call_func
from dataflow.pizza_settings import BATCH_MODULE_NAME, STREAM_MODULE_NAME
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper

"""
监控相关
数据流监控：
1. 获取整个 flow 的状态
2. 获取 flow 中单个节点的详细状态
"""
DMONITOR_ALERTS = "dmonitor_alerts"
DATA_LOSS_IO_TOTAL = "data_loss_io_total"
RESULT_TABLE_STATUS = "rt_status"
DATA_DELAY_MAX = "data_delay_max"
DATA_TREND = "data_trend"
STREAM_UNIT = _("条/分钟")
# 间隔1天
INTERVAL = 1
# 1 小时，60 分钟
START_TIME_ONE_HOUR = "now() - 1h"
# 1 天
START_TIME_ONE_DAY = "now() - 1d"
# 7 天，一周
START_TIME_ONE_WEEK = "now() - 7d"
# 30 天，一个月
START_TIME_ONE_MONTH = "now() - 30d"
# 90 天，三个月
START_TIME_THREE_MONTH = "now() - 90d"
# 常量
ONE_DAY_SECOND = 24 * 60 * 60
PERIOD_HOUR = "hour"
PERIOD_DAY = "day"
PERIOD_WEEK = "week"
PERIOD_MONTH = "month"


class MonitorHandler(object):
    # 各种节点支持的打点类型
    DEFAULT_NODE_MONITOR_MAP = {
        # 实时类别埋点
        NodeTypes.STREAM_SOURCE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.STREAM: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.DATA_MODEL_APP: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.DATA_MODEL_STREAM_INDICATOR: [
            DATA_DELAY_MAX,
            DATA_TREND,
        ],
        # 离线类别埋点
        NodeTypes.BATCH_SOURCE: [RESULT_TABLE_STATUS, DATA_TREND],
        NodeTypes.BATCH: [RESULT_TABLE_STATUS, DATA_TREND],
        NodeTypes.BATCHV2: [RESULT_TABLE_STATUS, DATA_TREND],
        NodeTypes.DATA_MODEL_BATCH_INDICATOR: [
            RESULT_TABLE_STATUS,
            DATA_TREND,
        ],
        NodeTypes.MODEL_APP: [RESULT_TABLE_STATUS, DATA_TREND],
        # 存储节点
        NodeTypes.MYSQL_STORGAE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.POSTGRESQL_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.HDFS_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.HERMES_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.DRUID_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.TREDIS_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.IGNITE_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.TSPIDER_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.TCAPLUS_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.CLICKHOUSE_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.QUEUE_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.QUEUE_PULSAR_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.ES_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        NodeTypes.TSDB_STORAGE: [DATA_DELAY_MAX, DATA_TREND]
        # 暂无埋点
        # NodeTypes.KV_SOURCE: [DATA_DELAY_MAX, DATA_TREND],
        # NodeTypes.UNIFIED_KV_SOURCE: [DATA_DELAY_MAX, DATA_TREND],
        # NodeTypes.TDW_BATCH: [RESULT_TABLE_STATUS, DATA_TREND],
        # NodeTypes.TDW_JAR_BATCH: [RESULT_TABLE_STATUS, DATA_TREND],
        # NodeTypes.TDW_STORAGE: [DATA_DELAY_MAX, DATA_TREND],
        # NodeTypes.PROCESS_MODEL: [DATA_DELAY_MAX, DATA_TREND, RESULT_TABLE_STATUS],
        # NodeTypes.MODEL_TS_CUSTOM: [DATA_DELAY_MAX, DATA_TREND, RESULT_TABLE_STATUS]
    }

    # 实时类埋点节点
    STREAM_NODE_MONITOR = [
        NodeTypes.STREAM_SOURCE,
        NodeTypes.STREAM,
        NodeTypes.DATA_MODEL_APP,
        NodeTypes.DATA_MODEL_STREAM_INDICATOR,
    ]

    # 离线类埋点
    BATCH_NODE_MONITOR = [
        NodeTypes.BATCH_SOURCE,
        NodeTypes.BATCH,
        NodeTypes.BATCHV2,
        NodeTypes.DATA_MODEL_BATCH_INDICATOR,
        NodeTypes.MODEL_APP,
    ]

    @staticmethod
    def get_monitor_config_by_node(node):
        node_type = node.node_type
        if node_type in NodeTypes.SOURCE_CATEGORY:
            # 获取rt结果表在其它flow的节点类型
            origin_rt = FlowNodeRelation.objects.filter(
                result_table_id=node.result_table_ids[0],
                node_type__in=NodeTypes.CALC_CATEGORY,
            )
            if origin_rt is None or origin_rt.count() == 0:
                # 若数据源节点非RT数据源
                return {}
            else:
                node_type = origin_rt[0].node_type
        if node_type in [
            NodeTypes.STREAM,
            NodeTypes.DATA_MODEL_APP,
            NodeTypes.DATA_MODEL_STREAM_INDICATOR,
        ]:
            return {"module": STREAM_MODULE_NAME, "component": "*"}
        elif node_type in NodeTypes.BATCH_CATEGORY:
            return {
                "module": BATCH_MODULE_NAME,
                "component": "*",
            }
        else:
            return {}

    def __init__(self):
        pass

    def list_monitor_data(self):
        """
        获得监控打点集合下指定类型的数据
        """
        raise NotImplementedError("`list_monitor_data()` must be implemented.")

    def get_input_output_loss_node_handler(self, node_handler, input_output_tag, data_format="series"):
        """
        根据节点，获取它的 input、output 指标
        input_output_tag = input_count/output_count
        format = 'series' : 取序列值，时间戳 + 字段
        format = 'value' : 取求和后的值，没有时间戳
        """
        node_type = node_handler.node_type
        if node_type in self.STREAM_NODE_MONITOR:
            return MonitorUtil.get_stream_input_output_metrics(node_handler, input_output_tag, data_format)
        elif node_type in self.BATCH_NODE_MONITOR:
            # 离线计算打点
            return MonitorUtil.get_batch_input_output_metrics(node_handler, input_output_tag, data_format)
        else:
            # 存储节点，看它的上游是实时还是离线类型节点，再定指标
            from_process_node = NodeUtils.get_storage_upstream_node_handler(node_handler.node_id)
            if from_process_node.node_type in self.STREAM_NODE_MONITOR:
                return MonitorUtil.get_stream_input_output_metrics(node_handler, input_output_tag, data_format)
            elif from_process_node.node_type in self.BATCH_NODE_MONITOR:
                return MonitorUtil.get_batch_input_output_metrics(node_handler, input_output_tag, data_format)
            else:
                return {}


class MonitorUtil(object):
    # -------- 组装 json 工具类
    @staticmethod
    def merge_input_and_output_data(input_dict, output_dict, result_table_id):
        if not input_dict:
            return {}
        input_dict["output"] = output_dict["output"]
        input_dict["data"][result_table_id]["output"] = output_dict["output"]
        return input_dict

    @staticmethod
    def get_input_or_output_metrics_data(processing_data_list, input_output_tag, data_format):
        """
        [
            {
                "data_set_id": "591_abc_123",
                "storage_cluster_type": "None",
                "node_id": "26016",
                "value": {
                    "input_count": 958327,
                    "time": 1617699342
                },
                "flow_id": "8514"
            },
            {
                "data_set_id": "591_abc_123",
                "storage_cluster_type": "['None', None]",
                "node_id": "26016",
                "value": {
                    "input_count": 0,
                    "time": 1617699342
                },
                "flow_id": "8514"
            },
            {
                "data_set_id": "591_abc_123",
                "storage_cluster_type": "kafka",
                "node_id": "26016",
                "value": {
                    "input_count": 0,
                    "time": 1617699342
                },
                "flow_id": "8514"
            }
        ]
        无论是 input/output，查询出来有多个值。取 input_count/output_count 最大的那个 dict
        """
        # 有多条，input_count/output_count 按条件取值，只有实时类有多条，内存或者kafka
        # 1. input_count 取内存中的值 storage_cluster_type: None
        # 2. output_count 取组件中的值 storage_cluster_type: kafka
        # processing_data_list 可能为空
        if data_format == "value":
            input_or_output_value = 0
            if not processing_data_list:
                return 0
            processing_data_len = len(processing_data_list)
            if processing_data_len > 1:
                # 有内存和 kafka
                for item in processing_data_list:
                    if input_output_tag == "input_count" and item["storage_cluster_type"] == "None":
                        return item["value"][input_output_tag]
                    if input_output_tag == "output_count" and item["storage_cluster_type"] == "kafka":
                        return item["value"][input_output_tag]
            elif processing_data_len == 1:
                # 只有一个值
                return processing_data_list[0]["value"][input_output_tag]
            return input_or_output_value
        else:
            # series
            result_dict = {}
            processing_data_len = len(processing_data_list)
            if processing_data_len > 1:
                for item in processing_data_list:
                    if input_output_tag == "input_count" and item["storage_cluster_type"] == "None":
                        return item
                    if input_output_tag == "output_count" and item["storage_cluster_type"] == "kafka":
                        return item
            elif processing_data_len == 1:
                # 只有一个值
                return processing_data_list[0]
            return result_dict

    @staticmethod
    def build_input_output_sum_dict(input_output_tag, result_table_id, input_output_tag_sum, node_type, unit):
        if input_output_tag == "input_count":
            return_dict = {
                "data": {
                    result_table_id: {
                        "output": 0,
                        "unit": unit,
                        "input": input_output_tag_sum,
                    }
                },
                "node_type": node_type,
                "output": 0,
                "unit": unit,
                "input": input_output_tag_sum,
            }
        else:
            return_dict = {
                "data": {
                    result_table_id: {
                        "output": input_output_tag_sum,
                        "unit": unit,
                        "input": 0,
                    }
                },
                "node_type": node_type,
                "output": input_output_tag_sum,
                "unit": unit,
                "input": 0,
            }
        return return_dict

    @staticmethod
    def build_input_output_series_dict(input_output_tag, result_table_id, result_dict):
        # 格式转化成前端的格式，返回
        data_dict = {}
        input_list = []
        time_list = []
        if result_dict and result_dict["series"]:
            for item in result_dict["series"]:
                input_list.append(item[input_output_tag])
                time_list.append(item["time"])
            if input_output_tag == "input_count":
                data_dict[result_table_id] = {"input": input_list, "time": time_list}
            else:
                data_dict[result_table_id] = {"output": input_list, "time": time_list}
        return data_dict

    @staticmethod
    def get_batch_monitor_unit(scheduler_period):
        """
        获取监控显示的数据吞吐量单位
        """
        unit = _("条/小时")
        if scheduler_period == PERIOD_HOUR:
            unit = _("条/小时")
        elif scheduler_period == PERIOD_DAY:
            unit = _("条/天")
        elif scheduler_period == PERIOD_WEEK:
            unit = _("条/周")
        elif scheduler_period == PERIOD_MONTH:
            unit = _("条/月")
        return unit

    @staticmethod
    def get_stream_input_output_metrics(node_handler, input_output_tag, data_format="series"):
        """
        实时类的指标，计算单位：条/分钟
        计算策略：计算最近 10 分钟的节点，除以 10 得到条/分钟
        包括：
        实时数据源
        实时计算，数据模型，实时指标
        """
        node_type = node_handler.node_type
        result_table_id = node_handler.result_table_ids[0]
        if node_type == NodeTypes.STREAM_SOURCE:
            # 区别：数据源传参 data_set_ids，计算节点传参 node_ids
            stream_source_data_list = DatamanageHelper.get_result_table_metric_v2(
                {
                    "measurement": input_output_tag,
                    "data_set_ids": result_table_id,
                    "start_time": START_TIME_ONE_HOUR,
                    "fill": "none",
                    "format": data_format,
                }
            )
        else:
            stream_source_data_list = DatamanageHelper.get_result_table_metric_v2(
                {
                    "measurement": input_output_tag,
                    "node_ids": [node_handler.node_id],
                    "start_time": START_TIME_ONE_HOUR,
                    "fill": "none",
                    "format": data_format,
                }
            )
        if data_format == "value":
            input_output_sum = (
                MonitorUtil.get_input_or_output_metrics_data(stream_source_data_list, input_output_tag, data_format)
                / 60
            )
            return MonitorUtil.build_input_output_sum_dict(
                input_output_tag,
                result_table_id,
                input_output_sum,
                node_type,
                STREAM_UNIT,
            )
        else:
            # series
            input_output_result_dict = MonitorUtil.get_input_or_output_metrics_data(
                stream_source_data_list, input_output_tag, data_format
            )
            return MonitorUtil.build_input_output_series_dict(
                input_output_tag, result_table_id, input_output_result_dict
            )

    @staticmethod
    def get_batch_metrics_start_time(schedule_period):
        if schedule_period == PERIOD_HOUR:
            # 24 小时取平均
            start_time = START_TIME_ONE_DAY
            ave_num = 24
        elif schedule_period == PERIOD_DAY:
            # 7 天取平均
            start_time = START_TIME_ONE_WEEK
            ave_num = 7
        elif schedule_period == PERIOD_WEEK:
            # 4 周取平均
            start_time = START_TIME_ONE_MONTH
            ave_num = 4
        else:
            # PERIOD_MONTH
            # 3 月取平均
            start_time = START_TIME_THREE_MONTH
            ave_num = 3
        return start_time, ave_num

    @staticmethod
    def get_batch_input_output_metrics(node_handler, input_output_tag, data_format="series"):
        """
        离线类的指标，计算单位：跟窗口保持一致
        计算策略：
        包括：
        离线数据源，离线计算等
        """
        node_type = node_handler.node_type
        result_table_id = node_handler.result_table_ids[0]
        if node_type == NodeTypes.BATCH_SOURCE:
            from_batch_node = node_handler.get_rt_generated_node()
            if from_batch_node:
                schedule_period = from_batch_node.get_schedule_period_from_batch_node()
            else:
                # 清洗表，上游无离线计算
                schedule_period = PERIOD_HOUR
            start_time, ave_num = MonitorUtil.get_batch_metrics_start_time(schedule_period)
            batch_source_data_list = DatamanageHelper.get_result_table_metric_v2(
                {
                    "measurement": input_output_tag,
                    "data_set_ids": result_table_id,
                    "start_time": start_time,
                    "fill": "none",
                    "format": data_format,
                }
            )
        else:
            if node_type in NodeTypes.STORAGE_CATEGORY:
                schedule_period = NodeUtils.get_storage_upstream_node_handler(
                    node_handler.node_id
                ).get_schedule_period_from_batch_node()
            else:
                schedule_period = node_handler.get_schedule_period_from_batch_node()
            start_time, ave_num = MonitorUtil.get_batch_metrics_start_time(schedule_period)
            batch_source_data_list = DatamanageHelper.get_result_table_metric_v2(
                {
                    "measurement": input_output_tag,
                    "node_ids": [node_handler.node_id],
                    "start_time": start_time,
                    "fill": "none",
                    "format": data_format,
                }
            )
        if data_format == "value":
            input_output_sum = (
                MonitorUtil.get_input_or_output_metrics_data(batch_source_data_list, input_output_tag, data_format)
                / ave_num
            )
            return MonitorUtil.build_input_output_sum_dict(
                input_output_tag,
                result_table_id,
                input_output_sum,
                node_type,
                MonitorUtil.get_batch_monitor_unit(schedule_period),
            )
        else:
            # series
            input_output_result_dict = MonitorUtil.get_input_or_output_metrics_data(
                batch_source_data_list, input_output_tag, data_format
            )
            return MonitorUtil.build_input_output_series_dict(
                input_output_tag, result_table_id, input_output_result_dict
            )


class FlowMonitorHandlerSet(MonitorHandler):
    """
    Flow监控打点集合
    """

    DEFAULT_MONITOR_TYPES = [DMONITOR_ALERTS, DATA_LOSS_IO_TOTAL, RESULT_TABLE_STATUS]

    def __init__(self, flow):
        self.flow = flow
        super(FlowMonitorHandlerSet, self).__init__()

    def list_monitor_data(self):
        monitor_types = self.DEFAULT_MONITOR_TYPES
        supported_monitor = {
            DMONITOR_ALERTS: self.get_flow_monitor_alerts,
            DATA_LOSS_IO_TOTAL: self.get_flow_data_loss_io_total,
            RESULT_TABLE_STATUS: self.get_flow_rt_status,
        }
        func_info = []
        monitor_data = {}
        for monitor_type in monitor_types:
            func_info.append([supported_monitor[monitor_type], {}])
        threads_res = concurrent_call_func(func_info)
        for idx, monitor_type in enumerate(monitor_types):
            monitor_data.update({monitor_type: threads_res[idx]})

        return monitor_data

    def get_flow_monitor_alerts(self):
        """
        告警信息监控打点
        @return:
        [
            {
                'message': 'xxx',
                'message_en': 'xxx',
                'full_message': 'xxx',
                'full_message_en': 'xxx',
                'msg': 'xxx',
                'node_id': 'xxx',
                'time': 1544174262
            }
        ]
        """
        current_language = translation.get_language()
        lang = "" if current_language in ["zh-hans", "zh-cn"] else "_%s" % current_language
        # 界面实际要读取的监控信息，只取这个监控信息
        msg_key = "full_message%s" % lang
        flow_id = self.flow.flow_id
        now = datetime.datetime.now()
        end_time = now.strftime("%Y-%m-%d %H:%M:%S")
        start_time = (now + datetime.timedelta(days=-1 * INTERVAL)).strftime("%Y-%m-%d %H:%M:%S")
        data = DatamanageHelper.get_alert_details(flow_id, start_time, end_time)
        for _d in data:
            if "alert_time" in _d:
                time_array = time.strptime(_d["alert_time"], "%Y-%m-%d %H:%M:%S")
                _d["time"] = int(time.mktime(time_array))
                _d["msg"] = _d[msg_key] if msg_key in _d else _("获取指定监控信息错误.")
            else:
                logger.error("日志格式错误: %s" % _d)
        return data

    def get_flow_data_loss_io_total(self):
        """
        1. 数据源节点，通过参数 data_set_ids 获取。静态数据源不用获取指标
        2. 实时计算节点，展示单位：条/分钟
        3. 离线计算节点，根据窗口配置 schedule_period 进行展示。函数：monitor_unit
           埋点只支持 7 天，周任务和月任务可能看不到指标数据
        返回 json 格式：
        result_node_map = {
            "224955": {
                "data": {
                    "615_mdnf_online_datadeal_tyf": {
                        "output": 10,
                        "unit": "条/分钟",
                        "input": 10
                    }
                },
                "node_type": "realtime",
                "output": 10,
                "unit": "条/分钟",
                "input": 10
            }
        }
        目前 data 内外值都是一样的，因为只有一个 rt 输出
        """
        result_node_map = {}
        flow_node_handler_list = self.flow.nodes
        for node_handler in flow_node_handler_list:
            if node_handler.node_type in list(MonitorHandler.DEFAULT_NODE_MONITOR_MAP.keys()):
                result_table_id = node_handler.result_table_ids[0]
                input_dict = self.get_input_output_loss_node_handler(node_handler, "input_count", "value")
                output_dict = self.get_input_output_loss_node_handler(node_handler, "output_count", "value")
                merged_result = MonitorUtil.merge_input_and_output_data(input_dict, output_dict, result_table_id)
                if merged_result:
                    result_node_map[node_handler.node_id] = merged_result
        return result_node_map

    def get_flow_rt_status(self):
        data = NodeMonitorHandlerSet.list_node_result_table_status(self.flow.nodes)
        return data


class NodeMonitorHandlerSet(MonitorHandler):
    def __init__(self, node_handler, monitor_type):
        self.node_handler = node_handler
        self.monitor_type = monitor_type
        super(NodeMonitorHandlerSet, self).__init__()

    def list_monitor_data(self):
        monitor_data = {}
        # 该节点未配置监控
        if self.node_handler.node_type not in list(self.DEFAULT_NODE_MONITOR_MAP.keys()):
            return monitor_data
        monitor_types = self.DEFAULT_NODE_MONITOR_MAP[self.node_handler.node_type]
        if self.monitor_type:
            if self.monitor_type not in monitor_types:
                raise NodeValidError("当前节点仅支持监控类型%s" % monitor_types)
            monitor_types = [self.monitor_type]
        # 各种监控类型对应的监控查询方式
        supported_monitor = {
            DMONITOR_ALERTS: self.get_node_monitor_alerts,
            DATA_LOSS_IO_TOTAL: self.get_node_data_loss_io_total,
            RESULT_TABLE_STATUS: self.get_node_result_table_status,
            DATA_DELAY_MAX: self.get_node_data_delay_max,
            DATA_TREND: self.get_node_data_trend,
        }
        for monitor_type in monitor_types:
            monitor_data.update({monitor_type: supported_monitor[monitor_type]()})
        return monitor_data

    def get_node_result_table_status(self):
        """
        获取离线节点各个rt的调度情况，仅离线节点需要统计该信息
        @return: node_monitors
        {
            'rt_id1': {
                'interval': xx,
                'start_time': xx,
                'node_type': xx
            }
        }
        """
        # 非离线 RT 不可获取调度信息
        data = {}
        processing_id = None
        processing_type = None
        if self.node_handler.node_type in NodeTypes.BATCH_SOURCE_CATEGORY:
            processing_id = self.node_handler.result_table_ids[0]
            processing_type = ResultTableHelper.get_processing_type(processing_id)
        elif self.node_handler.node_type in NodeTypes.BATCH_CATEGORY:
            processing_id = self.node_handler.processing_id
            processing_type = DataProcessingHelper.get_data_processing_type(processing_id)
        if processing_id and processing_type and processing_type in ["batch"]:
            data = DatamanageHelper.get_batch_executions([processing_id])
        return data

    @staticmethod
    def list_node_result_table_status(nodes):
        """
        获取离线rt的调度情况，仅离线节点需要统计该信息
        @param nodes:
        @return: node_monitors
        {
            'node_id1': {
                'rt_id1': {
                    'interval': xx,
                    'start_time': xx,
                    'node_type': xx
                }
            }
        }
        """
        batch_node_info_dict = {}
        for _n in nodes:
            # 暂不考虑离线数据源
            if _n.node_type in [
                NodeTypes.BATCH,
                NodeTypes.BATCHV2,
                NodeTypes.MODEL_APP,
                NodeTypes.DATA_MODEL_BATCH_INDICATOR,
            ]:
                for result_table_id in _n.result_table_ids:
                    batch_node_info_dict[result_table_id] = {
                        "node_id": _n.node_id,
                        "node_type": _n.node_type,
                    }
        # 批量获取flow中所有离线rt对应的错误状态信息
        data = DatamanageHelper.get_batch_executions(list(batch_node_info_dict.keys()))
        node_monitors = {}
        for _k, _v in list(data.items()):
            if _v:
                if _k not in batch_node_info_dict:
                    logger.warning(_("离线表执行状态返回信息错误，包含不存在的rt(%s)，节点可能已被删除") % _k)
                    continue
                node_monitors_detail = node_monitors.setdefault(batch_node_info_dict[_k]["node_id"], {})
                node_monitors_detail[_k] = {
                    "interval": _v.get("interval"),
                    "start_time": (_v["execute_history"][0]["start_time"] if _v.get("execute_history") else "-"),
                }
                # TODO：目前一个节点只有一个RT，为兼容旧接口，将RT相关信息放到节点层之下
                node_monitors_detail.update(node_monitors_detail[_k])
        return node_monitors

    def get_node_data_delay_max(self):
        node_type = self.node_handler.node_type
        result_table_id = self.node_handler.result_table_ids[0]
        if node_type == NodeTypes.STREAM_SOURCE or node_type == NodeTypes.BATCH_SOURCE:
            # 若为数据源节点，通过 result_table_id 获取打点信息
            source_delay_list = DatamanageHelper.get_result_table_metric_v2(
                {
                    "measurement": "process_time_delay",
                    "start_time": START_TIME_ONE_DAY,
                    "fill": "none",
                    "data_set_ids": [result_table_id],
                }
            )
            # 有多条，取 storage_cluster_type = kafka 或者 hdfs 的一条
            result_dict = None
            for item in source_delay_list:
                if item["storage_cluster_type"] == NodeTypes.KAFKA or item["storage_cluster_type"] == NodeTypes.HDFS:
                    result_dict = item
        else:
            processing_delay_list = DatamanageHelper.get_result_table_metric_v2(
                {
                    "measurement": "process_time_delay",
                    "start_time": START_TIME_ONE_DAY,
                    "fill": "none",
                    "node_ids": [self.node_handler.node_id],
                }
            )
            # 有多条，取 process_time_delay 最大的一个
            max_process_time_delay = -1
            result_dict = None
            for item in processing_delay_list:
                if item["series"][0]["process_time_delay"] > max_process_time_delay:
                    result_dict = item
                    max_process_time_delay = item["series"][0]["process_time_delay"]
        # 格式转化成前端的格式，返回
        data_delay_max_dict = {}
        delay_max_list = []
        time_list = []
        if result_dict and result_dict["series"]:
            for item in result_dict["series"]:
                delay_max_list.append(item["process_time_delay"])
                time_list.append(item["time"])
            data_delay_max_dict[result_table_id] = {
                "delay_max": delay_max_list,
                "time": time_list,
            }
        return data_delay_max_dict

    def get_node_monitor_alerts(self):
        # node_type = self.node_handler.node_type
        node_id = self.node_handler.node_id
        result_table_id = self.node_handler.result_table_ids[0]
        time_now = int(time.time())
        # 最近一天的告警
        start_time = time_now - ONE_DAY_SECOND
        # TODO 请求数据源告警列表返回都是 []
        alert_detail_list = DatamanageHelper.get_alert_detail_v2(
            {"start_time": start_time, "fill": "none", "node_id": node_id}
        )
        data_dict = {}
        alert_list = []
        for alert in alert_detail_list:
            item = {
                "time": alert["id"],
                "last_full_message_en": alert["full_message_en"],
                "last_message_en": alert["message_en"],
                "last_alert_status": alert["alert_status"],
                "last_recover_time": alert["alert_recover_time"],
                "last_message": alert["message"],
                "msg": alert["message"],
                "last_full_message": alert["full_message"],
            }
            alert_list.append(item)
        data_dict[result_table_id] = {"alert_list": alert_list}
        return data_dict

    def get_node_data_loss_io_total(self):
        data_loss_io_total = {}
        result_table_id = self.node_handler.result_table_ids[0]
        data_loss_io_total[result_table_id] = {"input": [], "output": [], "time": []}
        # input_dict，output_dict 都有可能是空
        input_dict = self.get_input_output_loss_node_handler(self.node_handler, "input_count", "series")
        output_dict = self.get_input_output_loss_node_handler(self.node_handler, "output_count", "series")
        if input_dict and input_dict[result_table_id]:
            data_loss_io_total[result_table_id]["input"] = input_dict[result_table_id]["input"]
            data_loss_io_total[result_table_id]["time"] = input_dict[result_table_id]["time"]
        # 把 output 移到 input_dict，有些节点没有输出
        if output_dict and output_dict[result_table_id]:
            data_loss_io_total[result_table_id]["output"] = output_dict[result_table_id]["output"]
        return data_loss_io_total

    def get_node_data_trend(self):
        """
        get_node_data_trend = get_node_data_loss_io_total + get_node_monitor_alerts
        """
        data_loss_io_total = self.get_node_data_loss_io_total()
        # 补充告警打点
        monitor_alert = self.get_node_monitor_alerts()
        result_table_id = self.node_handler.result_table_ids[0]
        data_loss_io_total[result_table_id]["alert_list"] = monitor_alert[result_table_id]["alert_list"]
        return data_loss_io_total
