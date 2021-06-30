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
from api.base import DataAPI, DataDRFAPISet, DRFActionAPI
from api.utils import add_dataapi_inner_header, add_esb_common_params
from conf.settings import DATAMANAGE_API_URL


class DatamanageApi(object):

    MODULE = "DATAMANAGE"

    def __init__(self):
        self.rules = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dataquality/rules/",
            primary_key="rule_id",
            module="datamanage",
            description="数据管理规则API",
        )

        self.tasks = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dataquality/audit_tasks/",
            primary_key="task_id",
            module="datamanage",
            description="数据管理规则任务API",
        )

        self.get_data_dict_count = DataAPI(
            url=DATAMANAGE_API_URL + "datamap/retrieve/get_data_dict_count/",
            method="POST",
            module="datamanage",
            description="获取数据字典列表统计数据",
        )

        self.get_data_dict_list = DataAPI(
            url=DATAMANAGE_API_URL + "datamap/retrieve/get_data_dict_list/",
            method="POST",
            module="datamanage",
            description="获取数据字典列表数据列表",
        )

        self.influx_query = DataAPI(
            url=DATAMANAGE_API_URL + "dmonitor/metrics/query/",
            method="POST",
            module="datamanage",
            description="查询数据质量TSDB中的监控指标",
        )

        self.influx_report = DataAPI(
            url=DATAMANAGE_API_URL + "dmonitor/metrics/report/",
            method="POST",
            module="datamanage",
            description="上报数据监控自定义指标",
        )

        self.range_metric_by_influxdb = DataAPI(
            url=DATAMANAGE_API_URL + "lifecycle/range/range_metric_by_influxdb/",
            method="GET",
            module="datamanage",
            description="range_metric_by_influxdb",
        )

        self.alerts = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dmonitor/alerts/",
            primary_key="alert_id",
            module="datamanage",
            description="告警相关接口",
            custom_config={
                "send": DRFActionAPI(method="post", detail=False),
            },
        )

        self.event_types = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dataquality/event_types/",
            primary_key="id",
            module="datamanage",
            description="事件类型",
        )

        self.notify_configs = DataAPI(
            url=DATAMANAGE_API_URL + "dataquality/events/notify_configs/",
            method="GET",
            module="datamanage",
            description="获取事件通知配置",
        )

        self.dmonitor_data_sets = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dmonitor/data_sets/",
            primary_key="data_set_id",
            module="datamanage",
            description="获取监控所有dataset的信息",
            default_timeout=180,
        )

        self.dmonitor_data_operations = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dmonitor/data_operations/",
            primary_key="data_operation_id",
            module="datamanage",
            description="获取监控所有data_processing和data_transferring",
            default_timeout=180,
        )

        self.sampling_result_tables = DataAPI(
            url=DATAMANAGE_API_URL + "dataquality/sampling/result_tables/",
            method="GET",
            module="datamanage",
            description="获取待采样的结果表列表",
            default_timeout=180,
        )

        self.dmonitor_flows = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dmonitor/flows/",
            primary_key="flow_id",
            module="datamanage",
            description="获取数据监控所需的数据流信息",
            default_timeout=180,
            custom_config={"dataflow": DRFActionAPI(method="get", detail=False)},
        )

        self.alert_configs = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dmonitor/alert_configs/",
            primary_key="alert_config_id",
            module="datamanage",
            description="获取监控配置",
            custom_config={
                "dataflow": DRFActionAPI(
                    method="get",
                    url_path="dataflow/{flow_id}",
                    detail=False,
                    url_keys=["flow_id"],
                ),
                "rawdata": DRFActionAPI(
                    method="get",
                    url_path="rawdata/{flow_id}",
                    detail=False,
                    url_keys=["flow_id"],
                ),
            },
        )

        self.alert_shields = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dmonitor/alert_shields/",
            primary_key="alert_shield_id",
            module="datamanage",
            description="告警屏蔽规则",
            custom_config={
                "in_effect": DRFActionAPI(method="get", detail=False),
            },
        )

        self.datamodels = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "datamodel/models/",
            primary_key="model_id",
            module="datamanage",
            description="数据模型接口集合",
            custom_config={
                "import_model": DRFActionAPI(
                    method="post", detail=False, url_path="import"
                ),
                "release": DRFActionAPI(method="post", detail=True),
            },
            custom_headers=add_dataapi_inner_header(),
            before_request=add_esb_common_params,
        )

        self.generate_datamodel_instance = DataAPI(
            url=DATAMANAGE_API_URL + "datamodel/instances/dataflow/",
            method="POST",
            module="datamanage",
            description="模型应用实例生成完整dataflow任务",
            custom_headers=add_dataapi_inner_header(),
            before_request=add_esb_common_params,
        )

        self.dmonitor_batch_executions = DataDRFAPISet(
            url=DATAMANAGE_API_URL + "dmonitor/batch_executions/",
            primary_key="exec_id",
            module="datamanage",
            description="按时间获取离线任务执行记录",
            default_timeout=180,
            custom_headers={
                "blueking-language": "en",
            },
            custom_config={
                "by_time": DRFActionAPI(method="get", detail=False),
                "latest": DRFActionAPI(method="get", detail=False),
            },
        )

        self.dmonitor_batch_schedules = DataAPI(
            method="GET",
            url=DATAMANAGE_API_URL + "dmonitor/batch_schedules/",
            module="datamanage",
            description=u"获取离线处理的调度信息",
            default_timeout=180,
        )
