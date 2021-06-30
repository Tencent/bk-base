# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""


import json
import re
from copy import deepcopy

import arrow
import six
from django.utils.decorators import available_attrs
from six.moves import filter

from apps.dataflow.models import ResultTableQueryRecord, ResultTableSelectedRecord
from apps.exceptions import DataError

try:
    from functools import wraps
except ImportError:
    from django.utils.functional import wraps

from django.utils.translation import ugettext_lazy as _

from apps import exceptions
from apps.api import (
    AuthApi,
    DatabusApi,
    DataFlowApi,
    DataManageApi,
    DataQueryApi,
    MetaApi,
    StorekitApi,
)
from apps.common.log import logger
from apps.dataflow.datamart.datadict import get_format_tag_list, processing_type_dict
from apps.dataflow.handlers.business import Business
from apps.dataflow.utils import subdigit
from apps.exceptions import ApiResultError
from apps.utils import APIModel
from apps.utils.time_handler import (
    DTEVENTTIMESTAMP_MULTIPLICATOR,
    generate_influxdb_time_range,
    get_active_timezone_offset,
    get_pizza_timestamp,
    list_date_by_a_period_of_time,
    list_year_month_by_a_period_of_time,
    time_format,
    timeformat_to_timestamp,
    timestamp_to_timeformat,
)


class ResultTable(APIModel):
    KEYS = ["result_table_id"]

    # 默认添加的时间字段
    DEFAULT_TIME_FIELDS_DETAILS = [
        {
            "field_name": "dtEventTime",
            "field_alias": _("数据时间，格式：YYYY-mm-dd HH:MM:SS"),
            "field_type": "string",
            "description": _("数据时间，格式：YYYY-mm-dd HH:MM:SS"),
        },
        {
            "field_name": "dtEventTimeStamp",
            "field_alias": _("数据时间戳，毫秒级别"),
            "field_type": "long",
            "description": _("数据时间戳，毫秒级别"),
            "is_dimension": 0,
        },
        {
            "field_name": "localTime",
            "field_alias": _("本地时间，格式：YYYY-mm-dd HH:MM:SS"),
            "field_type": "string",
            "description": _("本地时间，格式：YYYY-mm-dd HH:MM:SS"),
            "is_dimension": 0,
        },
        {
            "field_name": "thedate",
            "field_alias": _("数据时间，格式：YYYYmmdd"),
            "field_type": "int",
            "description": _("数据时间，格式：YYYYmmdd"),
            "is_dimension": 0,
        },
    ]

    # TSDB时间字段
    TSDB_TIME_FIELDS_DETAILS = [
        {
            "field_name": "time",
            "field_alias": _("数据时间戳，毫秒级别"),
            "type": "long",
            "description": _("数据时间戳，毫秒级别"),
            "is_dimension": 0,
        }
    ]

    class Platform(object):
        BKDATA_PLATFORM_NAME = "bkdata"
        TDW_PLATFORM_NAME = "tdw"

    class SourceType(object):
        RT_SOURCE = "rtsource"
        ETL_SOURCE = "etl_source"
        STREAM_SOURCE = "stream_source"
        BATCH_SOURCE = "batch_source"
        KV_SOURCE = "kv_source"
        BATCH_KV_SOURCE = "batch_kv_source"
        TDW_SOURCE = "tdw_source"
        UNIFIED_KV_SOURCE = "unified_kv_source"

    def __init__(self, result_table_id):
        super(ResultTable, self).__init__()
        self.result_table_id = result_table_id
        self._storage_query = []
        self._storage_display = {}

        self._recommend_sql = None
        self._recommend_storage_fields = None
        self._recommend_storage_type = None
        self._storage_type_sql = None

    @classmethod
    def list_as_source_by_project_id(cls, project_id, source_type, bk_biz_id=None):
        """
        根据数据源类型列出项目有权限的结果表列表
        :param project_id: 项目id
        :param source_type: 数据源类型
        :param bk_biz_id: 业务id
        """
        # 获取项目有权限的业务id
        params = {"project_id": project_id}
        if bk_biz_id is not None:
            params["bk_biz_id"] = bk_biz_id
        project_data = AuthApi.projects.data(params)

        biz_ids = list({_data["bk_biz_id"] for _data in project_data})

        auth_biz_ids = [_data["bk_biz_id"] for _data in project_data if not _data.get("result_table_id")]

        auth_result_table_ids = [_data["result_table_id"] for _data in project_data if _data.get("result_table_id")]

        rts = []

        # 先通过业务列表拉取到全部RT列表，本地在进行过滤
        rts_biz = ResultTable.list(bk_biz_id=biz_ids, has_fields=False, source_type=source_type)

        rts_biz = list(
            filter(
                (lambda _rt: _rt["bk_biz_id"] in auth_biz_ids or _rt["result_table_id"] in auth_result_table_ids),
                rts_biz,
            )
        )

        # 目标结果表列表
        rts.extend(rts_biz)

        # 将项目底下存在结果表也加入选择源
        rts_projects = ResultTable.list(project_id, bk_biz_id=bk_biz_id, has_fields=False, source_type=source_type)

        if rts_projects:
            rts.extend(rts_projects)

        cleaned_rts = []
        cleaned_rt_ids = []

        for _rt in rts:
            _o_rt = cls.init_by_data(_rt)
            if _o_rt.result_table_id not in cleaned_rt_ids:
                # 去重
                cleaned_rt_ids.append(_o_rt.result_table_id)
                cleaned_rts.append(_rt)
        return cleaned_rts

    @classmethod
    def list(
        cls, project_id=None, bk_biz_id=None, is_query=False, has_fields=True, source_type="rtsource", action_id=None
    ):
        """
        结果表列表，支持项、业务过滤

        @param {Boolean} is_query 是否仅返回支持查询的结果表列表
        @param {Boolean} has_fields 是否返回字段列表
        @param {Boolean} source_type
        rtsource(结果数据)
        etl_source(清洗)
        stream_source(实时)
        batch_source(离线)
        kv_source(关联)
        """
        # 默认查询包含存储关系和结果表类型的结果表列表
        api_param = {
            "related": ["storages", "result_table_type", "project_name"],
            "need_storage_detail": 0,
        }
        if project_id is not None:
            api_param["project_id"] = project_id
        if bk_biz_id is not None:
            api_param["bk_biz_id"] = bk_biz_id
        if has_fields:
            api_param["related"].append("fields")
        if source_type == "tdw_source":
            api_param["extra"] = True

        if action_id is None:
            rts = MetaApi.result_tables.list(api_param)
        else:
            api_param["action_id"] = action_id
            rts = MetaApi.result_tables.mine(api_param)

        cleaned_rts = []

        # 此处避免 filter_supported_query_storages 重复去调用 get_supported_storage_query
        support_query_storages = cls.get_supported_storage_query()
        if is_query:
            # 有存储的可查询的结果表
            cleaned_rts = []
            for _rt in rts:
                _o_rt = cls.init_by_data(_rt)

                _storages_arr = cls.filter_supported_query_storages(
                    _o_rt.storages_arr, support_storages=support_query_storages
                )

                # _storages_arr = _o_rt.query_storages_arr
                if _storages_arr:
                    _rt["query_storages_arr"] = _storages_arr
                    cleaned_rts.append(_rt)
        else:
            # 根据数据源类别获取rt
            for _rt in rts:
                # TDW表，
                _o_rt = cls.init_by_data(_rt)

                if _o_rt.judge_source_type(source_type):

                    # 作为TDW数据源时，特殊构造中文名
                    if source_type == "tdw_source":
                        _o_rt._data["result_table_name_alias"] = "{}[{}]".format(
                            _o_rt._data["result_table_name_alias"], _o_rt.tdw_table_name
                        )
                    cleaned_rts.append(_rt)

        return cleaned_rts

    @classmethod
    def list_rt_storage_info(cls, l_result_table_ids):
        """
        获取rt对应的存储信息
        :param l_result_table_ids: 结果表id列表
        :return:
        """
        if not l_result_table_ids:
            return []
        api_param = {"result_table_id": l_result_table_ids}
        storage_nodes = DataFlowApi.flow_nodes.storage_nodes(api_param)
        return storage_nodes

    @classmethod
    def list_latest_data(cls, rt_ids):
        """
        查询结果表最新的数据，支持批量
        :param rt_ids:结果表列表
        :return: {
            "rt1": {"time": "2018-01-01 11:11", },
        }
        """
        rt_condition = "logical_tag = '" + "' or logical_tag = '".join(rt_ids) + "'"
        sql = (
            'SELECT *,logical_tag FROM "data_loss_output_total" '
            "where data_inc > 0 and ({}) group by logical_tag, component "
            "order by time desc limit 1".format(rt_condition)
        )
        api_param = {"database": "monitor_data_metrics", "sql": sql}
        try:
            latest_data = DataManageApi.dmonitor_metrics.query(api_param)
        except (exceptions.ApiResultError, exceptions.ApiRequestError):
            latest_data = {"data_format": "error", "series": {"values": [[]]}}

        if latest_data["data_format"] == "series":
            data = [_d.get("values")[0] for _d in latest_data.get("series", [])]
        elif latest_data["data_format"] == "simple" and len(latest_data["series"]) > 0:
            data = [latest_data["series"][0]]
        else:
            data = []

        d_data = {}
        for _d in data:
            _d["time"] = timestamp_to_timeformat(_d.get("time"))
            d_data.update({_d.get("logical_tag"): _d})

        return d_data

    @classmethod
    def dmonitor_metrics(self, sql):
        """查询数据的监控指标"""
        api_param = {
            "database": "monitor_data_metrics",
            "sql": sql,
        }
        try:
            data_counts = DataManageApi.dmonitor_metrics.query(api_param)
        except (exceptions.ApiResultError, exceptions.ApiRequestError) as e:
            data_counts = {"series": []}
            logger.error("dmonitor_metrics api error, error={}".format(e.message))
        return data_counts

    def list_rt_count(self, start_timestamp, end_timestamp, frequency=None):
        """
        查询结果表的数据趋势
        """
        # 生成influxdb需要的时间段
        start_timestamp, end_timestamp = generate_influxdb_time_range(start_timestamp, end_timestamp)

        group_by = "GROUP BY time({})".format(frequency) if frequency else ""

        if self.recommend_storage_type:
            # 查meta的result_tables接口拿到storage_cluster_config_id
            storage_cluster_config_id = (
                self.storages.get(self.recommend_storage_type, "")
                .get("storage_cluster", {})
                .get("storage_cluster_config_id", "")
            )
            rt_condition = "logical_tag = '%s'" % self.result_table_id
            # sql = "select sum(data_inc) as cnt from data_loss_output_total where {} and storage = 'storage_{}' and " \
            #       "module = 'shipper' and time >= {} and  time <= {} {} fill(null)" \
            #     .format(rt_condition, storage_cluster_config_id, start_timestamp, end_timestamp, group_by)
            sql = (
                "select sum(data_inc) as cnt from data_loss_output_total where {} and storage = 'storage_{}' "
                "and time >= {} and  time <= {} {} fill(null)".format(
                    rt_condition, storage_cluster_config_id, start_timestamp, end_timestamp, group_by
                )
            )
            data_counts = self.dmonitor_metrics(sql)
            cnts = [data["cnt"] for data in data_counts.get("series", [])]
            time = [data["time"] for data in data_counts.get("series", [])]

            formated_time = time_format(time)
            return_data = {"cnt": cnts, "time": formated_time, "timezone": get_active_timezone_offset()}
        else:
            return_data = {"cnt": [], "time": []}
        return return_data

    @classmethod
    def query_rt(cls, sql, storage_type=None):
        api_param = {"sql": sql}
        if storage_type:
            api_param["prefer_storage"] = storage_type
        response = DataQueryApi.query(api_param)
        if storage_type == "es":
            data = response["list"]
            data_list = []
            # 正则只保留高亮部分标签，避免第三方数据造成的xss攻击
            regex = r"<(?P<tag>(?!(em|/em))(.*?))>"
            replace_tag = r"&lt;\g<tag>&gt;"
            for _d in data["hits"]["hits"]:
                _source = _d["_source"]
                for key in _d.get("highlight", []):
                    highlight = "".join(_d["highlight"][key])
                    highlight = re.sub(regex, replace_tag, highlight)
                    _source.update({key: highlight})
                data_list.append(_source)
            time_taken = round(float(data["took"]) / float(1000), 3)
        else:
            # 补充字段名称
            data_list = response.get("list", [])
            time_taken = round(response["timetaken"], 3)

        data_field = response.get("select_fields_order", [])
        if len(data_list):
            if len(data_field) == 0 or data_field[0] == "*":
                data_field = list(data_list[0].keys())
        response = dict(
            select_fields_order=data_field, list=data_list, total=response.get("totalRecords", 0), time_taken=time_taken
        )
        return response

    @classmethod
    def list_selected_history(cls, username):
        """
        用户最近选择的结果表
        :param username:
        :return:
        """
        rt_ids = ResultTableSelectedRecord.list_selected_history(username)
        if not rt_ids:
            return []

        history_result_tables = {}

        api_params = {"related": ["storages"], "result_table_ids": rt_ids}
        l_rt_ids = MetaApi.result_tables.list(api_params)
        for _rt_id in l_rt_ids:
            _rt_id["query_storages_arr"] = cls.filter_supported_query_storages(list(_rt_id["storages"].keys()))
            history_result_tables[_rt_id["result_table_id"]] = _rt_id

        rt_info = []

        latest_data = cls.list_latest_data(rt_ids)
        for rt_id in rt_ids:
            # 添加最后一条数据时间
            try:
                data = history_result_tables[rt_id]
            except KeyError:
                # 结果表不存在或被删了，则跳过
                continue

            latest_data_times = []
            for storage_type in data.get("query_storages_arr", []):
                try:
                    data_time = latest_data[rt_id].get("time")
                except KeyError:
                    # 不存在结果表+存储类型对应的数据
                    continue
                else:
                    latest_data_times.append(data_time)

            if latest_data_times:
                latest_data_time = max(latest_data_times)
            else:
                latest_data_time = _("暂无上报数据")

            data.update(latest_data_time=latest_data_time)
            rt_info.append(data)

        return rt_info

    def get_latest_msg(self, source_type):
        """
        根据数据源类型
        :param source_type: 数据源类型
        :return:
        """
        result = _("暂不支持此类数据源的数据查询")
        if source_type == "stream_source":
            result = self.get_kafka_latest_msg()
        if source_type == "batch_source":
            result = self.get_hdfs_latest_msg()
        if source_type == "kv_source":
            result = _("暂不支持关联数据查询")

        return result

    def get_kafka_latest_msg(self):
        """
        从 KAFKA 获取最新一条数据
        """
        if self.has_kafka():
            try:
                latest_msgs = DatabusApi.result_tables.tail({"result_table_id": self.result_table_id})
            except (exceptions.ApiResultError, exceptions.ApiResultError):
                latest_msgs = [{"data": _("获取实时数据失败")}]
        else:
            latest_msgs = []

        return json.dumps(latest_msgs[0]) if latest_msgs else _("暂无数据")

    def get_hdfs_latest_msg(self):
        """
        从 HDFS 中获取最新一条数据
        """
        if self.has_hdfs():
            try:
                _latest_msg = DataFlowApi.batch_hdfs_result_tables.new_line({"result_table_id": self.result_table_id})
            except (exceptions.ApiResultError, exceptions.ApiResultError):
                _latest_msg = _("获取离线数据失败")
            _latest_msg = _latest_msg or _("暂无数据")
        else:
            _latest_msg = _("暂无数据")
        return _latest_msg

    def list_calculate_fields(self):
        """
        返回 RT 的字段列表，计算字段
        """
        fields = self.fields
        fields = self.clear_default_field(fields)
        return fields

    def add_time_field(self, storage_type, fields):
        """
        添加默认时间字段
        :param storage_type:
        :param fields:
        :return:
        """
        if storage_type == "tsdb":
            fields.extend(self.TSDB_TIME_FIELDS_DETAILS)
        else:
            fields.extend(self.DEFAULT_TIME_FIELDS_DETAILS)
        return fields

    def list_storage_fields(self, storage_type="all", has_judge=True):
        """
        返回存储字段列表
        """
        fields = self.fields
        fields = self.clear_default_field(fields)
        fields = self.add_time_field(storage_type, fields)
        if has_judge:
            # 添加图标

            icons = [
                {"judge": "is_dimension", "key": "dimension", "description": _("维度")},
                {"judge": "is_analyzed", "key": "analyzed", "description": _("分词")},
                {"judge": "is_value", "key": "value", "description": _("值")},
                {"judge": "is_time", "key": "time", "description": _("时间")},
                {"judge": "is_index", "key": "index", "description": _("索引")},
                {"judge": "is_main_key", "key": "main_key", "description": _("主键")},
            ]

            for _f in fields:
                _f.update({"icon": []})
                for icon in icons:
                    if _f.get(icon["judge"]):
                        _f["icon"].append(icon)
        return fields

    @property
    def storage_type_sql(self):
        """
        获取rt关联对应物理存储的schema, 默认查询语句, 存储的查询顺序
        :return:
        {
            "storages":[
                "tsdb"
            ],
            "storage":{
                "tsdb":{
                    "fields":[
                        {
                            "physical_field":"ip",
                            "field_type":"string",
                            "field_alias":"未设置",
                            "description":"",
                            "is_dimension":true,
                            "field_index":0,
                            "is_time":false,
                            "field_name":"ip",
                            "physical_field_type":"string"
                        }
                    ],
                    "config":{

                    },
                    "sql":"SELECT ip FROM 0_docker_cpu_summary WHERE time > '1h' ORDER BY time DESC LIMIT 10"
                }
            },
            "order":[
                "tsdb"
            ]
        }
        """
        if not self._storage_type_sql:
            schema_type_sql_dict = StorekitApi.schema_and_sql.list({"result_table_id": self.result_table_id}, raw=True)
            if schema_type_sql_dict.get("result") and isinstance(schema_type_sql_dict.get("data"), dict):
                self._storage_type_sql = schema_type_sql_dict.get("data")

        return self._storage_type_sql

    @property
    def recommend_storage_type(self):
        """
        rt表从存储接口拿到的推荐存储类型
        """
        # 1 查存储
        if not self._recommend_storage_type and self.storage_type_sql:
            storage_type_list = self.storage_type_sql.get("order", [])
            if storage_type_list:
                self._recommend_storage_type = storage_type_list[0]
        return self._recommend_storage_type

    @property
    def recommend_sql(self):
        """
        rt表从存储接口拿到推荐存储对应的sql
        """
        if not self._recommend_sql and self.recommend_storage_type:
            self._recommend_sql = (
                self.storage_type_sql.get("storage", {}).get(self.recommend_storage_type, {}).get("sql", "")
            )
        return self._recommend_sql

    @property
    def recommend_storage_fields(self):
        """
        从存储接口拿到推荐存储rt所有的字段列表，包含dtEventTime字段，如果没有推荐的存储，则拿kafka对应的字段
        :return:[{},{}]
        """
        # 1 查存储,有存储
        # 2 没有存储, 查所有存储对应的信息，然后找到kafka
        if not self._recommend_storage_fields:
            storage_sql_res = {}
            if self.recommend_storage_type and self.storage_type_sql:
                storage_type = self.recommend_storage_type
                storage_sql_res = self.storage_type_sql
            if not self.recommend_storage_type:
                # 如果没有推荐存储类型，去拿kafka对应的schema

                storage_type = "kafka"
                storage_sql_res = self.kafka_schema_sql()

            fields_tmp = storage_sql_res.get("storage", {}).get(storage_type, {}).get("fields", [])
            self._recommend_storage_fields = [
                {
                    "field_name": field["physical_field"],
                    "field_alias": field["field_alias"],
                    "field_type": field["field_type"],
                    "description": field["description"],
                }
                for field in fields_tmp
            ]

        return self._recommend_storage_fields

    def kafka_schema_sql(self):
        """
        获取所有存储包含kafka的schema和默认查询语句
        :return:
        {
            "storages":[],
            "storage":{
                "kafka":{
                    "fields":[],
                    "config":{},
                    "sql":""
                },
                "queue":{},
            },
            "order":[]
        }
        """
        kafka_schema_sql_dict_tmp = StorekitApi.schema_and_sql.list(
            {"result_table_id": self.result_table_id, "flag": "all"}, raw=True
        )
        kafka_schema_sql_dict = kafka_schema_sql_dict_tmp.get("data") if kafka_schema_sql_dict_tmp.get("result") else {}
        return kafka_schema_sql_dict

    def tag_list(self, page=1, page_size=10):
        """
        获取rt包含的标签列表，根据tag类型排序，业务标签 > 来源标签 > 应用标签 > 描述标签 > 管理标签（地域）
        :return:
        [{tag_alias: "中国内地", tag_code: "inland", tag_type: "manage"}]
        """
        target_filter = {"condition": "OR"}
        criterion = [{"k": "result_table_id", "v": self.result_table_id, "func": "eq"}]
        target_filter["criterion"] = criterion
        params = {
            "page": page,
            "page_size": page_size,
            "target_type": "result_table",
            "target_filter": json.dumps([target_filter]),
        }
        tag_search = MetaApi.target_tag_info(params)

        tag_list = []
        if tag_search and tag_search.get("content", []):
            tag_search = tag_search.get("content", [])[0].get("tags", {})
            # 对实体查到的标签进行格式化处理和排序
            tag_list = get_format_tag_list(tag_search)
        return tag_list

    def data_manager(self):
        """
        rt的数据管理员
        :return:
        """
        manager = AuthApi.roles_users.list({"role_id": "result_table.manager", "scope_id": self.result_table_id})
        return manager

    def data_viewer(self):
        """
        rt的数据观察员
        :return:
        """
        viewer = AuthApi.roles_users.list({"role_id": "result_table.viewer", "scope_id": self.result_table_id})
        return viewer

    def data_manager_and_viewer(self):
        """
        rt的数据管理员 & 数据观察员
        :return:
        """
        manager = self.data_manager()
        viewer = self.data_viewer()
        return manager, viewer

    def data_category_alias(self):
        """
        rt对应的数据分类中文名
        :return:
        """
        data_category_alias = None
        if self.data.get("data_category", ""):
            data_category_dict = MetaApi.tag_info({"code": self.data["data_category"]}, raw=True)
            data_category_alias = data_category_dict.get("alias", "") if data_category_dict.get("result", "") else None
        return data_category_alias

    def judge_permission(self, username, action_type="query_data"):
        """查询用户有无结果表的查询权限"""
        object_type = "{}.{}".format("result_table", action_type)
        try:
            permission_dict = AuthApi.check_user_perm(
                {
                    "user_id": username,
                    "action_id": object_type,
                    "object_id": self.result_table_id,
                    "display_detail": True,
                }
            )
            has_permission = permission_dict.get("result", False)
            no_pers_reason = permission_dict.get("message", "")
        except DataError as e:
            logger.warning("rt judge_permission error: [%s]" % e.message)
            has_permission = False
            no_pers_reason = ""
        return has_permission, no_pers_reason

    def expires(self):
        """获取rt表的存储天数"""
        storage_types_dict = {}
        for key, value in six.iteritems(self.data.get("storages", {})):
            storage_types_dict[key] = subdigit(value.get("expires", ""))
        expires = storage_types_dict
        return expires

    def can_search(self):
        """查询rt是否有可以查询的存储"""
        can_search_list = ["tspider", "mysql", "druid", "tdw", "hdfs", "tsdb", "hermes", "es"]
        can_search = False
        for each_storage in self.storages_arr:
            if each_storage in can_search_list:
                can_search = True
                break
        return can_search

    def has_storage(self, storage):
        """
        查看 RT 是否有指定的存储
        """
        _arr = self.storages_arr
        return storage in _arr

    def has_kafka(self):
        """
        是否具有 KAFKA 存储，只要 RT（非离线） 需要入库某一类存储，都认定为具有 KAFKA 存储，因为 RT 要
        入库到存储，必须经过总线分发，而总线的介质层就是 kafka

                               (connector)
        RT ---------> kafka -------------------> Mysql

        """
        return self.has_storage("kafka")

    def has_hdfs(self):
        """
        是否具有 HDFS 存储，存储配置中显性配置以及离线表默认带上 HDFS
        """
        return self.has_storage("hdfs")

    def has_tredis(self):
        """
        是否具有 Tredis 存储
        """
        return self.has_storage("tredis")

    def has_ipredis(self):
        """
        是否具有 IPREDIS 存储
        """
        return self.has_storage("ipredis")

    def has_ignite(self):
        """
        是否具有 Ignite 存储
        """
        return self.has_storage("ignite")

    def has_tdw(self):
        """
        是否具有 tdw 存储
        """
        return self.has_storage("tdw")

    def is_clean(self):
        """
        结果表类型为1，则是clean
        """
        return self.result_table_type == "clean"

    def is_batch(self):
        """
        结果表类型为2，则是batch
        """
        return self.result_table_type == "batch"

    def judge_source_type(self, source_type):
        """
        判断是否为某种数据源类型
        :param source_type: 数据源类型 source_type 见类SourceType
        :return:
        """
        result = False

        if source_type == self.SourceType.RT_SOURCE:
            # 非清洗表就是结果数据
            result = not self.is_clean() and (self.has_kafka() or self.has_hdfs())
        elif source_type == self.SourceType.ETL_SOURCE:
            result = self.is_clean()
        elif source_type == self.SourceType.STREAM_SOURCE:
            result = self.has_kafka()
        elif source_type == self.SourceType.BATCH_SOURCE:
            result = self.has_hdfs()
        elif source_type == self.SourceType.KV_SOURCE:
            result = self.has_tredis() or self.has_ipredis()
        elif source_type == self.SourceType.BATCH_KV_SOURCE:
            result = self.has_ignite()
        elif source_type == self.SourceType.UNIFIED_KV_SOURCE:
            result = self.has_ignite() or self.has_ipredis()
        elif source_type == self.SourceType.TDW_SOURCE:
            result = self.can_be_tdw_source()

        return result

    def can_be_tdw_source(self):
        """
        # 判断能否作为tdw数据源表
        # 可作为TDW数据源的条件：
        # 1. 有TDW存储（storages包含tdw）
        #   1.1 受控表（is_managed=1)
        #   1.2 非受控表（is_managed=0)
        #       1.2.1 目前仅支持周期类型为小时和日的TDW表（self.count_freq_unit in ['H', 'd']）
        """

        def judge_count_freq():
            # @todo 由于产品问题，暂不判断统计频率，若不满足需求，保存节点时flowapi报错即可
            return True
            # return self.count_freq_unit in ['H', 'd']

        return self.has_tdw() and (self.is_managed or (not self.is_managed and judge_count_freq()))

    def is_exist(self):
        try:
            self._data = self._get_data()
            return True
        except ApiResultError:
            return False

    def _get_data(self):
        api_params = {"result_table_id": self.result_table_id, "related": ["storages", "fields", "result_table_type"]}
        return MetaApi.result_tables.retrieve(api_params)

    @classmethod
    def filter_supported_query_storages(cls, storages, support_storages=None):
        """
        过滤出支持在WEB页面查询的存储列表
        """
        # 支持查询的存储
        if support_storages is None:
            support_storages = cls.get_supported_storage_query()

        query_storages = [storage for storage in storages if storage in support_storages]
        return query_storages

    @classmethod
    def get_supported_storage_query(cls):
        """
        支持查询的存储，从StorekitApi中获取
        """
        return StorekitApi.common.list().get("storage_query", [])

    @property
    def storages_arr(self):
        """
        结果表已落的存储列表
        :return:
        """
        return list(self.data["storages"].keys())

    @property
    def storages(self):
        """
        结果表已落的存储信息
        :return:
        """
        return self.data["storages"]

    @property
    def query_storages_arr(self):
        """
        @note： 如果大量RT列表，轮训该属性，需要考虑性能问题
        """
        return self.filter_supported_query_storages(self.storages_arr)

    @property
    def support_storages(self):
        storages = list(self.data["storages"].keys())
        support_storages = [storage for storage in storages if storage in self.get_supported_storage_query()]
        return support_storages

    @property
    def description(self):
        return self.data["description"]

    @property
    def result_table_type(self):
        """
        结果表类型
        id=1: clean
        id=2: batch
        id=3: steam
        """
        return self.data.get("processing_type")

    @property
    def result_table_type_alias(self):
        """
        结果表类型中文名
        """
        # processing_type_dict = get_processing_type_dict()
        processing_type_alias = (
            processing_type_dict.get(self.result_table_type)
            if self.result_table_type and self.result_table_type not in ["storage", "view"]
            else ""
        )
        return processing_type_alias

    @property
    def platform(self):
        """
        结果表所属平台
        bkdata=蓝鲸基础计算平台
        tdw=TDW
        """
        return self.data.get("platform", "bkdata")

    @property
    def fields(self):
        """
        字段列表
        """
        return self.data.get("fields", [])

    @property
    def biz_id(self):
        return int(self.data["bk_biz_id"])

    @property
    def biz_name(self):
        all_biz_dict = Business.get_name_dict()
        if self.data.get("bk_biz_id", ""):
            return all_biz_dict.get(self.biz_id)
        else:
            return ""

    @property
    def project_id(self):
        return int(self.data.get("project_id", 0))

    @property
    def table_name(self):
        return self.data.get("result_table_name", "")

    @property
    def tdw_table_name(self):
        """
        TDW表名
        """
        try:
            return self.data["extra"]["tdw"]["table_name"]
        except KeyError:
            return ""

    @property
    def count_freq_unit(self):
        """
        TDW统计频率单位
        """
        try:
            return self.data["count_freq_unit"]
        except KeyError:
            return ""

    @property
    def count_freq(self):
        """
        TDW统计频率
        """
        try:
            return self.data["count_freq"]
        except KeyError:
            return 0

    @property
    def is_managed(self):
        """
        是否平台受控表
        """
        return self.data.get("is_managed", 1)

    @staticmethod
    def save_query_record(storage_type=None):
        """
        记录结果表查询记录装饰器
        """

        def _wrap(func):
            @wraps(func, assigned=available_attrs(func))
            def _deco(self, request, *args, **kwargs):
                param = request.data
                record = ResultTableQueryRecord()
                record.project_id = kwargs.get("project_id")
                record.result_table_ids = kwargs.get("result_table_id")
                record.operator = request.user.username
                record.storage_type = param.get("storage_type", storage_type)
                try:
                    _f = func(self, request, *args, **kwargs)
                    record.result = True
                    record.err_msg = None
                except exceptions.ApiResultError as e:
                    record.result = False
                    record.err_msg = e.message
                    raise e
                else:
                    record.time_taken = _f.data.get("time_taken")
                    record.total = _f.data.get("total")
                    return _f
                finally:
                    try:
                        defaults = {
                            "time_taken": record.time_taken,
                            "total": record.total,
                            "result": record.result,
                            "err_msg": record.err_msg,
                        }
                        unique = {
                            "project_id": record.project_id,
                            "operator": record.operator,
                            "storage_type": record.storage_type,
                            "result_table_ids": record.result_table_ids,
                        }
                        if record.storage_type == "es":
                            record.keyword = param.get("keyword")
                            # 无关键字时不记录
                            if record.keyword:
                                unique.update(
                                    {
                                        "keyword": record.keyword,
                                        "search_range_start_time": param.get("start_time"),
                                        "search_range_end_time": param.get("end_time"),
                                    }
                                )

                                ResultTableQueryRecord.objects.update_or_create(defaults=defaults, **unique)
                        else:
                            record.sql = param.get("sql")
                            unique.update(
                                {
                                    "sql": record.sql,
                                }
                            )
                            ResultTableQueryRecord.objects.update_or_create(defaults=defaults, **unique)
                    except Exception:
                        pass

            return _deco

        return _wrap

    @staticmethod
    def clear_default_field(fields):
        """
        默认去掉timestamp, offset等内部系统默认字段
        """
        return [_d for _d in fields if _d["field_name"] not in ["timestamp", "offset"]]

    def list_output_count(self, query_params):
        """
        数据输出量
        """
        query_params["data_set_ids"] = self.result_table_id
        try:
            output_data = DataManageApi.dmonitor_metrics.output_count(query_params)
        except (exceptions.ApiResultError, exceptions.ApiRequestError) as e:
            output_data = {"series": []}
            logger.error("dmonitor_metrics_output_data api error, error={}".format(e.message))
        return output_data

    def list_input_count(self, query_params):
        """
        数据输入量
        """
        query_params["data_set_ids"] = self.result_table_id
        try:
            output_data = DataManageApi.dmonitor_metrics.input_count(query_params)
        except (exceptions.ApiResultError, exceptions.ApiRequestError) as e:
            output_data = {"series": []}
            logger.error("dmonitor_metrics_input_data api error, error={}".format(e.message))
        return output_data


class ES(object):
    TIME_MULTIPLE = 1000
    TIME_OFFSET = {
        "7d": 3600 * 24 * 7 * TIME_MULTIPLE,
        "12h": 3600 * 12 * TIME_MULTIPLE,
        "24h": 3600 * 24 * TIME_MULTIPLE,
    }
    BODY_DATA = {
        "highlight": {
            "pre_tags": ["<em>"],
            "post_tags": ["</em>"],
            "fields": {
                "*": {"number_of_fragments": 0},
            },
            "require_field_match": False,
        },
        "query": {
            "bool": {
                "filter": [{"range": {"dtEventTime": {"gt": "", "lte": ""}}}],
                "must": [
                    {
                        "query_string": {
                            "fields": [],
                            "lenient": True,
                            "query": {},
                            "analyze_wildcard": True,
                        }
                    }
                ],
            }
        },
        "sort": [
            {
                "dtEventTimeStamp": {"order": "desc"},
            }
        ],
    }
    SPECIAL_STR_RE = re.compile(r"[\+\-=&|><!(){}\[\]^\"~*?:/]|AND|OR|TO|NOT")

    @classmethod
    def check_special_string(cls, string):
        """
        @summary 是否含有特殊字符
        """
        return re.search(cls.SPECIAL_STR_RE, string)

    @staticmethod
    def create_query_keyword(keyword, fields):
        """
        @summary 组装关键字
        """
        key_list = keyword.split(" ")
        query_list = []

        # 包含特殊字符则不加通配符，否则拿通配符包裹
        for query_key in key_list:
            if not query_key:
                continue
            pattern = "%s" if ES.check_special_string(query_key) else "*%s*"
            query_list.append(pattern % query_key)

        query_str = " ".join(query_list)

        query_dict = {
            "must": [
                {
                    "query_string": {
                        "query": query_str,
                        "fields": fields,
                        "lenient": True,
                        "analyze_wildcard": True,
                    }
                }
            ]
        }
        return dict(query_dict)

    @staticmethod
    def create_query_range(start_time, end_time):
        return {
            "filter": [
                {
                    "range": {
                        "dtEventTimeStamp": {
                            "from": timeformat_to_timestamp(start_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                            "to": timeformat_to_timestamp(end_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                        }
                    }
                }
            ]
        }

    @staticmethod
    def create_body_data(size, start, search_key, fields, start_time, end_time):
        """
        日志查询构造请求参数
        """

        body_data = deepcopy(ES.BODY_DATA)
        body_data.update(
            {
                "sort": [{"dtEventTimeStamp": "desc"}],
                "size": size,
                "from": start,
            }
        )
        body_data["query"]["bool"].update(ES.create_query_keyword(search_key, fields))
        body_data["query"]["bool"].update(ES.create_query_range(start_time, end_time))
        if search_key == "":
            body_data["query"]["bool"].pop("must")

        return body_data

    @staticmethod
    def get_index(result_table_id, start_time=None, end_time=None):
        """
        索引由 result_table_id_${日期} 组成，若不限制查询时间，会比较慢，建议加上时间
        :param result_table_id: 结果表
        :param start_time: 开始时间
        :param end_time: 结束时间
        :return: result_table_id_20181201,result_table_id_20190102,result_table_id_20190203
        """
        # 搜索当前结果表的所有es索引

        # 查询时长超过两个月时使用月份精度，小于两个月时使用日期精度
        if (arrow.get(end_time) - arrow.get(start_time)).days < 60:
            date_list = list_date_by_a_period_of_time(start_time, end_time)
            index_list = [
                "{result_table_id}_{date}".format(result_table_id=result_table_id, date=date) for date in date_list
            ]
        else:
            year_mont_list = list_year_month_by_a_period_of_time(start_time, end_time)
            index_list = [
                "{result_table_id}_{year_month}*".format(result_table_id=result_table_id, year_month=year_month)
                for year_month in year_mont_list
            ]
        return ",".join(index_list)

    @staticmethod
    def get_default_dsl(result_table_id):
        index = ES.get_index(result_table_id)
        time_zone = get_active_timezone_offset()
        time_zone = ":".join([time_zone[:3], time_zone[3:]])
        body_data = {
            "aggs": {
                "time_aggs": {
                    "date_histogram": {
                        "field": "dtEventTimeStamp",
                        "interval": "hour",
                        "time_zone": time_zone,
                        # "format": "yyyy-MM-dd"
                    }
                }
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "dtEventTimeStamp": {
                                    "gt": int(get_pizza_timestamp() * ES.TIME_MULTIPLE) - ES.TIME_OFFSET["24h"],
                                    "lte": int(get_pizza_timestamp() * ES.TIME_MULTIPLE),
                                    "format": "epoch_millis",
                                }
                            }
                        }
                    ],
                    "must": [{"query_string": {"query": "*", "analyze_wildcard": True}}],
                }
            },
            "sort": [
                {
                    "dtEventTimeStamp": {"order": "desc"},
                }
            ],
            "from": 0,
            "size": 10,
        }
        sql = json.dumps({"index": index, "body": body_data})
        return sql

    @staticmethod
    def get_search_dsl(size, start, keyword, start_time, end_time, result_table_id):
        """
        @summary:查询日志
        """
        index = ES.get_index(result_table_id, start_time, end_time)
        fields_detail = ResultTable(result_table_id).list_storage_fields()
        l_field_name = [_f["field_name"] for _f in fields_detail]
        body_data = ES.create_body_data(size, start, keyword, l_field_name, start_time, end_time)

        sql = json.dumps({"index": index, "body": body_data})
        return sql

    @staticmethod
    def get_chart_dsl(keyword, interval, start_time, end_time, result_table_id):
        """
        获取以一定时间频率为间隔聚合统计的图表数据
        @param {String} keyword 搜索关键字
        @param {String} date_interval 时间间隔（7d, 12h, 24h）
        """

        index = ES.get_index(result_table_id, start_time, end_time)

        body_dict = {
            "aggs": {
                "time_aggs": {
                    "date_histogram": {
                        "field": "dtEventTimeStamp",
                        "interval": interval,
                        # "time_zone": "+00:00",
                    }
                }
            },
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "dtEventTimeStamp": {
                                    "from": timeformat_to_timestamp(start_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                                    "to": timeformat_to_timestamp(end_time, DTEVENTTIMESTAMP_MULTIPLICATOR),
                                    # 'format': 'epoch_millis'
                                }
                            }
                        }
                    ],
                    "must": [{"query_string": {"query": "*", "analyze_wildcard": True}}],
                }
            },
        }
        if keyword:
            fields_detail = ResultTable(result_table_id).list_storage_fields()
            l_field_name = [_f["field_name"] for _f in fields_detail]
            query_dict = ES.create_query_keyword(keyword, l_field_name)
            body_dict["query"]["bool"].update(query_dict)
        sql = json.dumps({"index": index, "body": body_dict})
        return sql

    @staticmethod
    def get_es_chart(sql):
        api_param = {"prefer_storage": "es", "sql": sql}
        response = DataQueryApi.query(api_param)
        try:
            buckets = response["list"]["aggregations"]["time_aggs"]["buckets"]
        except KeyError:
            buckets = []
        _time = []
        cnt = []
        for item in buckets:
            _time.append(timestamp_to_timeformat(item["key"], DTEVENTTIMESTAMP_MULTIPLICATOR))
            cnt.append(item["doc_count"])

        data = {"time": _time, "cnt": cnt}
        return data
