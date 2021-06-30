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
import random
import re
import string

from django.utils.translation import ugettext as _
from six.moves import range

from apps import exceptions
from apps.api import DatabusApi, DataManageApi
from apps.common.log import logger
from apps.exceptions import DataError
from apps.utils import APIModel


class ETL(APIModel):
    """
    清洗配置，目前清洗配置使用 result_table_id 作为唯一标识
    """

    FORM_ROOT_LABEL = "root"

    def __init__(self, result_table_id):
        super(ETL, self).__init__()
        self.result_table_id = result_table_id
        self._task = None

    def list_monitor_data_loss_total(self, io_type, interval="6h", group_time="1m"):
        """
        获取清洗 RT 统计数据量

        @param {String} io_type 可选值有 input（输入）| output（输出）
        """
        sql = (
            'SELECT sum("data_inc") as cnt '
            "FROM data_loss_{io_type}_total "
            "WHERE logical_tag = '{result_table_id}' "
            "AND ((module='{module}' AND component='{component}') "
            "OR (module='databus' AND component =~ /clean*/)) "
            "AND time > now() - {interval} "
            "GROUP BY time({group_time}) "
            "fill(null)"
        ).format(
            io_type=io_type,
            module=self.module,
            component=self.component,
            result_table_id=self.result_table_id,
            interval=interval,
            group_time=group_time,
        )

        api_params = {"database": "monitor_data_metrics", "sql": sql, "tags": self.geog_area}
        series = DataManageApi.dmonitor_metrics.query(api_params).get("series", [])

        cleaned_series = {
            "cnt": [data["cnt"] for data in series],
            "time": [data["time"] for data in series],
        }
        return cleaned_series

    def list_monitor_data_loss_drop(self, interval="1h"):
        """
        获取清洗 RT 统计丢弃量
        """
        sql = (
            "SELECT reason, data_cnt "
            "FROM data_loss_drop "
            "WHERE time > now() - {interval} "
            "AND logical_tag = '{rt_id}' "
            "AND module='{module}' "
            "AND component='{component}' "
            "ORDER BY time DESC "
        ).format(interval=interval, module=self.module, component=self.component, rt_id=self.result_table_id)

        api_params = {"database": "monitor_data_metrics", "sql": sql, "tags": self.geog_area}
        return DataManageApi.dmonitor_metrics.query(api_params).get("series", [])

    def start(self, raise_exception=True):
        """
        启动清洗程序
        """
        api_params = {"result_table_id": self.result_table_id, "storages": ["kafka"]}
        try:
            return DatabusApi.tasks.create(api_params)
        except (exceptions.ApiRequestError, exceptions.ApiResultError) as e:
            if raise_exception:
                raise e
            else:
                logger.error("Skip error for starting connector, error={}".format(e.message))

    def stop(self):
        """
        停止清洗程序
        """
        api_params = {"result_table_id": self.result_table_id, "storages": ["kafka"]}
        return DatabusApi.tasks.delete(api_params)

    @classmethod
    def form_to_etl(cls, form_config):
        """
        前端表单配置转换为可解析ETL配置

        @param form_config 表单配置
        """
        graph = cls.extract_graph(form_config)

        # 检查是否存在环
        cls.check_undirected_circle(graph)

        # 目前仅支持一个清洗起点
        if len(graph[cls.FORM_ROOT_LABEL]) > 1:
            raise exceptions.ETLCheckError(_("当前仅支持对 RawData 进行1次处理"))
        if len(graph[cls.FORM_ROOT_LABEL]) == 0:
            raise exceptions.ETLCheckError(_("请在步骤1中对 RawData 进行处理"))

        # 遍历算子，生成配置
        return cls.build_etl_node(graph[cls.FORM_ROOT_LABEL][0], form_config, graph)

    @classmethod
    def build_etl_node(cls, label, form_config, graph):
        """
        calc(form_config) -> node(etl_config)
        """
        _calc = cls.get_calc_by_label(form_config, label)
        _calc_id = _calc["calc_id"]

        _node = get_calc_handler(_calc_id).to_etl_node(_calc)

        # 计算子步骤数量，决定了 next 拼接方式
        _child_labels = graph[label]
        if len(_child_labels) == 0:
            _node["next"] = None
        elif len(_child_labels) == 1:
            _node["next"] = cls.build_etl_node(_child_labels[0], form_config, graph)
        else:
            _nexts = []
            _child_labels.sort()
            for _child in _child_labels:
                _nexts.append(cls.build_etl_node(_child, form_config, graph))

            _node["next"] = {"type": "branch", "name": "", "label": None, "next": _nexts}

        return _node

    @classmethod
    def check_undirected_circle(cls, graph):
        """
        检查是否存在无向回路
        """
        _stack = [cls.FORM_ROOT_LABEL]
        _iter_labels = []

        while len(_stack):
            _label = _stack.pop()
            if _label in _iter_labels:
                _err = _("存在回路，步骤（{}）重复出现".format(_label))
                raise exceptions.ETLCheckError(_err)

            _iter_labels.append(_label)
            for _child_label in graph[_label]:
                _stack.append(_child_label)

        return True

    @classmethod
    def extract_graph(cls, form_config):
        """
        提取出图信息，便于查找上下关系

        @returnExample
            {
                'root': [1, 2],
                2: [3]
            }
        """
        # 初始化空关系图
        graph = {cls.FORM_ROOT_LABEL: []}
        for _calc in form_config:
            graph[_calc["label"]] = []

        for _calc in form_config:
            graph[_calc["input"]].append(_calc["label"])

        return graph

    @classmethod
    def etl_to_form(cls, elt_config):
        """
        可解析ETL配置转换为前端表单配置

        @param elt_config 可解析ETL配置
        """
        form_config = []

        root_node = elt_config
        cls.build_form_calc(root_node, {}, form_config)
        return form_config

    @classmethod
    def template_to_form(cls, template):
        """
        清洗模板配置装换为前端表单配置
        """
        config = {}
        json_config = template.get("json_config", {})
        config["conf"] = cls.etl_to_form(json_config.get("extract", {}))
        config["fields"] = template.get("fields", [])
        return config

    @classmethod
    def get_template_by_data_id(cls, data_id):
        """
        根据data_id获取清洗模板
        """
        try:
            template = DatabusApi.cleans.etl_template({"raw_data_id": data_id})
        except DataError as e:
            # 忽略模板的异常
            logger.error("获取清洗模板失败{}, dataid:{}".format(e, data_id))
            template = {}
        return cls.template_to_form(template)

    @classmethod
    def get_etl_hint(cls, config_data):
        """
        推荐算子
        """
        _etl_conf = ETL.form_to_etl(config_data["conf"])
        api_param = {"msg": config_data["msg"], "conf": json.dumps(_etl_conf)}
        data = {}
        _hints = DatabusApi.cleans.hint(api_param)
        for _lable, _value in list(_hints.items()):
            front_hints = []

            for _hint in _value.get("hints", []):
                _front_hint = get_calc_handler_by_node(_hint).to_form_calc(_hint)
                _front_hint["input"] = _lable
                front_hints.append(_front_hint)
            data[_lable] = front_hints
        return data

    @classmethod
    def verify_etl_conf(cls, debug_data):
        """
        清洗调试
        :param debug_data: 调试数据
        :return:
        """
        _etl_conf = cls.form_to_etl(debug_data["conf"])
        api_param = {
            "msg": debug_data["msg"],
            "conf": json.dumps(_etl_conf),
            "debug_by_step": debug_data["debug_by_step"],
        }
        data = DatabusApi.cleans.verify(api_param)

        for _label, _err in list(data["errors"].items()):
            if not _err:
                data["errors"].pop(_label)
        return data

    @classmethod
    def build_form_calc(cls, node, parent, form_config):
        """
        node(etl_config) -> calc(form_config)，在层层递归中，填充form_config算子列表
        """
        # 从后台生成清洗配置不存在 label 这里需要补充随机ID
        if "label" not in node:
            node["label"] = get_random_id()

        calc = get_calc_handler_by_node(node).to_form_calc(node)

        # 有关算子的上下关系，在整体拼接位置补充
        calc["input"] = parent.get("label", cls.FORM_ROOT_LABEL)
        form_config.append(calc)

        # 如果不存在子节点，则递归结束
        if "next" not in node or node["next"] is None:
            return

        # 如果子节点为 branch 节点，需要深入一层
        if node["next"]["type"] == "branch":
            nexts = node["next"]["next"]
        else:
            nexts = [node["next"]]

        for _next in nexts:
            cls.build_form_calc(_next, node, form_config)

    def _get_data(self):
        return DatabusApi.cleans.retrieve({"processing_id": self.result_table_id})

    def format_data(self):
        """
        格式化数据
        """
        result_table = self.data["result_table"]
        conf = self.etl_conf
        return {
            "clean_config_name": self.data["clean_config_name"],
            "raw_data_id": self.raw_data_id,
            "result_table_id": self.result_table_id,
            "result_table_name": result_table["result_table_name"],
            "result_table_name_alias": result_table["result_table_name_alias"],
            "description": self.data["description"],
            "time_format": conf["time_format"],
            "timezone": conf["timezone"],
            "time_field_name": conf["time_field_name"],
            "encoding": conf.get("encoding", "UTF8"),
            "timestamp_len": conf.get("timestamp_len", 0),
            "updated_by": self.data["updated_by"],
            "updated_at": self.data.get("updated_at"),
            "created_by": self.data["created_by"],
            "created_at": self.data.get("created_at"),
            "conf": self.form_config,
            "raw_config": self.etl_json_config,
            "fields": self.fields,
            "status": self.status,
            "status_display": self.status_display,
        }

    @property
    def raw_data_id(self):
        return self.data["raw_data_id"]

    @property
    def etl_json_config(self):
        """
        获取ETL数据
        """
        return json.loads(self.data["json_config"])

    @property
    def form_config(self):
        return self.etl_to_form(self.etl_json_config["extract"])

    @property
    def etl_conf(self):
        return self.etl_json_config["conf"]

    @property
    def fields(self):
        # 清洗的第一个字段为timestamp时间字段，不需要暴露给用户
        return self.data["result_table"]["fields"][1:]

    @property
    def status(self):
        return self.data.get("status", "no-status")

    @property
    def status_display(self):
        return self.data.get("status_display", self.status)

    @property
    def task(self):
        if self._task is None:
            self._task = DatabusApi.tasks.retrieve({"result_table_id": self.result_table_id, "storage": "kafka"})

        return self._task

    @property
    def module(self):
        """
        清洗结果表上报数据质量的模块
        :return:
        """
        return self.task.get("module", "clean")

    @property
    def component(self):
        """
        清洗结果表上报数据质量的组件
        :return:
        """
        return self.task.get("component", "*")

    @property
    def geog_area(self):
        """
        区域标签
        """
        try:
            return [tag["code"] for tag in self.data["result_table"]["tags"]["manage"]["geog_area"]]
        except KeyError:
            return []

    @staticmethod
    def get_calc_by_label(form_config, label):
        """ """
        for _calc in form_config:
            if _calc["label"] == label:
                return _calc

        raise exceptions.ETLAnalyseError("invalid label: {}".format(label))


class FunctionCalc(object):
    """
    基类算子，函数算子
    """

    METHOD = None

    @classmethod
    def to_etl_node(cls, calc):
        """
        前端参数->后端参数
        """
        return {
            "type": "fun",
            "method": cls.METHOD,
            "result": calc["calc_params"]["result"],
            "label": calc["label"],
            "args": cls.build_args(calc),
        }

    @classmethod
    def build_args(cls, calc):
        """
        函数算子，calc->node，相关的参数需要差异化配置，此处留出回调
        """
        return []

    @classmethod
    def to_form_calc(cls, node):
        """
        后端参数->前端参数
        """
        _params = {"result": node.get("result") or node.get("label")}
        _params.update(cls.build_params(node))
        return {"input": "", "calc_id": cls.METHOD, "calc_params": _params, "label": node.get("label")}

    @classmethod
    def build_params(cls, node):
        """
        函数算子，node->calc，相关的参数需要差异化配置，此处留出回调
        """
        return {}


class FromJsonCalc(FunctionCalc):
    METHOD = "from_json"


class FromJsonListCalc(FunctionCalc):
    METHOD = "from_json_list"


class CsvCalc(FunctionCalc):
    METHOD = "csvline"


class FromUrlCalc(FunctionCalc):
    METHOD = "from_url"


class IterateCalc(object):
    @classmethod
    def to_etl_node(cls, calc):
        calc_params = calc["calc_params"]
        iteration_type = calc_params["iteration_type"]
        node_params = {"type": "fun", "label": calc["label"], "result": calc["calc_params"]["result"], "args": []}
        if iteration_type == "list":
            node_params["method"] = "iterate"
        elif iteration_type == "hash":
            node_params["method"] = "items"
        return node_params

    @classmethod
    def to_form_calc(cls, node):
        first_args = node["args"][0] if len(node["args"]) > 0 else node.get("label")
        if first_args is None:
            first_args = get_random_id()

        if node["method"] == "iterate":
            return {
                "input": "",
                "calc_id": "iterate",
                "calc_params": {"iteration_type": "list", "result": node.get("result") or node.get("label")},
                "label": node.get("label"),
            }
        elif node["method"] == "items":
            return {
                "input": "",
                "calc_id": "iterate",
                "calc_params": {"iteration_type": "hash", "result": node.get("result") or node.get("label")},
                "label": node.get("label"),
            }


class SplitCalc(FunctionCalc):
    METHOD = "split"

    @classmethod
    def build_args(cls, calc):
        args = [calc["calc_params"]["delimiter"]]
        limit = calc["calc_params"].get("limit")
        if limit:
            try:
                args.append(int(limit))
            except ValueError:
                return args
        return args

    @classmethod
    def build_params(cls, node):
        return {"delimiter": node["args"][0], "limit": node["args"][1] if len(node["args"]) == 2 else None}


class SplitKvCalc(FunctionCalc):
    METHOD = "splitkv"

    @classmethod
    def build_args(cls, calc):
        return [calc["calc_params"]["record_split"], calc["calc_params"]["kv_split"]]

    @classmethod
    def build_params(cls, node):
        return {"record_split": node["args"][0], "kv_split": node["args"][1] if len(node["args"]) > 1 else ""}


class ReplaceCalc(FunctionCalc):
    METHOD = "replace"

    @classmethod
    def build_args(cls, calc):
        return [calc["calc_params"]["from"], calc["calc_params"]["to"]]

    @classmethod
    def build_params(cls, node):
        return {"from": node["args"][0], "to": node["args"][1] if len(node["args"]) > 1 else ""}


class PopCalc(FunctionCalc):
    METHOD = "pop"


class ZipCalc(FunctionCalc):
    METHOD = "zip"

    @classmethod
    def build_args(cls, calc):
        return [calc["calc_params"]["list_key_a"], calc["calc_params"]["list_key_b"]]

    @classmethod
    def build_params(cls, node):
        return {"list_key_a": node["args"][0], "list_key_b": node["args"][1] if len(node["args"]) > 1 else ""}


class AssignCalc(object):
    @classmethod
    def to_etl_node(cls, calc):
        if calc["calc_params"]["assign_method"] == "key":
            return {
                "type": "assign",
                "subtype": "assign_obj",
                "label": calc["label"],
                "assign": [
                    {"type": _f["type"], "assign_to": _f["assign_to"], "key": _f["location"]}
                    for _f in calc["calc_params"]["fields"]
                ],
            }
        elif calc["calc_params"]["assign_method"] == "index":
            return {
                "type": "assign",
                "subtype": "assign_pos",
                "label": calc["label"],
                "assign": [
                    {"type": _f["type"], "assign_to": _f["assign_to"], "index": _f["location"]}
                    for _f in calc["calc_params"]["fields"]
                ],
            }
        elif calc["calc_params"]["assign_method"] == "json":
            return {
                "type": "assign",
                "subtype": "assign_json",
                "label": calc["label"],
                "assign": [
                    {"type": _f["type"], "assign_to": _f["assign_to"], "key": _f["location"]}
                    for _f in calc["calc_params"]["fields"]
                ],
            }
        else:
            # assign_value直接赋值
            return {
                "type": "assign",
                "subtype": "assign_value",
                "label": calc["label"],
                "assign": {
                    "type": calc["calc_params"]["fields"]["type"],
                    "assign_to": calc["calc_params"]["fields"]["assign_to"],
                },
            }

    @classmethod
    def to_form_calc(cls, node):
        if node["subtype"] == "assign_obj":
            keys = node.get("keys", [])
            return {
                "input": "",
                "calc_id": "assign",
                "calc_params": {
                    "assign_method": "key",
                    "fields": [
                        {"type": _f["type"], "assign_to": _f["assign_to"], "location": _f["key"]}
                        for _f in node["assign"]
                    ],
                    "keys": keys,
                },
                "label": node.get("label"),
            }
        elif node["subtype"] == "assign_json":
            return {
                "input": "",
                "calc_id": "assign",
                "calc_params": {
                    "assign_method": "json",
                    "fields": [
                        {"type": _f["type"], "assign_to": _f["assign_to"], "location": _f["key"]}
                        for _f in node["assign"]
                    ],
                },
                "label": node.get("label"),
            }
        elif node["subtype"] == "assign_pos":
            length = node.get("length", 1)
            return {
                "input": "",
                "calc_id": "assign",
                "calc_params": {
                    "assign_method": "index",
                    "fields": [
                        {"type": _f["type"], "assign_to": _f["assign_to"], "location": _f["index"]}
                        for _f in node["assign"]
                    ],
                    "length": length,
                },
                "label": node.get("label"),
            }
        else:
            # assign_value 直接赋值
            return {
                "input": "",
                "calc_id": "assign",
                "calc_params": {
                    "assign_method": "direct",
                    "fields": {
                        "type": node["assign"]["type"],
                        "assign_to": node["assign"]["assign_to"],
                    },
                },
                "label": node.get("label"),
            }


class AccessCalc(object):
    @classmethod
    def to_etl_node(cls, calc):
        access_method = calc["calc_params"]["assign_method"]

        if access_method == "key":
            return {
                "type": "access",
                "subtype": "access_obj",
                "label": calc["label"],
                "key": calc["calc_params"]["location"],
                "result": calc["calc_params"]["result"],
                "default_type": calc["calc_params"].get("default_type", "null"),
                "default_value": calc["calc_params"].get("default_value", ""),
            }
        else:
            return {
                "type": "access",
                "subtype": "access_pos",
                "label": calc["label"],
                "index": calc["calc_params"]["location"],
                "result": calc["calc_params"]["result"],
                "default_type": calc["calc_params"].get("default_type", "null"),
                "default_value": calc["calc_params"].get("default_value", ""),
            }

    @classmethod
    def to_form_calc(cls, node):
        result = node.get("result") or node.get("label")
        if node["subtype"] == "access_obj":
            keys = node.get("keys", [])
            return {
                "input": "",
                "calc_id": "access",
                "calc_params": {
                    "assign_method": "key",
                    "location": node.get("key"),
                    "result": result,
                    "keys": keys,
                    "default_type": node.get("default_type", "null"),
                    "default_value": node.get("default_value", ""),
                },
                "label": node.get("label"),
            }
        else:
            length = node.get("length", 1)
            return {
                "input": "",
                "calc_id": "access",
                "calc_params": {
                    "assign_method": "index",
                    "location": node.get("index"),
                    "result": result,
                    "length": length,
                    "default_type": node.get("default_type", "null"),
                    "default_value": node.get("default_value", ""),
                },
                "label": node.get("label"),
            }


class RegexExtractCalc(FunctionCalc):
    """
    正则提取算子
    """

    METHOD = "regex_extract"

    @classmethod
    def to_etl_node(cls, calc):
        """
        前端参数->后端参数
        """
        keys = cls.get_regex_keys(calc["calc_params"]["regex"])
        regexp = cls.replace_regex(calc["calc_params"]["regex"], keys)
        return {
            "type": "fun",
            "method": cls.METHOD,
            "label": calc["label"],
            "args": [{"result": calc["calc_params"]["result"], "keys": keys, "regexp": regexp}],
        }

    @classmethod
    def to_form_calc(cls, node):
        """
        后端参数->前端参数
        """
        regex = cls.restore_regex(node["args"][0].get("regexp"), node["args"][0].get("keys"))
        _params = {"result": node["args"][0].get("result"), "regex": regex}
        _params.update(cls.build_params(node))
        return {"input": "", "calc_id": cls.METHOD, "calc_params": _params, "label": node.get("label")}

    @staticmethod
    def get_regex_keys(regex):
        """
        通过正则表达式获取 key
        :param regex: (?P<remote_ip>[^ ]*) (?P<date>[^ ]*)
        :return:[remote_ip, date]
        """
        # 正则表达式不符合规范, 后端 API 会有异常, 直接在这里做处理
        try:
            regexp = re.compile(regex)
        except Exception as e:
            error_msg = "正则表达式错误({regex})--({e})"
            logger.warning(error_msg.format(regex=regex, e=e))
            raise exceptions.ETLCheckError(_(error_msg.format(regex=regex, e=e)))
        else:
            if regexp.groupindex:
                return list(regexp.groupindex.keys())
            raise exceptions.ETLCheckError(_("正则表达式不规范({regex})".format(regex=regex)))

    @staticmethod
    def replace_regex(regex, keys):
        """
        改变正则表达式适配后端 Java
        """
        for key in keys:
            regex = regex.replace("?P<{}>".format(key), "?<{}>".format(key))
        return regex

    @staticmethod
    def restore_regex(regex, keys):
        """
        还原正则表达式
        """
        for key in keys:
            regex = regex.replace("?<{}>".format(key), "?P<{}>".format(key))
        return regex


def get_calc_handler(calc_id):
    m_calc = {
        "from_json": FromJsonCalc,
        "from_json_list": FromJsonListCalc,
        "csvline": CsvCalc,
        "from_url": FromUrlCalc,
        "split": SplitCalc,
        "splitkv": SplitKvCalc,
        "replace": ReplaceCalc,
        "pop": PopCalc,
        "zip": ZipCalc,
        "iterate": IterateCalc,
        "items": IterateCalc,
        "assign": AssignCalc,
        "access": AccessCalc,
        "regex_extract": RegexExtractCalc,
    }
    if calc_id not in m_calc:
        raise exceptions.ETLCheckError(_("非法清洗算子({})".format(calc_id)))
    return m_calc[calc_id]


def get_calc_handler_by_node(node):
    _type = node["type"]

    if _type == "fun":
        m_method_calc = {
            "from_json": FromJsonCalc,
            "from_json_list": FromJsonListCalc,
            "from_url": FromUrlCalc,
            "split": SplitCalc,
            "replace": ReplaceCalc,
            "pop": PopCalc,
            "csvline": CsvCalc,
            "zip": ZipCalc,
            "splitkv": SplitKvCalc,
            "iterate": IterateCalc,
            "items": IterateCalc,
            "regex_extract": RegexExtractCalc,
        }
        return m_method_calc[node["method"]]
    if _type == "access":
        return AccessCalc
    elif _type == "assign":
        return AssignCalc
    else:
        raise exceptions.ETLCheckError(_("非法ETL类型({})".format(_type)))


def get_random_id(length=5):
    """
    获得一个随机字符串ID

    @param {Int} length 随机ID的长度
    """
    return "label%s" % "".join(random.choice(string.ascii_lowercase) for _ in range(length))
