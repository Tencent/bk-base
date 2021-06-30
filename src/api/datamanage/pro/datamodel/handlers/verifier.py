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
import operator
import re

import sqlparse
from datamanage import pizza_settings as settings
from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.model_dict import InnerField, SchedulingEngineType
from datamanage.pro.datamodel.utils import (
    get_sql_fields_alias_mapping,
    get_sql_source_columns_mapping,
)
from datamanage.utils.api import DataflowApi
from django.core.cache import cache

from common.log import logger

from . import time

python_string_type = str


class SQLVerifier(object):

    sql_template = """SELECT {fields} FROM {table_name} {conditions} {appends};"""
    default_map = dict(table_name="table_name", fields="*", conditions="", appends="")
    cast_type_mapping = dict(
        int="int",
        float="float",
        double="double",
        string={
            SchedulingEngineType.FLINK.value: "varchar",
            SchedulingEngineType.SPARK.value: "string",
        },
    )
    requests_clients = {
        SchedulingEngineType.FLINK.value: DataflowApi.flink_sql_check,
        SchedulingEngineType.SPARK.value: DataflowApi.spark_sql_check,
    }

    def __init__(self, bk_biz_id=1, sql=None, sql_parts=None, scopes=None, auto_format=True):
        """
        初始化sql校验模块，传入sql语句或局部sql以及必要参数
        SELECT [fields] FROM [table] WHERE [conditions] [appends]

        :param bk_biz_id: int 业务id
        :param sql: string 完整sql语句 "SELECT * FROM table WHERE 1"
        :param sql_parts: dict 局部sql，传入后需要对sql进行补全(sql参数若不为空，优先于sql_parts)
            {
                "fields": {"field_alias1": "field_name", "field_alias2":"concat(field_name, '_', 'ex')"},
                # fields中，key是输出的字段名称，value是输入表达式(或输入字段名称)
                "conditions": "a >= 1 and b < 2",
                "appends": {"group_by": ["b", "c"]} # OR "GROUP BY b, c"
                "table_name": "591_durant1119"
            }
        :param scopes: dict sql执行可引用的表范围
            {"591_durant1119": [
                    {
                        "field_type": "string",
                        "field_name": "a"
                    }
                ]
            }
        }
        :param auto_format: boolean 是否自动格式化sql内容
        """
        self.bk_biz_id = bk_biz_id
        self.auto_format = auto_format
        self.sql = sql
        self.sql_parts = sql_parts if sql_parts is not None else {}
        self.scopes = dict()
        self.source_table_mapping = dict()
        self._set_scope_table_name(scopes)
        self.sql_alias_exp_mapping = dict()
        self._set_sql_alias_exp_mapping()
        self.logger = logger
        self.err_trace = dict()

    @staticmethod
    def _pre_deal_scope_list(scope_field_list):
        """
        预处理scope字段列表
        1.去除无效字段
        2.添加默认字段

        :param scope_field_list: list
        :return: list
        """
        processed_scope_field_list = list()
        if isinstance(scope_field_list, list):
            processed_scope_field_list = [
                field_item
                for field_item in scope_field_list
                if isinstance(field_item, dict)
                and field_item.get("field_type", "")
                and field_item.get("field_name", "")
            ]
        processed_scope_field_list.append(dict(field_type="string", field_name="verify__bk_data_sql__default"))
        return processed_scope_field_list

    def _set_scope_table_name(self, scopes):
        """
        对scope表的表名称进行预处理

        :param scopes: dict sql执行可引用的表范围
        :return: None
        """

        raw_scopes = scopes if scopes is not None else {}
        # Todo: 因防止缓存竞争冲突，主表名称会增加随机后缀，所以需要对scope中的表名进行替换，当期因需要只替换了主表表名, 完备方案有两套:
        #   1.首选, 推动flink去掉缓存中转的方案，直接支持参数传递schema, 即可取消随机后缀
        #   2.对多输入scope情况中主表外其他所有表进行随机后缀替换，且需要替换sql中对scope表的引用表名, 较复杂，暂未实现
        if self.sql is None and self.sql_parts:
            raw_main_table_name = self.sql_parts.get("table_name", "")
            self.sql_parts["table_name"] = self._gen_table_name(self.bk_biz_id, raw_main_table_name)
            for scope_table_name, scope_table_info in list(raw_scopes.items()):
                tb_name = self.sql_parts["table_name"] if scope_table_name == raw_main_table_name else scope_table_name
                scope_table_info = self._pre_deal_scope_list(scope_table_info)
                self.scopes[tb_name] = scope_table_info
                self.source_table_mapping[tb_name] = scope_table_name
        else:
            self.scopes = raw_scopes

    def _set_sql_alias_exp_mapping(self):
        """
        从输入中获取字段别名和表达式的关系

        :return: None
        """

        fields_part = self.sql_parts.get(str("fields"), {})
        for field_alias, field_exp in list(fields_part.items()):
            if field_alias and field_exp:
                self.sql_alias_exp_mapping[field_alias] = field_exp

    @staticmethod
    def get_func_list(expr, prefix=None):
        """
        从表达式中捕获使用的函数

        :param expr: string 表达式
        :param prefix: string 指定函数名的前缀
        :return: list 捕获函数列表
        """
        func_list = list(set(re.findall(r"([\S]+?)\(", expr)))
        if prefix and isinstance(prefix, str):
            prefix = prefix.lower()
            return list({func_name for func_name in func_list if func_name.lower().startswith(prefix)})
        return func_list

    @classmethod
    def gen_simple_sql(cls, table_name=None, fields=None, conditions=None, appends=None):
        """
        生成一个基础sql

        :param table_name: string 表名
        :param fields: string 字段表达式
        :param conditions: string 条件表达式(包含where)
        :param appends: string 聚合表达式
        :return: string sql表达式
        """
        raw_sql = cls.sql_template.format(
            table_name=table_name if table_name else cls.default_map["table_name"],
            fields=fields if fields else cls.default_map["fields"],
            conditions=conditions if conditions else cls.default_map["conditions"],
            appends=appends if appends else cls.default_map["appends"],
        )
        return sqlparse.format(raw_sql, strip_comments=True, strip_whitespace=True).strip()

    def gen_sql_expression(self, require_schema, engine):
        """
        根据局部sql和参数构建校验用的sql表达式

        :param require_schema: dict 希望sql返回的schema
        :param engine: string 执行sql的引擎
        :return: 返回str的sql语句
        """

        require_schema_fields = list(require_schema.values())[0]
        field_type_mapping = {
            str(field_item["field_name"]): str(field_item["field_type"]) for field_item in require_schema_fields
        }
        raw_sql = self.sql_template.format(
            table_name=self.sql_parts["table_name"],
            fields=self._gen_fields_exp(field_type_mapping, engine),
            conditions=self._gen_conditions_exp(),
            appends=self._gen_appends_exp(),
        )
        self.sql = sqlparse.format(raw_sql, strip_comments=self.auto_format, strip_whitespace=self.auto_format).strip()
        return self.sql

    @staticmethod
    def _gen_table_name(prefix, table_name):
        """
        生成表名

        :param prefix: 业务id前缀(兼容flink接口)
        :param table_name: 表名
        :return: string 字符串表名
        """
        # 默认带上时间戳后缀，避免缓存竞争
        table_name = "{}_{}".format(table_name, int(round(time.time() * 1000)))
        return "{}_{}".format(prefix, table_name) if not table_name.startswith("{}_".format(prefix)) else table_name

    def _gen_fields_exp(self, field_type_mapping, engine):
        """
        SQL片段 fields部分
        {"key1": "field_1", "key2": "sum(a) as field_2"}
        变量解释: field_name: 模型要求的输出字段 field_alias: 从表达式解析出来的别名 field_expr: sql字段表达式

        :param field_type_mapping: dict 字段类型映射字典
        :param engine: string 执行sql的引擎
        :return: string 字段表达式
        """

        field_exp_list = []
        fields_alias_mapping = get_sql_fields_alias_mapping(
            self.sql_alias_exp_mapping,
            strip_comments=self.auto_format,
            strip_whitespace=self.auto_format,
        )
        for field_name, field_alias_item in list(fields_alias_mapping.items()):
            field_alias = field_alias_item["alias"]
            field_expr = field_alias_item["expr"]
            target_field_type = self.get_target_parse_type(field_type_mapping[field_name], engine)
            # 表达式和模型字段名一致(没做处理)
            if field_expr == field_name:
                field_exp_list.append("cast(`{}` as {}) as `{}`".format(field_expr, target_field_type, field_name))
            elif field_alias:
                # 表达式中设置了指定别名
                if field_alias == field_name:
                    field_exp_list.append("cast({} as {}) as `{}`".format(field_expr, target_field_type, field_name))
                # 表达式中没有设置指定别名
                else:
                    raise dm_pro_errors.AliasSetError(message_kv={"alias": field_name, "err_alias": field_alias})
            else:
                # 表达式没有设置别名，自动补充
                field_exp_list.append("cast({} as {}) as `{}`".format(field_expr, target_field_type, field_name))
        return ", ".join(field_exp_list)

    def _gen_conditions_exp(self):
        """
        SQL片段 conditions部分
        "a >= 1 and b < 2" OR dict(暂未实现)

        :return: string 条件表达式
        """

        conditions_part = self.sql_parts.get(str("conditions"), None)
        if conditions_part and isinstance(conditions_part, dict):
            # TODO:暂未实现字典类型传入
            return ""
        conditions_expr = format(conditions_part) if conditions_part else self.default_map["conditions"]
        return (
            "WHERE {}".format(conditions_expr)
            if conditions_expr and not conditions_expr.strip().lower().startswith("where")
            else conditions_expr
        )

    def _gen_appends_exp(self):
        """
        SQL片段 appends部分

        {"group_by": ["b", "c"]} # OR "GROUP BY b, c"
        :return: string 聚合表达式
        """

        appends_part = self.sql_parts.get(str("appends"), None)
        if appends_part and isinstance(appends_part, dict):
            appends_exp_template = "{group_by_exp}"
            group_by_exp_str = ""
            group_by_exp_list = appends_part.get("group_by", [])
            if group_by_exp_list:
                group_by_exp_str = "GROUP BY {}".format(", ".join(group_by_exp_list))
            return appends_exp_template.format(group_by_exp=group_by_exp_str)
        return appends_part if appends_part else self.default_map["appends"]

    @classmethod
    def get_target_parse_type(cls, require_type, engine):
        """
        根据类型和engine获取实际cast的目标类型

        :param require_type: 用户需要的通用字段类型
        :param engine: 执行引擎
        :return: 实际有效的执行字段类型
        """

        require_type = require_type if require_type in cls.cast_type_mapping else "string"
        target_type_item = cls.cast_type_mapping.get(require_type)
        return str(target_type_item[engine]) if isinstance(target_type_item, dict) else str(target_type_item)

    def _get_runner_properties(self, engine, udf_list, ret_table_name_list, agg):
        """
        根据校验引擎构造参数

        :param engine: string 校验引擎
        :param udf_list: list udf列表
        :param ret_table_name_list: list 期望获得的表名列表
        :param agg: boolean 是否需要聚合
        :return: dict 校验参数
        """
        # Todo: 配置化
        param_dict = {
            "flink_sql": {
                "flink.env": "product",
                "flink.bk_biz_id": self.bk_biz_id,
                "flink.source_data": list(self.scopes.keys()),
                "flink.function_name": ", ".join(udf_list),
                "flink.result_table_name": ", ".join(ret_table_name_list),
                "flink.lateness_time": 1,
                "flink.system_fields": [],
                "flink.expired_time": 0,
                "flink.allowed_lateness": False,
                "flink.session_gap": 0,
                "flink.window_length": 0,
                "flink.count_freq": 1,
            },
            "spark_sql": {
                "spark.dataflow_udf_env": "product",
                "spark.bk_biz_id": self.bk_biz_id,
                "spark.input_result_table": [],
                "spark.input_result_table_with_schema": self.scopes,
                "spark.dataflow_udf_function_name": ", ".join(udf_list),
                "spark.result_table_name": ", ".join(ret_table_name_list),
                "spark.self_dependency_config": {},
            },
        }
        properties = param_dict.get(engine, {})
        if agg and engine == SchedulingEngineType.FLINK.value:
            properties["flink.window_type"] = "tumbling"
            properties["flink.window_length"] = 30
        return properties

    @classmethod
    def _pre_deal_validation_message(cls, message):
        """
        对校验api返回的错误信息进行处理后再输出

        :param message: 原始错误信息
        :return: 处理后的错误信息
        """
        match_obj = re.match(
            r'ParseException: Encountered "[^\w]*([\w]+)" at(.+)Was expecting one of',
            message,
            re.S,
        )
        if match_obj:
            message = '"{0}"是一个保留关键字,请在"字段加工逻辑"中用"`{0}`"替换"{0}"来进行转义(仅替换AS之前的部分)'.format(match_obj.group(1))
        return message

    def _check_sql_validation(self, require_schema, engine, agg):
        """
        提取sql中的udf等信息，组装参数，请求sql校验api

        :param require_schema: dict 期望得到的结果schema字典
        :param engine: string sql检查引擎
        :param agg: boolean 是否需要聚合
        :return: tuple (boolean, dict) check后得到的结果和schema
            CASE 成功: (True, {message: succeed, schema: table_name: [
                {field_name: YYY, field_type: int},
                {field_name: XXX, field_type: string}
            ]})
        """

        # if self.sql is None:
        self.gen_sql_expression(require_schema, engine)
        udf_list = self.get_func_list(self.sql, prefix="udf_")
        ret_table_name_list = list(require_schema.keys())
        check_sql = self.sql.encode("utf-8") if isinstance(self.sql, str) else self.sql
        resp = self.requests_clients[engine](
            {
                "sql": check_sql,
                "properties": self._get_runner_properties(engine, udf_list, ret_table_name_list, agg),
            },
            raise_exception=False,
        )
        if not resp.is_success() or not resp.data:
            self.err_trace = dict(
                message=self._pre_deal_validation_message(resp.message),
                code=resp.code,
                sql=check_sql,
            )
            return False, self.err_trace
        # 对返回数据进行格式化清洗
        ret_data = resp.data
        ret_fields_dict = dict()
        for schema_item in ret_data:
            for field_item in schema_item.get("fields", []):
                if schema_item["name"] not in ret_fields_dict:
                    ret_fields_dict[schema_item["name"]] = list()
                if "field_name" not in field_item and "field" in field_item:
                    field_item["field_name"] = field_item["field"]
                if "field_type" not in field_item and "type" in field_item:
                    field_item["field_type"] = field_item["type"]
                if field_item["field_name"] in InnerField.INNER_FIELD_LIST:
                    continue
                ret_fields_dict[schema_item["name"]].append(
                    dict(
                        field_name=field_item["field_name"],
                        field_type=field_item["field_type"],
                    )
                )
        return True, dict(message="check succeed", schema=ret_fields_dict)

    def get_err_trace(self):
        return self.err_trace

    @staticmethod
    def check_schema_consistency(ret_schema, require_schema):
        """
        校验每个返回的表及其字段是否符合预期

        :param ret_schema: 校验返回结果schema
        :param require_schema: 期望得到schema
        :return:  tuple (boolean, None/string)
            CASE 校验成功: (True, None)
            CASE 校验失败: (False, 错误Message)
        """

        matched = False
        match_info = None
        # 校验返回表名列表是否匹配
        if operator.ne(set(require_schema.keys()), set(ret_schema.keys())):
            return False, "require tables: {}, but return tables: {}".format(
                ", ".join(list(require_schema.keys())),
                ", ".join(list(ret_schema.keys())),
            )
        # 校验返回字段信息是否匹配
        for tb_name, fields in list(require_schema.items()):
            require_fields_dict = {str(field["field_name"]): field for field in fields}
            return_fields_dict = {str(ret_field["field_name"]): ret_field for ret_field in ret_schema[tb_name]}
            # 校验字段名列表
            if operator.ne(set(require_fields_dict.keys()), set(return_fields_dict.keys())):
                match_info = "require fields: {}, but return fields: {}".format(
                    ", ".join(list(require_fields_dict.keys())),
                    ", ".join(list(return_fields_dict.keys())),
                )
                break
            # 校验字段类型
            for field in fields:
                require_type = field["field_type"]
                if require_type == "numeric":
                    require_type_list = settings.VALID_NUMERIC_TYPE_LIST
                elif require_type == "float":
                    require_type_list = ["float", "double"]
                else:
                    require_type_list = [require_type]
                ret_field_type = return_fields_dict[field["field_name"]]["field_type"]
                if ret_field_type not in require_type_list:
                    match_info = '{} should be "{}" type, given "{}"'.format(
                        field["field_name"], require_type, ret_field_type
                    )
                    break
            if match_info is not None:
                break
        else:
            matched = True
        return matched, match_info

    def check_res_schema(
        self,
        require_schema,
        engines=SchedulingEngineType.FLINK.value,
        agg=True,
        extract_source_column=False,
    ):
        """
        验证sql是否能得到期望的schema

        :param require_schema: dict 期望得到的结果schema字典
        numeric 指数值类型，int，float，bigint等任一数值类型均可
        {
            "require_result_table_name": [
                {"field_name": "field_name_1", "field_type": "int"},
                {"field_name": "field_name_2", "field_type": "numeric"},
                {"field_name": "field_name_3", "field_type": "text"}
            ]
        }
        :param engines: list/string sql检查引擎
        :param agg: boolean 是否需要聚合
        :param extract_source_column: boolean 是否解析提取source_column
        :return: tuple (boolean, dict/string)
            CASE 校验成功: (True, 校验相关信息)
            CASE 校验失败: (False, 错误Message)
        """
        if not isinstance(engines, list):
            engines = [engines]
        matched = False
        ret_info = dict()
        for engine in engines:
            if engine == SchedulingEngineType.FLINK.value:
                cache_key = None
                try:
                    for scope_table_name, scope_table_info in list(self.scopes.items()):
                        cache_key = settings.SQL_VERIFY_CACHE_KEY.format(table=scope_table_name)
                        cache_info = {
                            "bk_biz_id": self.bk_biz_id,
                            "result_table_name": scope_table_name,
                            "fields": scope_table_info,
                        }
                        cache.set(cache_key, json.dumps(cache_info), 300)
                except Exception as e:
                    raise dm_pro_errors.SetCacheError(message_kv={"cache_key": cache_key, "error": e})
            check_flag, check_info = self._check_sql_validation(require_schema, engine, agg)
            # 校验sql失败
            if not check_flag:
                return False, check_info["message"]
            ret_schema = check_info["schema"]
            matched, match_info = self.check_schema_consistency(ret_schema, require_schema)
            if not matched:
                return False, match_info
        if extract_source_column:
            source_columns_mapping = get_sql_source_columns_mapping(self.sql, formatted=True)
            ret_info["source_columns"] = source_columns_mapping["fields"]
            ret_info["condition_columns"] = source_columns_mapping["conditions"]
        return matched, ret_info

    @property
    def show_sql(self):
        show_sql = self.sql
        for processed_table_name in list(self.scopes.keys()):
            show_sql = show_sql.replace(processed_table_name, self.source_table_mapping[processed_table_name])
        return show_sql


def validate_calculation_content(
    table_name,
    calculation_atom_name,
    field_type,
    calculation_formula,
    schema_list,
    filter_formula=None,
    aggregation_fields=None,
):
    """
    统计口径和指标SQL校验
    :param table_name: {String} 主表名称
    :param calculation_atom_name: {String} 统计口径名称
    :param field_type: {String} 数据类型
    :param calculation_formula: {String} 统计SQL
    :param schema_list: {List} 主表字段列表
    :param filter_formula: {String} 过滤条件
    :param aggregation_fields: {List} 聚合字段列表
    :return:
    """
    schema_dict = {table_name: schema_list}
    calculation_content_dict = {
        "scopes": schema_dict,
        "sql_parts": {
            "fields": {calculation_atom_name: calculation_formula},
            "conditions": filter_formula,
            "appends": {"group_by": aggregation_fields},
            "table_name": table_name,
        },
    }
    require_schema_dict = {table_name: [{"field_type": field_type, "field_name": calculation_atom_name}]}

    verifier = SQLVerifier(**calculation_content_dict)
    matched, match_info = verifier.check_res_schema(
        require_schema_dict,
        [SchedulingEngineType.FLINK.value, SchedulingEngineType.SPARK.value],
        extract_source_column=True,
    )
    if not matched:
        # matched为False时，match_info是错误message
        raise dm_pro_errors.SqlValidateError(
            message_kv={"error_info": "{} [ERROR_SQL: {}]".format(match_info, verifier.show_sql)}
        )
    # matched为True时, match_info是Dict, 例如{'source_columns': {'item_sales_amt': ['channel_id', 'price']}}
    return match_info["source_columns"], match_info["condition_columns"]


def strip_comment(orig_sql):
    """
    删除sql中的注释语句
    :param orig_sql: {String} 原始sql片段
    :return: {String} 删除注释后的sql片段
    """
    return sqlparse.format(orig_sql, strip_comments=True).strip()
