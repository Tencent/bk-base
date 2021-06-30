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
import sqlparse
from collections import defaultdict

from datamanage.pro.datamodel.application.jobs.prototype import DataModelNode
from datamanage.pro.datamodel.handlers.verifier import SQLVerifier
from datamanage.pro.datamodel.utils import get_sql_fields_alias_mapping
from functools import reduce

achieved_dm_node = dict()
"""
已实现节点实例字典
"""


def data_model_register(cls):
    """
    注册数据模型类

    :param cls: 装饰类
    :return: boolean True
    """
    node_type = cls.node_type
    if node_type not in achieved_dm_node:
        achieved_dm_node[node_type] = cls
    return cls


class BaseDataModelNode(DataModelNode):
    """
    数据模型节点基类

    {
        node_type: 'dimension_table'
        node_conf: {
            main_table: XXX,
            fields: [
                {
                    field_name: XXX                                 字段名
                    field_type: [string/int/long/double]            字段目标类型
                    field_clean_content: {}                         字段构建加工逻辑
                    ins_field: {}                                   实例配置, 包含应用加工逻辑、输入rt、输入field_name
                    dmm_relation: {}                                模型关联配置,包含关联方式: 如left-join
                    ins_relation: {}                                实例关联配置,包含关联字段
                }
            ]
        }
    }
    """

    def __init__(self, node_conf):
        super(BaseDataModelNode, self).__init__()
        self.main_source_table = node_conf['main_table']
        self.main_raw_fields = list()
        self.main_derive_fields = list()
        self.dim_extend_fields = defaultdict(list)
        self.set_node_config(node_conf)

    def set_node_config(self, node_conf):
        """
        设置节点属性配置

        :param node_conf: dict 节点配置数据
        :return: boolean
        """

        for dmm_field in node_conf['fields']:
            field_name = dmm_field['field_name']
            source_input_table = dmm_field.get('ins_field', {}).get('input_result_table_id', '')
            source_input_field = dmm_field.get('ins_field', {}).get('input_field_name', '')
            outer_scope_table = (
                source_input_table
                if source_input_table and source_input_table != self.main_source_table
                else self.outer_built_table
            )
            outer_scope_name = field_name if outer_scope_table == self.outer_built_table else source_input_field
            self.type_conf[field_name] = dmm_field.get('field_type', 'string')
            # 输出层逻辑
            self.schema_conf[field_name] = dict(
                process={},
                input_table=outer_scope_table,  # 构建层临时表 or 外部关联维度表
                input_field=outer_scope_name,
                output_field=field_name,
            )
            # 构建层逻辑
            self.built_conf[field_name] = dict(
                process=dmm_field.get('field_clean_content', {}) or {},
                input_table=self.inner_app_table,  # 清洗层临时表
                input_field=field_name,
                output_field=field_name,
            )
            # 清洗层逻辑
            self.app_conf[field_name] = dict(
                process=dmm_field.get('ins_field', {}).get('application_clean_content', {}) or {},
                input_table=source_input_table,  # (主)输入原始表
                input_field=source_input_field,
                output_field=field_name,
            )
            # 关联层逻辑
            self.related_conf[field_name] = dict(
                method=dmm_field.get('dmm_relation', {}).get('related_method', 'left-join'),
                on_table=dmm_field.get('ins_relation', {}).get('input_result_table_id', ''),
                on_field=dmm_field.get('ins_relation', {}).get('input_field_name', ''),
            )

            # 主表原生字段
            if source_input_table == self.main_source_table:
                self.main_raw_fields.append(field_name)
            # 主表构建字段
            elif not source_input_table:
                self.main_derive_fields.append(field_name)
            # 维度表扩展字段
            else:
                self.dim_extend_fields[source_input_table].append(field_name)

    def get_main_table_fields(self, specific_type=None):
        """
        获取主表字段列表

        :param specific_type: 具体类别(raw-原始字段; derive-派生字段; extend-维度扩展字段)
        :return: list
        """
        if specific_type == 'raw':
            return self.main_raw_fields
        if specific_type == 'derive':
            return self.main_derive_fields
        if specific_type == 'extend':
            return reduce(lambda x, y: x + y, list(self.dim_extend_fields.values()), [])
        return (
            self.main_raw_fields
            + self.main_derive_fields
            + reduce(lambda x, y: x + y, list(self.dim_extend_fields.values()), [])
        )

    def get_dim_table_fields(self):
        """
        获取维度表扩展字段

        :return: list
        """
        fields_list = []
        for dim_table_name in list(self.dim_extend_fields.keys()):
            for field_name in self.dim_extend_fields[dim_table_name]:
                fields_list.append(field_name)
        return fields_list

    def gen_subquery_expression(self, fields, conf, inner_table, engine, join_expr='', with_cast=False):
        """
        构造子查询生成中间表

        :param fields: list 子查询字段列表
        :param conf: dict 子查询字段逻辑配置
        :param inner_table: string 子查询来源内部表
        :param engine: sql引擎
        :param join_expr: join表达式
        :param with_cast: 是否需要进行强制类型转换

        :return: string 中间表表达式
        """
        fields_expr_dict = {}
        for field_name in fields:
            field_expr = conf[field_name]['process'].get('clean_content')

            if not field_expr:
                if conf[field_name]['input_field']:
                    field_expr = '`{}`.`{}`'.format(conf[field_name]['input_table'], conf[field_name]['input_field'])

            if field_expr:
                fields_expr_dict[field_name] = field_expr

        fields_alias_expr_mapping = get_sql_fields_alias_mapping(fields_expr_dict)

        fields_expr_list = []
        for field_name, field_alias_item in list(fields_alias_expr_mapping.items()):
            field_alias = field_alias_item['alias']
            field_expr = (
                field_alias_item['expr'].decode('utf-8')
                if isinstance(field_alias_item['expr'], str)
                else field_alias_item['expr']
            )
            # 未设置别名且存在加工逻辑自动添加别名
            if not field_alias:
                field_alias = field_name

            if with_cast and conf[field_name]['input_field']:
                field_type = SQLVerifier.get_target_parse_type(self.type_conf[field_name], engine)
                field_expr = self.cast_field_expr_template.format(
                    field_expr=field_expr, type=field_type, alias=field_alias
                )
            elif field_expr != field_name:
                field_expr = '{} as `{}`'.format(field_expr, field_alias)
            fields_expr_list.append(field_expr)
        return self.sql_template.format(
            fields_expr=self.filed_separator.join(fields_expr_list),
            main_table=inner_table,
            join_expr=join_expr,
            conditions_expr='',
            group_by_expr='',
        )

    def gen_sql_expression(self, engine):
        """
        构建节点sql表达式

        :param engine: sql引擎
        :return: string sql表达式
        """

        # 构建数据模型输出外层查询
        fields_list = []
        for field_name, schema_item in list(self.schema_conf.items()):
            field_type = SQLVerifier.get_target_parse_type(self.type_conf[field_name], engine)
            outer_built_field = '`{}`.`{}`'.format(self.outer_built_table, field_name)
            fields_list.append(
                self.outer_field_expr_template.format(inner_expr=outer_built_field, type=field_type, alias=field_name)
            )

        # 如果存在维度表关联字段,构建SQL关联JOIN部分
        join_list = []
        if self.get_dim_table_fields():
            for field_name, related_item in list(
                {k: v for k, v in list(self.related_conf.items()) if v['on_table']}.items()
            ):
                # 映射数据库存储的关联方式到合法的 SQL JOIN 语句. (e.g., left-join -> LEFT JOIN)
                related_method = related_item['method'].replace('-', ' ').upper()
                main_expr = '`{table_name}`.`{field_name}`'.format(
                    table_name=self.main_source_table, field_name=self.app_conf.get(field_name, {}).get('input_field')
                )
                related_expr = '`{table_name}`.`{field_name}`'.format(
                    table_name=related_item['on_table'], field_name=related_item['on_field']
                )
                join_list.append(
                    self.join_expr_template.format(
                        related_method=related_method,
                        related_table=related_item['on_table'],
                        main_expr=main_expr,
                        related_expr=related_expr,
                    )
                )

        # 构建应用阶段清洗子查询
        app_tab_expr = '({inner_expr}) {tab_alias}'.format(
            tab_alias=self.inner_app_table,
            inner_expr=self.gen_subquery_expression(
                self.get_main_table_fields(),
                self.app_conf,
                '{} AS `{}`'.format(self.main_source_table, self.main_source_table),
                engine,
                join_expr=self.join_separator.join(join_list),
                with_cast=True,
            ),
        )
        # 构建构建阶段计算子查询
        main_tab_expr = '({inner_expr}) {tab_alias}'.format(
            tab_alias=self.outer_built_table,
            inner_expr=self.gen_subquery_expression(
                self.get_main_table_fields(), self.built_conf, app_tab_expr, engine
            ),
        )

        # 生成完整sql
        sql = self.sql_template.format(
            fields_expr=self.filed_separator.join(fields_list),
            main_table=main_tab_expr,
            join_expr='',
            conditions_expr='',
            group_by_expr='',
        )
        # return sqlparse.format(sql, strip_comments=True, strip_whitespace=True).strip()
        return sql


@data_model_register
class DimModelNode(BaseDataModelNode):
    """
    维度表模型实例

    {
        node_type: 'dimension_table'
        node_conf: {
            main_table: XXX,
            fields: [
                {
                    field_name: XXX                                 字段名
                    field_type: [string/int/long/double]            字段目标类型
                    field_clean_content: {}                         字段构建加工逻辑
                    ins_field: {}                                   实例配置, 包含应用加工逻辑、输入rt、输入field_name
                    dmm_relation: {}                                模型关联配置,包含关联方式: 如left-join
                    ins_relation: {}                                实例关联配置,包含关联字段
                }
            ]
        }
    }
    """

    node_type = 'dimension_table'
    """
    维度表模型类型
    """


@data_model_register
class FactModelNode(BaseDataModelNode):
    """
    事实表模型实例

    {
        node_type: 'fact_table'
        node_conf: {
            main_table: XXX,
            fields: [
                {
                    field_name: XXX                                 字段名
                    field_type: [string/int/long/double]            字段目标类型
                    field_clean_content: {}                         字段构建加工逻辑
                    ins_field: {}                                   实例配置, 包含应用加工逻辑、输入rt、输入field_name
                    dmm_relation: {}                                模型关联配置,包含关联方式: 如left-join
                    ins_relation: {}                                实例关联配置,包含关联字段
                }
            ]
        }
    }
    """

    node_type = 'fact_table'


@data_model_register
class IndicatorModelNode(DataModelNode):
    """
    指标模型实例

    {
        node_type: 'indicator'
        node_conf: {
            main_table: XXX
            calculation_atom: {}    统计口径, 包含口径的名称，和所规定的字段聚合方法以及输出类型
            ins_indicator: {}       指标实例, 包含指标聚合的字段以及过滤条件表达式
    }

    """

    node_type = 'indicator'

    def __init__(self, node_conf):
        super(IndicatorModelNode, self).__init__()
        self.main_source_table = node_conf['main_table']
        self.set_node_config(node_conf)

    def set_node_config(self, node_conf):
        """
        设置节点属性配置

        :param node_conf: dict 节点配置数据
        :return: boolean
        """

        self.indicator_conf['field_name'] = node_conf['calculation_atom']['calculation_atom_name']
        self.indicator_conf['field_type'] = node_conf['calculation_atom']['field_type']
        self.indicator_conf['field_expr'] = sqlparse.format(
            node_conf['calculation_atom']['calculation_formula'], strip_comments=True, strip_whitespace=True
        ).strip()
        self.indicator_conf['filter_expr'] = sqlparse.format(
            node_conf['ins_indicator']['filter_formula'], strip_comments=True, strip_whitespace=True
        ).strip()
        self.indicator_conf['group_expr'] = node_conf['ins_indicator']['aggregation_fields']

    def gen_sql_expression(self, engine):
        """
        构建节点sql表达式

        :param engine: sql引擎
        :return: string sql表达式
        """

        # 字段目标输出类型
        field_type = SQLVerifier.get_target_parse_type(self.indicator_conf['field_type'], engine)
        # 字段表达式
        field_expr = self.outer_field_expr_template.format(
            inner_expr=self.indicator_conf['field_expr'], type=field_type, alias=self.indicator_conf['field_name']
        )
        if self.indicator_conf['group_expr']:
            field_expr = '{ind_field}, {dim_fields}'.format(
                ind_field=field_expr, dim_fields=self.indicator_conf['group_expr']
            )
        # 条件筛选表达式
        conditions_expr = self.indicator_conf['filter_expr']
        if conditions_expr and not conditions_expr.strip().lower().startswith('where'):
            conditions_expr = 'WHERE {conditions}'.format(conditions=conditions_expr)
        # 聚合字段表达式
        group_by_expr = self.indicator_conf['group_expr']
        if group_by_expr:
            group_by_expr = 'GROUP BY {group_by_expr}'.format(group_by_expr=group_by_expr)
        sql = self.sql_template.format(
            fields_expr=field_expr,
            main_table=self.main_source_table,
            join_expr='',
            conditions_expr=conditions_expr,
            group_by_expr=group_by_expr,
        )
        # return sqlparse.format(sql, strip_comments=True, strip_whitespace=True).strip()
        return sql
