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

from django.db import models

from datamanage.utils.api import DataqueryApi
from datamanage.pro.exceptions import SqlValidateError


def list_diff(orig_list, new_list):
    """
    原有列表 和 新列表的差异
    :param orig_list: {List} 原有列表
    :param new_list: {List} 新列表
    :return:
    add_list: {List} 待新增的列表
    delete_list: {List} 待删除的列表
    same_list: {List} 不变的列表/变更的列表
    """
    new_obj_map = {hash(obj): obj for obj in new_list}
    # 在new_list但不在orig_list中，即待新增的标签
    add_list = list(set(new_list).difference(orig_list))
    # 在orig_list但不在new_list中，即待删除的标签
    delete_list = list(set(orig_list).difference(new_list))
    same_list = [new_obj_map[hash(same_obj)] for same_obj in list(set(orig_list).intersection(set(new_list)))]
    return add_list, delete_list, same_list


def list_diff_without_same_content(orig_list, new_list, ignore_keys=None, models_cls=None):
    """
    原有列表 和 新列表的差异
    :param orig_list: {List} 原有列表
    :param new_list: {List} 新列表
    :param ignore_keys: {List} 忽略变更的字段
    :param models_cls: {Class} models cls
    :return:
    add_list: {List} 待新增的列表
    delete_list: {List} 待删除的列表
    same_list: {List} 不变的列表/变更的列表
    """
    add_list, delete_list, same_list = list_diff(orig_list, new_list)
    if models_cls is None or not issubclass(models_cls, models.Model):
        return add_list, delete_list, same_list

    update_list = []
    if ignore_keys is None:
        ignore_keys = []
    new_obj_map = {hash(obj): obj for obj in new_list}
    orig_obj_map = {hash(obj): obj for obj in orig_list}
    for obj in same_list:
        if diff_orig_and_new_models_object(models_cls, orig_obj_map[hash(obj)], new_obj_map[hash(obj)], ignore_keys):
            update_list.append(obj)

    return add_list, delete_list, update_list


def diff_orig_and_new_models_object(models_cls, orig_obj, new_obj, ignore_keys=None):
    """
    对比变更前后的两个models object
    :param models_cls:
    :param orig_obj:
    :param new_obj:
    :param ignore_keys:
    :return: has_update:{Boolean} 相同返回False,不同返回True
    """
    if ignore_keys is None:
        ignore_keys = []
    if not issubclass(models_cls, models.Model):
        return True
    attr_list = [f.name for f in models_cls._meta.fields if f.name not in ignore_keys]
    return any(getattr(orig_obj, attr, None) != getattr(new_obj, attr, None) for attr in attr_list)


def get_sql_fields_alias_mapping(expr_dict, strip_comments=True, strip_whitespace=True):
    """
    解析sql字段表达式列表，提取表达式中的别名

    :param expr_dict: dict sql表达式字典
    :param strip_comments: boolean 字段表达式是否去除注释
    :param strip_whitespace: boolean 字段表达式是否去除空格
    :return: dict sql表达式和别名的列表
    """
    alias_expr_mapping = dict()

    def _inner_alias_expr_parse(field_key, token_item):
        item = alias_expr_mapping[field_key]
        item['buffer'] += str(token_item)
        if str(token_item).upper() == str('AS') and str(token_item.ttype) == str('Token.Keyword'):
            item['as_flag'] = True
            return
        if item['as_flag']:
            if str(token.ttype) == str('Token.Text.Whitespace'):
                return
            item['alias'] = str(token_item).strip(str('`'))
            item['as_flag'] = False
            return
        # 保持输出为unicode
        if item['buffer']:
            item['expr'] += item['buffer']
            item['buffer'] = str('')

    for key, expr in list(expr_dict.items()):
        pure_expr = sqlparse.format(expr, strip_comments=strip_comments, strip_whitespace=strip_whitespace).strip()
        parse_tree = sqlparse.parse(pure_expr)
        if not parse_tree:
            continue
        statement = parse_tree[0]
        alias_expr_mapping[key] = dict(alias=str(''), expr=str(''), buffer=str(''), as_flag=False)
        if isinstance(statement, sqlparse.sql.Statement):
            for identifier in statement:
                if isinstance(identifier, sqlparse.sql.TokenList):
                    for token in identifier:
                        _inner_alias_expr_parse(key, token)
                else:
                    _inner_alias_expr_parse(key, identifier)
            now_item = alias_expr_mapping[key]
            now_item['expr'] = (
                now_item['expr'].strip().decode('utf-8')
                if isinstance(now_item['expr'], str)
                else now_item['expr'].strip()
            )
            now_item['alias'] = (
                now_item['alias'].strip().decode('utf-8')
                if isinstance(now_item['alias'], str)
                else now_item['alias'].strip()
            )

    return alias_expr_mapping


def extract_source_columns(expr, column_set, depth=0):
    """
    从parse_tree中提取source_column信息

    :param expr: dict parse_tree表达式
    :param column_set: set 字段集合
    :param depth: int 迭代深度
    :return: None
    """

    # 最大迭代次数
    depth_limit = 30
    if depth >= depth_limit:
        return
    next_depth = depth + 1
    if isinstance(expr, list):
        for sub_expr in expr:
            extract_source_columns(sub_expr, column_set, next_depth)
        return
    if isinstance(expr, dict):
        if 'expressionType' in expr and expr['expressionType'] == 'column':
            column_set.add(expr['columnName'].strip('`'))
            return
        for key, item in list(expr.items()):
            if isinstance(item, (dict, list)):
                extract_source_columns(item, column_set, next_depth)
        return
    return


def get_sql_source_columns_mapping(sql, formatted=False):
    """
    从sql语句中提取每个输出字段依赖的原始字段映射

    :param sql: string sql语句
    :param formatted: boolean sql是否已经是格式化后的标准sql
    :return: dict 输出字段依赖原始字段的映射字典 {column: [source_column_list]}
    """

    if formatted:
        pure_sql = sql
    else:
        pure_sql = sqlparse.format(sql, strip_comments=True, strip_whitespace=True).strip()
    response = DataqueryApi.sql_parse({'sql': pure_sql})
    if not response.is_success():
        if isinstance(pure_sql, str):
            pure_sql = pure_sql.decode('utf-8')
        err_message = response.message.decode('utf-8') if isinstance(response.message, str) else response.message
        err_message = '{} [ERROR_SQL: {}]'.format(err_message, pure_sql)
        raise SqlValidateError(message_kv={'error_info': err_message})
    parse_tree = response.data
    # 获取字段列表的source_columns
    source_column_mapping = dict()
    select_item_list = parse_tree.get('selectBody', {}).get('selectItems', [])
    for select_item in select_item_list:
        if select_item.get('type', '') == 'select_expr_item' and 'expression' in select_item:
            expression = select_item.get('expression')
            alias_name = select_item.get('alias', {}).get('name', '').strip('`')
            item_name = expression['columnName'].strip('`') if expression['expressionType'] == 'column' else ''
            column_name = alias_name if alias_name else item_name
            if column_name:
                source_columns_set = set()
                extract_source_columns(expression, source_columns_set)
                source_column_mapping[column_name] = list(source_columns_set)
    # 获取条件表达式的source_columns
    condition_item = parse_tree.get('selectBody', {}).get('where', {})
    condition_columns_set = set()
    extract_source_columns(condition_item, condition_columns_set)
    return dict(fields=source_column_mapping, conditions=list(condition_columns_set))


def replace_sql_token(token_expr, rep_expr, sql_pattern):
    """
    在SQL中进行token表达式的替换

    :param token_expr: string 查找的目标token表达式
    :param rep_expr: string 用于替换的表达式
    :param sql_pattern: string 基准SQL片段
    :return: string (unicode)替换后的SQL片段表达式
    """

    parse_tree = sqlparse.parse(sql_pattern)
    token_list = list()
    for statement in parse_tree:
        if not isinstance(statement, sqlparse.sql.Statement):
            continue
        token_list = list()
        for expr in statement:
            generator = expr.flatten()
            for token in generator:
                if token.value in [token_expr, '`{}`'.format(token_expr)]:
                    token.value = rep_expr
                token_list.append(token)
        # 只取一个有效sql语句
        if token_list:
            break
    return str(sqlparse.sql.Statement(token_list)) if token_list else str(sql_pattern)
