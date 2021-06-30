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
import re
import MySQLdb
import sys
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword

from common.log import logger

from .dataflowapi import DataFlowAPI
from datamanage.pro.dstantools.models import Test
from .metaapi import MetaAPi
import imp


DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%i:%s'
NONE_STORAGE = 'none_storage'


def get_tag_target_obj(tag_targets_list, result_table_id, tag_code, bk_biz_id, project_id):
    is_exists = False
    for tagged_obj in tag_targets_list:
        if tagged_obj['target_id'] == result_table_id:
            is_exists = True
            tagged_obj['tags'].append({'tag_code': tag_code})
            break
    if not is_exists:
        tag_targets_list.append(
            {
                "target_id": result_table_id,
                "target_type": "result_table",
                "bk_biz_id": bk_biz_id,
                "project_id": project_id,
                "tags": [{"tag_code": tag_code}],
            }
        )


def escape_string(s):
    imp.reload(sys)
    sys.setdefaultencoding('utf-8')
    return MySQLdb.escape_string(s)


def get_fulltext_info(bk_biz_id, keyword):
    sql = """select a.result_table_name, a.bk_biz_id, date_format(a.created_at,'{0}') created_at, a.sensitivity,
     a.result_table_name_alias,a.updated_by, a.created_by,a.result_table_id,a.count_freq,a.description,
     date_format(a.updated_at,'{0}') updated_at,a.generate_type,a.result_table_type,a.processing_type,a.project_id,
     b.project_name from result_table a left join project_info b on a.project_id=b.project_id where 1=1""".format(
        DEFAULT_DATE_FORMAT
    )
    where_cond = ' and b.active=1 and a.bk_biz_id=' + str(bk_biz_id)
    if keyword:
        where_cond += (
            " and (a.result_table_id like '%{0}%' or a.result_table_name like '%{0}%' "
            " or a.result_table_name_alias like '%{0}%' or a.description like '%{0}%'"
            " or b.project_name like '%{0}%' or b.description like '%{0}%'"
            " or a.created_by like '%{0}%')"
        )
        where_cond = where_cond.format(escape_string(keyword))
    sql += where_cond
    query_result = MetaAPi.complex_search({'statement': sql, 'backend_type': 'mysql'}, raw=True)
    result = query_result['result']
    message = query_result['message']
    if not result:
        raise Exception(message)
    data = query_result['data']
    return data


def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):  # 用来判断浮点数是否相等
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def value_range_valid(field_type, ope_value, val, valid_result, key):
    if field_type not in ('string', 'text'):
        act_val = ope_value[1 : len(ope_value) - 1]
        mm_val = act_val.split('~')  # min or max value,还缺少对无穷符号的校验
        pre_char = ope_value[0]
        suf_char = ope_value[len(ope_value) - 1]
        min_str = mm_val[0].strip()
        max_str = mm_val[1].strip()
        parse_func = get_parse_func(field_type)
        try:
            digit_min = parse_func(min_str)
            digit_max = parse_func(max_str)
            digit_val = parse_func(val)
        except Exception as e:
            logger.error('不符合值约束[值范围]:{} for:{}'.format(ope_value, str(e)))
            valid_result[key] = '不符合值约束[值范围]:' + ope_value
            return 'break'
        if pre_char == '[':  # 包含
            if suf_char == ']':
                if parse_func == int:
                    if not (digit_min <= digit_val <= digit_max):
                        valid_result[key] = '不符合值约束[值范围]:' + ope_value
                else:  # float
                    if isclose(digit_val, digit_min) or isclose(digit_val, digit_val):
                        return 'continue'
                    if not (digit_min < digit_val < digit_max):
                        valid_result[key] = '不符合值约束[值范围]:' + ope_value
            elif suf_char == ')':
                if parse_func == int:
                    if not (digit_min <= digit_val < digit_max):
                        valid_result[key] = '不符合值约束[值范围]:' + ope_value
                else:  # float
                    if isclose(digit_val, digit_min):
                        return 'continue'
                    if not (digit_min < digit_val < digit_max):
                        valid_result[key] = '不符合值约束[值范围]:' + ope_value
        elif pre_char == '(':  # 不包含
            if suf_char == ']':
                if parse_func == int:
                    if not (digit_min < digit_val <= digit_max):
                        valid_result[key] = '不符合值约束[值范围]:' + ope_value
                else:  # float
                    if isclose(digit_val, digit_max):
                        return 'continue'
                    if not (digit_min < digit_val < digit_max):
                        valid_result[key] = '不符合值约束[值范围]:' + ope_value
            elif suf_char == ')':  # float可以直接比较>和<
                valid_result[key] = '不符合值约束[值范围]:' + ope_value
    else:  # string or text
        pass


def value_enum_valid(field_type, ope_value, val, valid_result, key):  # noqa
    """"""
    if str(val) not in ope_value.split(','):
        valid_result[key] = '不符合值约束[枚举]:' + ope_value


def regex_valid(field_type, ope_value, val, valid_result, key):  # noqa
    """"""
    if not re.search(ope_value, val):
        valid_result[key] = '不符合值约束[正则表达式]:' + ope_value


def start_with_valid(field_type, ope_value, val, valid_result, key):  # noqa
    """"""
    if not val.startswith(ope_value):
        valid_result[key] = '不符合值约束[开头是]:' + ope_value


def end_with_valid(field_type, ope_value, val, valid_result, key):  # noqa
    """"""
    if not val.startswith(ope_value):
        valid_result[key] = '不符合值约束[结尾是]:' + ope_value


def not_include_valid(field_type, ope_value, val, valid_result, key):  # noqa
    """"""
    if str(val) in ope_value.split(','):
        valid_result[key] = '不符合值约束[不包括]:' + ope_value


ope_type_valid_func_map = {
    'value_range': value_range_valid,
    'value_enum': value_enum_valid,
    'regex': regex_valid,
    'start_with': start_with_valid,
    'end_with': end_with_valid,
    'not_include': not_include_valid,
}


def valid_data_veracity(data_list, standard_version_id):
    if data_list:  # 进行标准化数据校验
        standard_detail_dict = MetaAPi.get_standard_detail(
            {'dm_standard_version_config_id': standard_version_id}, raw=True
        )  # 取值明细数据标准字段信息
        field_info_dict = {}
        if standard_detail_dict['result']:
            data = standard_detail_dict['data']
            if data and data['standard_content']:
                standard_content = data['standard_content']
                for content in standard_content:
                    standard_info = content['standard_info']
                    standard_fields = content['standard_fields']
                    standard_content_type = standard_info['standard_content_type']
                    if standard_content_type == 'detaildata':
                        for field_dict in standard_fields:
                            field_info_dict[field_dict['field_name']] = field_dict
                        break

        for row_dict in data_list:
            valid_result = {}  # 校验结果
            for key, val in list(row_dict.items()):
                field_info = field_info_dict.get(key.lower())
                if field_info and field_info.get('constraint'):
                    constraint = field_info.get('constraint')
                    field_type = field_info['field_type']  # 数据类型
                    rule_list = constraint['rule']
                    for rule in rule_list:
                        operator = rule['operator']
                        if operator == 'and' and key in valid_result:
                            break
                        if operator == 'or' and key in valid_result:
                            del valid_result[key]
                        ope_type = rule['type']
                        ope_value = rule['value_use']
                        ignore_case = rule.get('ignore_case')
                        if ignore_case:  # 忽略大小写
                            ope_value = ope_value.lower()
                            val = val.lower()
                        return_type = ope_type_valid_func_map[ope_type](field_type, ope_value, val, valid_result, key)
                        if return_type == 'break':
                            break
                        if return_type == 'continue':
                            continue
                else:  # 字段不属于标准定义里的
                    valid_result[key] = '不属于标准定义里的字段'

            row_dict['_valid_result_'] = valid_result


def get_parse_func(field_type):
    if field_type == 'int':
        return int
    if field_type == 'long':
        return int
    return float


def window_type_handler(params_config, node_config):  # 处理窗口类型参数
    window_type = params_config['window_type']
    node_config['window_type'] = window_type
    if window_type != 'none':
        count_freq = params_config['count_freq']
        waiting_time = params_config['waiting_time']
        node_config['count_freq'] = count_freq
        node_config['waiting_time'] = waiting_time
        if window_type == 'slide' or window_type == 'accumulate':
            window_time = params_config['window_time']
            node_config['window_time'] = window_time


def parse_interface_result(res_dict):
    result = res_dict['result']
    message = res_dict['message']
    data = res_dict['data']
    return result, message, data


def valid_standard_sql(sql, standard_version_id):
    standard_content_config = Test.objects.get(id=standard_version_id)
    standard_sql = standard_content_config.name  # just for test
    if standard_sql:
        st_alias_list, st_dimension_list, _ = get_sql_info(standard_sql)
        alias_list, dimension_list, _ = get_sql_info(sql)
        if len(st_alias_list) != len(alias_list):
            raise Exception('字段数不相等')
        if len(st_dimension_list) != len(dimension_list):
            raise Exception('维度数不相等')
        for alias in alias_list:
            if alias not in st_alias_list:
                raise '['

        for dimension in dimension_list:
            if dimension not in st_dimension_list:
                raise Exception('[' + dimension + ']不是标准维度字段')
    else:
        raise Exception('找不到标准定义')


def get_sql_info(sql):  # 校验sql
    common_parse_result = DataFlowAPI.parse_sql(params={'sql': sql}, raw=True)
    result = common_parse_result['result']
    message = common_parse_result['message']
    data = common_parse_result['data']
    if not result:  # result==false
        raise Exception('解析sql出错,message=', message, ',sql=', sql)

    selectBody = data.get('selectBody')
    if not selectBody:
        raise Exception('不是select查询语句,sql=', sql)

    selectItems = selectBody['selectItems']
    groupByColumnReferences = selectBody.get('groupByColumnReferences')

    column_alias_name_list = []
    group_by_dimension_list = []

    for item in selectItems:
        # expression
        expression = item['expression']
        # alias
        alias = item.get('alias')
        alias_name = None
        if alias:  # 首先添加了as 后的别名
            alias_name = alias['name']
            column_alias_name_list.append(alias_name)

        expressionType = expression['expressionType']
        if expressionType != 'column':
            if alias is None:
                print('lack as key.')
        else:
            columnName = expression.get('columnName')
            if alias is None:
                column_alias_name_list.append(columnName)

    column_as_real_name_list = get_as_real_column_list(sql)
    if len(column_alias_name_list) != len(column_as_real_name_list):
        print('解析出错,请检查列名是否包含了sql的保留关键字!')
        raise Exception('解析出错,请检查列名是否包含了sql的保留关键字!')

    res_column_alias_dict_list = []
    res_column_alias_list = []

    for index in range(len(column_alias_name_list)):
        res_column_alias_dict_list.append(
            {'real_name': column_as_real_name_list[index], 'alias_name': column_alias_name_list[index]}
        )
        res_column_alias_list.append(column_alias_name_list[index])

    if groupByColumnReferences:  # group by 后面的字段
        for group_by_obj in groupByColumnReferences:
            group_by_dimension_list.append(group_by_obj['columnName'])

    return res_column_alias_list, group_by_dimension_list, res_column_alias_dict_list


def get_as_real_column(column_val):
    if ' as ' in column_val:
        real_name = column_val[: column_val.rindex(' as ')]
        return real_name
    elif ')as ' in column_val:
        real_name = column_val[: column_val.rindex(')as ') + 1]
        return real_name
    elif ' ' in column_val:
        return column_val[0 : column_val.rindex(' ')].strip()
    return column_val


def get_as_real_column_list(sql):
    res = sqlparse.parse(sql)[0]
    tokens = res.tokens
    column_as_real_name_list = []
    for token in tokens:
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                column_str = identifier.value
                as_pre = get_as_real_column(column_str)
                column_as_real_name_list.append(as_pre)

        elif isinstance(token, Identifier):
            column_str = token.value
            as_pre = get_as_real_column(column_str)
            column_as_real_name_list.append(as_pre)

        elif token.ttype is Keyword and token.value.upper() == 'FROM':
            break
    return column_as_real_name_list
