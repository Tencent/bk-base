# coding=utf-8
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
import pymysql
from django.db import connection
from django.forms.models import model_to_dict
from django.utils.translation import ugettext as _
from django.db.models import Count

from datamanage.lite.tag.models import TagAttributesConfig, TagTargetConfig


def escape_string(s):
    return pymysql.escape_string(s)


def query_paging_results(query_sql, page, page_size):
    result_dict = {}
    count_sql = 'select count(*) counts from (' + query_sql + ') tmp'
    counts_list = query_direct_sql_to_map_list(connection, count_sql)
    for count_dict in counts_list:
        result_dict['count'] = count_dict['counts']
    offset = (page - 1) * page_size
    page_sql = query_sql + ' limit ' + str(offset) + ',' + str(page_size)
    results = query_direct_sql_to_map_list(connection, page_sql)
    result_dict['results'] = results
    return result_dict


# 直接根据sql查询返回Map对象的List列表
def query_direct_sql_to_map_list(conn, sql):
    if not isinstance(sql, str):
        raise Exception(_("sql参数传递错误:需要传递字符串参数"))

    cursor = conn.cursor()
    result = []
    try:
        cursor.execute(sql)
        raw_data = cursor.fetchall()
        col_names = [desc[0].lower() for desc in cursor.description]  # key统一小写

        for row in raw_data:
            obj_dict = {}
            for index, value in enumerate(row):
                obj_dict[col_names[index]] = value
            result.append(obj_dict)

    finally:
        cursor.close()

    return result


def get_tag_details_query_sql():
    return (
        "select a.id,a.code,a.alias,a.parent_id,a.tag_type,a.sync,a.kpath,a.icon,a.created_by,"
        "date_format(a.created_at,'%Y-%m-%d %H:%i:%s') created_at,a.updated_by,"
        "date_format(a.updated_at,'%Y-%m-%d %H:%i:%s') updated_at,a.description,b.code parent_code,"
        "b.alias parent_alias from tag_config a left join tag_config b on a.parent_id=b.id where 1=1 and a.active=1"
    )


def add_attribute_list(results):
    tag_code_list = []
    if results:
        tag_attr_map_dict = {}
        for tag_obj in results:
            tag_code = tag_obj['code']
            tag_attr_map_dict[tag_code] = []
            tag_code_list.append(tag_code)
        # print (tag_code_list)
        tag_attr_list = TagAttributesConfig.objects.filter(tag_code__in=tag_code_list, active=1).order_by(
            'tag_code', 'attr_index'
        )
        if tag_attr_list:
            for tag_attr_obj in tag_attr_list:
                tmp_tag_code = tag_attr_obj.tag_code
                tag_attr_dict = model_to_dict(tag_attr_obj)
                tag_attr_dict['created_at'] = (
                    tag_attr_obj.created_at.strftime("%Y-%m-%d %H:%M:%S") if tag_attr_obj.created_at else None
                )
                tag_attr_dict['updated_at'] = (
                    tag_attr_obj.updated_at.strftime("%Y-%m-%d %H:%M:%S") if tag_attr_obj.updated_at else None
                )
                tag_attr_map_dict[tmp_tag_code].append(tag_attr_dict)

        for tag_obj2 in results:
            tag_obj2['attribute_list'] = tag_attr_map_dict[tag_obj2['code']]

    return tag_code_list


def add_tag_target_count(results, tag_code_list):
    if tag_code_list:
        tag_target_counts = (
            TagTargetConfig.objects.filter(tag_code__in=tag_code_list, active=1)
            .values_list('tag_code')
            .annotate(target_count=Count('id'))
        )
        # print (tag_target_counts.query) //打印出执行的sql
        tt_count_dict = {}
        if tag_target_counts:
            for count_obj in tag_target_counts:
                tt_count_dict[count_obj[0]] = count_obj[1]

        # print (tt_count_dict)
        for tag_obj in results:
            tag_code = tag_obj['code']
            tag_obj['tag_target_count'] = tt_count_dict.get(tag_code, 0)
