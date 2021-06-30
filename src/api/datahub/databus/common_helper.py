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
import re
from urllib import parse

from common.local import get_request_username
from common.log import logger
from datahub.databus.settings import log_item_format

from datahub.databus import model_manager


def find_value_by_key(target, key, result_list):
    """
    :param target: 目标dict 或者 list
    :param key: key的过滤条件值
    :param result_list: 结果集
    :return: 返回符合当前key值的结果集
    """
    if isinstance(target, dict):
        for k, v in target.items():
            if key == k:
                result_list.append(target)
            else:
                find_value_by_key(v, key, result_list)

    elif isinstance(target, (list, tuple)):
        for each in target:
            find_value_by_key(each, key, result_list)

    return result_list


def find_key_by_value(target, value, result_list):
    """
    :param target: 目标dict 或者 list
    :param value: value的过滤条件值
    :param result_list: 结果集
    :return: 返回符合当前value值的结果集
    """
    if isinstance(target, dict):
        for k, v in target.items():
            if value == v:
                result_list.append(target)
            else:
                find_key_by_value(v, value, result_list)

    elif isinstance(target, (list, tuple)):
        for each in target:
            find_key_by_value(each, value, result_list)
    return result_list


def is_json(str):
    try:
        json_str = json.loads(str)
    except ValueError:
        return False, None

    return True, json_str


def db_type2trt_type(db_type):
    types = {
        "int": "int",
        "tinyint": "int",
        "Tinyint": "int",
        "smallint": "int",
        "uint8": "int",
        "int8": "int",
        "bigint": "long",
        "uint": "long",
        "utinyint": "long",
        "long": "long",
        "uint64": "long",
        "int64": "long",
        "Int64": "long",
        "uint32": "long",
        "uint16": "long",
        "int32": "long",
        "int16": "long",
        "usmallint": "long",
        "ubigint": "long",
        "biguint": "long",
        "char": "string",
        "uchar": "string",
        "varchar": "string",
        "datetime": "string",
        "date": "string",
        "DateTime": "string",
        "string": "string",
        "String": "string",
        "float": "double",
        "double": "double",
    }

    if types.get(db_type):
        return types.get(db_type)

    if re.findall("enum", db_type, flags=re.IGNORECASE):
        return "int"

    if re.findall("int", db_type, flags=re.IGNORECASE):
        return "long"

    if re.findall("double", db_type, flags=re.IGNORECASE):
        return "double"

    if re.findall("float", db_type, flags=re.IGNORECASE):
        return "double"

    if re.findall("decimal", db_type, flags=re.IGNORECASE):
        return "double"

    if re.findall("char", db_type, flags=re.IGNORECASE):
        return "string"

    if re.findall("string", db_type, flags=re.IGNORECASE):
        return "string"

    if re.findall("time", db_type, flags=re.IGNORECASE):
        return "string"

    if re.findall("text", db_type, flags=re.IGNORECASE):
        return "text"

    return db_type


def get_expire_days(storage_expire_days, default=3):
    """
    根据字符串计算过期天数
    :param storage_expire_days: 过期天数字符串，格式如： 123(d|w|m|y)
    :return: 过期天数
    """
    reg = re.compile(r"^(?P<value>[(\-|\+)?\d]*)(?P<unit>[dwmy]?)$")
    reg_match = reg.match(storage_expire_days)
    if reg_match is None:
        return default
    rst = reg_match.groupdict()
    if int(rst["value"]) < 0:
        return -1
    elif rst["unit"] == "" or rst["unit"] == "d":
        return int(rst["value"])
    elif rst["unit"] == "w":
        return int(rst["value"]) * 7
    elif rst["unit"] == "m":
        return int(rst["value"]) * 30
    elif rst["unit"] == "y":
        return int(rst["value"]) * 365
    else:
        return default


def check_keys_equal(dict1, dict2, keys):
    """
    对比两个字典中指定的keys，
    :param dict1: {dict} 对比字典1
    :param dict2: {dict} 对比字典2
    :param keys:  {list} 需对比key列表
    :return: {boolean} 如果keys对于的values都相同，则返回True，否则False
    """
    try:
        for key in keys:
            if str(dict1[key]) != str(dict2[key]):
                logger.warning("task conf is different! {} != {}".format(dict1[key], dict2[key]))
                return False
        return True
    except Exception:
        return False


def add_task_log(operation_type, storage_type, rt_id, args, content, error_msg="", bk_username=""):
    _content = json.dumps(
        {
            "content": content,
            "error_msg": error_msg,
        }
    )
    _bk_username = bk_username if bk_username else get_request_username()

    try:
        model_manager.add_databus_oplog(
            operation_type,
            log_item_format % (storage_type, rt_id),
            "",
            json.dumps(args),
            _content,
            _bk_username,
        )
    except Exception as e:
        logger.warning(u"add operation log error:%s" % str(e))


def delete_task_log(storage_type, rt_id):
    try:
        model_manager.delete_databus_oplog_by_item(log_item_format % (storage_type, rt_id))
    except Exception as e:
        logger.warning(u"delete operation log error:%s" % str(e))


def split_url(url):
    """
    提取URL中的start和end字段
    :param url: 示例http://xxx.com/?start=<start>&end=<end>
    :return: start,end,URL
    """
    start_field = ""
    end_field = ""
    params = ""
    tuple_url = parse.urlsplit(url)
    query = tuple_url.query
    if query:
        query_params = parse.parse_qs(query)
        for k, v in query_params.items():
            if re.search(r"<\s*start\s*>", str(v)):
                start_field = k
            elif re.search(r"<\s*end\s*>", str(v)):
                end_field = k
            else:
                for value in v:
                    params = params + k + "=" + value + "&"

    # <scheme>://<netloc>/<path>?<query>#<fragment>
    if params:
        url_format = "%s://%s%s?%s"
        url = url_format % (
            tuple_url.scheme,
            tuple_url.netloc,
            tuple_url.path,
            params[:-1],
        )
    else:
        url_format = "%s://%s%s"
        url = url_format % (tuple_url.scheme, tuple_url.netloc, tuple_url.path)

    return start_field, end_field, url
