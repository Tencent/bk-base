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

import re
import socket
import hashlib
import itertools

from common.log import logger


def is_ip(ci_name):
    try:
        socket.inet_aton(ci_name)
        return True
    except Exception as e:
        logger.info('{} is not ip for:{}'.format(ci_name, str(e)))
        return False


def get_first(objs, default=""):
    """get the first element in a list or get blank"""
    if len(objs) > 0:
        return objs[0]
    return default


def get_list(obj):
    return obj if isinstance(obj, list) else [obj]


def split_list(raw_string):
    if isinstance(raw_string, (list, set)):
        return raw_string
    re_obj = re.compile(r'\s*[;,]\s*')
    return [x for x in re_obj.split(raw_string) if x]


def expand_list(obj_list):
    return list(itertools.chain.from_iterable(obj_list))


def remove_blank(objs):
    if isinstance(objs, (list, set)):
        return [str(obj) for obj in objs if obj]
    return objs


def remove_tag(text):
    """去除 html 标签"""
    tag_re = re.compile(r'<[^>]+>')
    return tag_re.sub('', text)


def _count_md5(content):
    if content is None:
        return None
    m2 = hashlib.md5()
    if isinstance(content, str):
        m2.update(content.encode("utf8"))
    else:
        m2.update(content)
    return m2.hexdigest()


def get_md5(content):
    if isinstance(content, list):
        return [_count_md5(c) for c in content]
    else:
        return _count_md5(content)
