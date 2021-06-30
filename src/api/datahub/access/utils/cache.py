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

from datetime import datetime


class Cache(object):
    """
    内存缓存对象
    __content__ = {
        key: [time, data]
    }
    """

    def __init__(self, **kwargs):
        """
        expire: 缓存时间
        """
        self.__content__ = {}
        self.default_expire = 24 * 3600
        self.conf = dict()
        self.conf.update(kwargs)
        self.conf["expire"] = kwargs.get("expire") or self.default_expire

    def _cache(self, key, content):
        self.__content__[key] = [datetime.utcnow(), content]

    def _is_expired(self, key):
        now = datetime.utcnow()
        return (now - self.__content__[key][0]).seconds > self.conf["expire"]

    def keys(self):
        return self.__content__.keys()

    def clear(self):
        self.__content__ = {}

    def _get(self, key):
        if key in self.__content__ and not self._is_expired(key):
            return self.__content__[key][1]
        else:
            if key in self.__content__:
                del self.__content__[key]
            return None

    def _check_expired(self):
        expired = []
        for each in self.__content__:
            if self._is_expired(each):
                expired.append(each)
        for exp in expired:
            del self[exp]

    def __getitem__(self, item):
        return self._get(item)

    def __setitem__(self, key, value):
        self._cache(key, value)

    def __delitem__(self, key):
        del self.__content__[key]

    def __iter__(self):
        self._check_expired()
        return iter(self.__content__)

    def __repr__(self):
        self._check_expired()
        return str(self.__content__)


# 默认过期时间1天
DEFAULT_CONF = {"expire": 24 * 3600}

STATIC_CONF = {"expire": 24 * 3600 * 30}

SOURCE_INFO = Cache(**STATIC_CONF)

SCENARIO_INFO = Cache(**STATIC_CONF)

CATEGORY_INFO = Cache(**DEFAULT_CONF)

APP_INFO = Cache(**STATIC_CONF)

BIZ_INFO = Cache(**DEFAULT_CONF)
