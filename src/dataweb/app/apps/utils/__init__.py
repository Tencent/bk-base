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
import unicodedata
from html.parser import HTMLParser

from django.conf import settings


class APIModel(object):
    KEYS = []

    @classmethod
    def init_by_data(cls, data):
        kvs = {_key: data[_key] for _key in cls.KEYS}
        o = cls(**kvs)
        o._data = data
        return o

    def __init__(self, *args, **kwargs):
        self._data = None

    def _get_data(self):
        """
        获取基本数据方法，用于给子类重载
        """
        return None

    @property
    def data(self):
        if self._data is None:
            self._data = self._get_data()

        return self._data


def build_auth_args(request):
    """
    组装认证信息
    """
    # auth_args 用于ESB身份校验
    auth_args = {}
    if request is None:
        return auth_args

    for k, v in list(settings.OAUTH_COOKIES_PARAMS.items()):
        if v in request.COOKIES:
            auth_args.update({k: request.COOKIES[v]})

    return auth_args


def html_decode(key):
    """
    @summary:符号转义
    """
    h = HTMLParser()
    cleaned_text = unicodedata.normalize("NFKD", h.unescape(key).strip())
    return cleaned_text


def get_display_from_choices(key, choices):
    """
    choices中获取display
    @apiParam {List} flow_ids
    @apiParamExample {*args} 参数样例:
        "key": "project",
        "choices": (
            ("user", _(u"用户")),
            ("app", _(u"APP")),
            ("project", _(u"项目")),
    @apiSuccessExample {String}
        项目
    """
    for choice in choices:
        if key == choice[0]:
            return choice[1]


def cmp_to_key(mycmp):
    "Convert a cmp= function into a key= function"

    class K(object):
        def __init__(self, obj, *args):
            self.obj = obj

        def __lt__(self, other):
            return mycmp(self.obj, other.obj) < 0

        def __gt__(self, other):
            return mycmp(self.obj, other.obj) > 0

        def __eq__(self, other):
            return mycmp(self.obj, other.obj) == 0

        def __le__(self, other):
            return mycmp(self.obj, other.obj) <= 0

        def __ge__(self, other):
            return mycmp(self.obj, other.obj) >= 0

        def __ne__(self, other):
            return mycmp(self.obj, other.obj) != 0

    return K
