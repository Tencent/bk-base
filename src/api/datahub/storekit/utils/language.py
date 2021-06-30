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
from copy import deepcopy

from django.utils.functional import Promise
from django.utils.translation import ugettext as _
from django.utils.translation import ungettext_lazy
from django.utils.translation.trans_real import translation


class Bilingual(object):
    """
    双语模块
    """

    def __init__(self, zh, en=None):
        """
        @param {String} zh 传入的中文，未翻译内容
        @param {String} [en] 传入的英文，已翻译内容
        """
        if en is None:
            en = self.do_translate(zh)
        self.zh = zh.replace("\n", " ")
        self.en = en.replace("\n", " ")

    def __unicode__(self):
        return _(self.zh)

    def __str__(self):
        return _(self.zh)

    def format(self, *args, **kwargs):
        """
        对Bilingual进行格式化，kwargs 参数支持双语对象
        """
        kwargs_zh = deepcopy(kwargs)
        kwargs_en = kwargs
        for _k, _v in list(kwargs.items()):
            # 兼容懒翻译的情况
            if isinstance(_v, Promise):
                try:
                    # 获取懒翻译前的中文
                    kwargs_zh.update({_k: _v._proxy____args[0]})
                except (AttributeError, KeyError):
                    pass
            # 兼容格式化占位内容也为双语的情况
            elif isinstance(_v, Bilingual):
                kwargs_zh.update({_k: _v.zh})
                kwargs_en.update({_k: _v.en})
        self.zh = self.zh.format(*args, **kwargs_zh)
        self.en = self.en.format(*args, **kwargs_en)
        return self

    @staticmethod
    def do_translate(msg, language="en"):
        """
        把中文转成英文（默认）
        @param {String} msg 中文字符串
        @param {String} language 语言
        """
        eol_message = msg.replace(str("\r\n"), str("\n")).replace(str("\r"), str("\n"))
        translation_object = translation(language)
        return translation_object.ugettext(eol_message)

    @classmethod
    def from_lazy(cls, lazy_obj):
        """
        将一个懒翻译对象转化为双语对象
        """
        # 获取懒翻译前的中文
        return cls(lazy_obj._proxy____args[0])


EXTRA_TRANSLATION = [
    ungettext_lazy(
        "Ensure this value has at most %(limit_value)d character (it has %(show_value)d).",
        "Ensure this value has at most %(limit_value)d characters (it has %(show_value)d).",
        "limit_value",
    ),
    ungettext_lazy(
        "Ensure this value has at least %(limit_value)d character (it has %(show_value)d).",
        "Ensure this value has at least %(limit_value)d characters (it has %(show_value)d).",
        "limit_value",
    ),
]
