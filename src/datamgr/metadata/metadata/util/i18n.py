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

from gettext import translation

from speaklater import make_lazy_string

try:
    from metadata.runtime import rt_context
except ImportError:
    from metadata.util.context import default_context as rt_context


def selfish(item):
    return item


available_translations = {}


def get_translation(language):
    t = translation('messages', 'metadata_contents/translations', (language,))
    available_translations[language] = t
    available_translations[t.info()['language']] = t
    return t


def current_ugettext(item, current_context):
    lang = getattr(current_context, 'language', None)
    trans = available_translations.get(lang, None)
    if trans:
        return trans.ugettext(item)
    else:
        return item


zh_hans_translation = get_translation('zh_Hans')


def lazy_selfish(item):
    return make_lazy_string(current_ugettext, item, rt_context)
