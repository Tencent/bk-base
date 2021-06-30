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

import os

early_patched = False


def early_patch():
    global early_patched
    if not early_patched:
        from gevent.monkey import patch_all

        patch_all()

        import logging

        logging.getLogger('lazy_import').setLevel(logging.INFO)

        from metadata.util.common import jsonext_patch

        jsonext_patch()

        from tblib import pickling_support

        pickling_support.install()

        import click

        click.disable_unicode_literals_warning = True

        from datetime import datetime

        import arrow
        from cattr import register_structure_hook, register_unstructure_hook

        register_structure_hook(datetime, lambda x, _: arrow.get(x).datetime if isinstance(x, str) else x)
        register_unstructure_hook(datetime, lambda x: x)

        from metadata.util.context import get_runtime_context

        rt_context = get_runtime_context('default')
        rt_context.g.env = os.environ.get('RUN_MODE', 'develop')

    early_patched = True
