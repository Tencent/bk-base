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

from enum import Enum
from functools import wraps

import mock
from django.utils.decorators import available_attrs

SHARED_DIR = "dataflow.shared"


class SharedHelperTypes(Enum):
    access = SHARED_DIR + ".%s" % "access.access_helper.AccessHelper"
    batch = SHARED_DIR + ".%s" % "batch.batch_helper.BatchHelper"
    databus = SHARED_DIR + ".%s" % "databus.databus_helper.DatabusHelper"
    datamanage = SHARED_DIR + ".%s" % "datamanage.datamanage_helper.DatamanageHelper"
    jobnavi = SHARED_DIR + ".%s" % "jobnavi.jobnavi_helper.JobnaviHelper"
    processing = SHARED_DIR + ".%s" % "meta.processing.processing_helper.ProcessingHelper"
    project = SHARED_DIR + ".%s" % "meta.project.project_helper.ProjectHelper"
    result_table = SHARED_DIR + ".%s" % "meta.result_table.result_table_helper.ResultTableHelper"
    storekit = SHARED_DIR + ".%s" % "storekit.storekit_helper.StorekitHelper"
    stream = SHARED_DIR + ".%s" % "stream.stream_helper.StreamHelper"


def mock_shared_helper_wrap(shared_helper, origin_func_name, source_func):
    """
    # 例子：
        @mock_shared_helper_wrap(SharedHelperTypes.result_table, 'get_result_table', get_result_table)
        def test_1():
            pass
    对调用外部模块的方法进行mock
    若测试过程需要根据返回参数决定mock返回信息，则需要通过上下文管理器(mock.patch)的方式自主调用
    @param shared_helper: 待mock方法的SharedHelperTypes类型
    @param origin_func_name: 待mock方法名称
    @param source_func: 即将替换成的mock调用
    @return:
    """

    def _deco(func):
        @wraps(func, assigned=available_attrs(func))
        def _wrap(self, *arg, **kwargs):
            # 检查待mock模块是否合法
            if not isinstance(shared_helper, SharedHelperTypes):
                raise Exception("模块%s不支持mock." % shared_helper)
            # 检查待mock模块是否拥有指定待mock方法

            if not getattr(eval(shared_helper.value), origin_func_name, None):
                raise Exception("方法{}在模块{}中未定义.".format(origin_func_name, shared_helper.value))
            import_module_str = shared_helper.value + "." + origin_func_name
            with mock.patch(import_module_str, staticmethod(source_func)):
                func(self, *arg, **kwargs)

        return _wrap

    return _deco
