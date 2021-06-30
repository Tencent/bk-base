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

from django.db import transaction

from dataflow.stream.handlers import processing_stream_info
from dataflow.stream.processing.processing_builder import FlinkCodeProcessingBuilder, SparkCodeProcessingBuilder
from dataflow.stream.processing.processing_for_code import FlinkCodeProcessing, SparkCodeProcessing
from dataflow.stream.settings import ImplementType


class ProcessingHandler(object):
    """
    Processing Handler
    """

    _processing_id = None
    _processing_info = None
    _processing = None

    def __init__(self, processing_id):
        self._processing_id = processing_id
        # 初始化processing实例
        self._processing = self._build_inner_processing()

    def _build_inner_processing(self):
        if ImplementType.CODE.value == self.processor_type and self.component_type_without_version == "flink":
            return FlinkCodeProcessing(self._processing_id)
        elif (
            ImplementType.CODE.value == self.processor_type
            and self.component_type_without_version == "spark_structured_streaming"
        ):
            return SparkCodeProcessing(self._processing_id)
        else:
            raise Exception("component_type(%s) is not defined." % self.component_type)

    @property
    def component_type(self):
        return self.processing_info.component_type

    @property
    def component_type_without_version(self):
        return self.component_type.split("-")[0]

    @property
    def processor_type(self):
        return self.processing_info.processor_type

    @property
    def processing_info(self):
        if not self._processing_info:
            self._processing_info = processing_stream_info.get(self._processing_id)
        return self._processing_info

    @classmethod
    def create_processing(cls, args):
        cls.compatibleCodeJar(args)
        if args["component_type"] == "flink":
            return FlinkCodeProcessingBuilder().create_processing(args)
        elif args["component_type"] == "spark_structured_streaming":
            return SparkCodeProcessingBuilder().create_processing(args)
        else:
            raise Exception("component_type(%s) is not defined." % args["component_type"])

    @transaction.atomic()
    def update_processing(self, args):
        self.compatibleCodeJar(args)
        return self._processing.update_processing(args)

    # 兼容code和jar两种提交方式
    @classmethod
    def compatibleCodeJar(self, args):
        if "code" not in args["dict"]:
            args["dict"]["code"] = ""
        if "user_main_class" not in args["dict"]:
            args["dict"]["user_main_class"] = "_._"
        if "package" not in args["dict"]:
            args["dict"]["package"] = {}
            args["dict"]["package"]["path"] = ""
        return args
