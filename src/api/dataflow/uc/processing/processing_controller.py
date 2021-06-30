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

from dataflow.uc.adapter.tensorflow_code_operation_adapter import TensorFlowCodeOperationAdapter
from dataflow.uc.exceptions.comp_exceptions import IllegalArgumentException
from dataflow.uc.settings import ComponentType


class ProcessingController(object):
    def __init__(self, processing_id):
        self.processing_id = processing_id

    @classmethod
    def create_processing(cls, processing_conf):
        operation = cls.__get_operation_adapter(processing_conf["component_type"])
        return operation.create_processing(processing_conf)

    def update_processing(self, processing_conf):
        processing_conf["processing_id"] = self.processing_id
        operation = self.__get_operation_adapter(processing_conf["component_type"])
        return operation.update_processing(self.processing_id, processing_conf)

    @classmethod
    def __get_operation_adapter(cls, component_type):
        if component_type == ComponentType.TENSORFLOW.value:
            return TensorFlowCodeOperationAdapter()
        else:
            raise IllegalArgumentException("Not support processing of the component type %s" % component_type)
