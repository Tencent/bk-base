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

from dataflow.uc.exceptions.comp_exceptions import IllegalArgumentException
from dataflow.uc.settings import ComponentType, ImplementType, UnifiedComputingJobType


class JobVertex(object):
    def __init__(self, heads):
        self.heads = heads
        self.processing = None
        self.inputs = []
        self.outputs = []
        self.job_type = None

    def set_processing(self, processing):
        """
        The IDs of all operators contained in this vertex.

        The ID pairs are stored depth-first post-order; for the forking chain below the ID's
        would be stored as [D, E, B, C, A]
        A - B -D
         \\   \
         C    E
        This is the same order that processing are stored in the

        :param processing: job vertex包含的processing
        """
        self.processing = processing

    def add_input(self, in_job_edge):
        self.inputs.append(in_job_edge)

    def add_output(self, out_job_edge):
        self.outputs.append(out_job_edge)

    def set_job_type(self, component_type, implement_type, programming_language):
        """
        增加适配器需要在这里增加对应的job 类型

        :param component_type:  组件类型 如 spark/flink
        :param implement_type:  实现方式 如 sql/code
        :param programming_language:  编程语言 如 java/python
        :return:
        """
        if (
            component_type == ComponentType.SPARK_STRUCTURED_STREAMING.value
            and implement_type == ImplementType.CODE.value
        ):
            self.job_type = UnifiedComputingJobType.SPARK_STRUCTURED_STREAMING_CODE.value
        elif component_type == ComponentType.SPARK.value and implement_type == ImplementType.CODE.value:
            self.job_type = UnifiedComputingJobType.SPARK_CODE.value
        elif component_type == ComponentType.TENSORFLOW.value:
            self.job_type = UnifiedComputingJobType.TENSORFLOW_CODE.value
        else:
            raise IllegalArgumentException(
                "Not support the component_type %s, implement_type %s, programming_language %s job"
                % (component_type, implement_type, programming_language)
            )

    def get_existing_job_id(self, existing_job_infos):
        """
        获取当前 job vertex 在已存在任务中的job id

        :param existing_job_infos:  已存在的job信息，结构为{'job_id': set([processing])}
        :return: job id
        """
        current_one_processing_id = self.processing[0].processing_id
        # 获取当前 job vertex 中包含的processing
        current_processing_set = set()
        for one_processing in self.processing:
            current_processing_set.add(one_processing.processing_id)
        for job_id, processing_set in list(existing_job_infos.items()):
            if current_one_processing_id in processing_set and processing_set == current_processing_set:
                return job_id
        raise IllegalArgumentException(
            "The existing job %s is inconsistent with the updated job %s"
            % (str(existing_job_infos), str(current_processing_set))
        )
