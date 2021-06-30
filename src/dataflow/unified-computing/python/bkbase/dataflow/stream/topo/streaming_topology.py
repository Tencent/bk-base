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

from bkbase.dataflow.core.types.node import (
    StreamingSinkNodeBuilder,
    StreamingSourceNodeBuilder,
)
from bkbase.dataflow.core.types.savepoint import Savepoint
from bkbase.dataflow.core.types.topology import Topology


class StreamTopology(Topology):
    def __init__(self, builder):
        super().__init__(builder)
        self.savepoint = Savepoint(
            builder.savepoint["opened"], builder.savepoint["hdfs_path"], builder.savepoint["start_position"]
        )
        self.map_nodes(builder.type_tables)
        self.run_mode = builder.run_mode
        self.code = builder.code
        self.metric = builder.metric

    def map_nodes(self, type_tables):
        for node_type in type_tables:
            if node_type == "source":
                self.source_nodes = self.__generate_source_nodes(type_tables[node_type])
            elif node_type == "sink":
                self.sink_nodes = self.__generate_sink_nodes(type_tables[node_type])

    def __generate_source_nodes(self, args):
        """
        :param args:
            {
                '591_xxx': {
                    'id': 'xxx',
                    'input': {
                        'type': 'kafka',
                        'cluster_domain': 'kafka.service.test-1.bk',
                        'cluster_port': 9092
                    },
                    'name': 'xxx',
                    'fields': [
                        'origin': '',
                        'field': 'dtEventTime',
                        'type': 'string',
                        'description': 'xx',
                        'event_time': false
                    ]
                }
            }
        :return:
        """
        source_nodes = []
        for source_result_table_id in args:
            table_info = args[source_result_table_id]
            source_nodes.append(StreamingSourceNodeBuilder(source_result_table_id, table_info).create())
        return source_nodes

    def __generate_sink_nodes(self, args):
        sink_nodes = []
        for sink_result_table_id in args:
            table_info = args[sink_result_table_id]
            sink_nodes.append(StreamingSinkNodeBuilder(sink_result_table_id, table_info).create())
        return sink_nodes
