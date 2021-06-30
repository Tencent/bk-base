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


class NodeField(object):
    def __init__(self, field_name, field_type, description, event_time):
        self.field_name = field_name
        self.field_type = field_type
        self.description = description
        self.event_time = event_time

    def __repr__(self):
        return "{{field_name: {}, field_type: {}, description: {}, event_time: {}}}".format(
            self.field_name, self.field_type, self.description, self.event_time
        )


class Node(object):
    def __init__(self):
        self.node_id = None
        self.fields = None
        self.system_fields = {"dtEventTime": "string"}

    def get_field_names(self):
        field_names = []
        for field in self.fields:
            field_names.append(field.field_name)
        return field_names


class SourceNode(Node):
    def __init__(self):
        super().__init__()
        self.input = None

    def __str__(self):
        return "Source node, node_id {}, fields {}, input {}".format(self.node_id, self.fields, self.input)


class SinkNode(Node):
    def __init__(self):
        super().__init__()
        self.output = None
        self.event_time = None

    def __str__(self):
        return "Sink node, node_id {}, fields {}, output {}, event_time {}".format(
            self.node_id,
            self.fields,
            self.output,
            self.event_time,
        )


class NodeBuilder(object):
    def __init__(self):
        self.node = None

    def create(self):
        return self.node


class StreamingNodeBuilder(NodeBuilder):
    def __init__(self, node_type, result_table_id, table_info):
        super().__init__()
        if node_type == "source":
            self.node = SourceNode()
        elif node_type == "sink":
            self.node = SinkNode()
        else:
            raise Exception("Not support node type %s" % node_type)
        # 系统字段
        self.system_field = {"dtEventTime": "string", "dtEventTimeStamp": "long", "localTime": "string"}
        self._node_type = node_type
        self._result_table_id = result_table_id
        self._table_info = table_info
        # 将rt信息映射为node
        self._map_node()

    def _map_node(self):
        self.node.node_id = self._result_table_id
        self.node.fields = self._map_fields()

    def _map_fields(self):
        fields = []
        for _field in self._table_info["fields"]:
            fields.append(NodeField(_field["field"], _field["type"], _field["description"], _field["event_time"]))
        if self._node_type == "sink":
            # 对输出增加系统字段
            for key, value in self.system_field.items():
                fields.append(NodeField(key, value, None, event_time=False))
        return fields

    def parse_storage_info(self, channel_direction):
        """
        :param channel_direction: input | output
        :return:
        """
        storage_info = self._table_info[channel_direction]
        if storage_info["type"] == "kafka":
            kafka_info = "{cluster_domain}:{cluster_port}".format(
                cluster_domain=storage_info["cluster_domain"], cluster_port=storage_info["cluster_port"]
            )
            topic = "table_{result_table_id}".format(result_table_id=self._result_table_id)
            return {"type": storage_info["type"], "info": kafka_info, "topic": topic}
        else:
            raise Exception("Not support parsing storage %s" % storage_info["type"])


class StreamingSourceNodeBuilder(StreamingNodeBuilder):
    def __init__(self, result_table_id, table_info):
        super().__init__("source", result_table_id, table_info)
        self._generate_input()

    def _generate_input(self):
        self.node.input = self.parse_storage_info("input")


class StreamingSinkNodeBuilder(StreamingNodeBuilder):
    def __init__(self, result_table_id, table_info):
        super().__init__("sink", result_table_id, table_info)
        self._generate_output()
        self._get_event_time()

    def _generate_output(self):
        self.node.output = self.parse_storage_info("output")

    def _get_event_time(self):
        for field in self.node.fields:
            if field.event_time:
                self.node.event_time = field.field_name
