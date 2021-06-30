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

from xml.etree import ElementTree as ET

DEFAULT_PARTITION = "DEFAULT_PARTITION"
DEFAULT_MAX_APPLICATIONS = "10000"


def generate_capacity_xml(queue_infos):
    configuration = ET.Element("configuration")
    # generate common
    generate_property_xml(
        configuration,
        "yarn.scheduler.capacity.maximum-applications",
        DEFAULT_MAX_APPLICATIONS,
    )
    generate_property_xml(configuration, "yarn.scheduler.capacity.maximum-am-resource-percent", "0.3")
    generate_property_xml(
        configuration,
        "yarn.scheduler.capacity.resource-calculator",
        "org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator",
    )
    generate_property_xml(configuration, "yarn.scheduler.capacity.node-locality-delay", "-1")
    generate_property_xml(configuration, "yarn.scheduler.capacity.queue-mappings", "")
    generate_property_xml(configuration, "yarn.scheduler.capacity.queue-mappings-override.enable", "false")
    # node label
    # generate_property_xml(configuration, "yarn.node-labels.enabled", "true")
    # generate_property_xml(configuration, "yarn.node-labels.manager-class",
    #                       "org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager")
    # generate_property_xml(configuration, "yarn.node-labels.fs-store.root-dir", "hdfs:///user/yarn/node-labels")
    # queue info
    label_names = []
    queue_names = []
    for queue_info in queue_infos:
        label = queue_info["label"]
        queue_name = queue_info["queue_name"]
        if label not in label_names:
            label_names.append(label)
        if queue_name in queue_names:
            raise Exception("queue name " + queue_name + " only support config once")
        queue_names.append(queue_name)
        if label and label != "default":
            default_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".capacity"
            default_capacity_value = "0"
            generate_property_xml(configuration, default_capacity_name, default_capacity_value)

            default_max_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".maximum-capacity"
            default_max_capacity_value = "0"
            generate_property_xml(configuration, default_max_capacity_name, default_max_capacity_value)

            label_capacity_name = (
                "yarn.scheduler.capacity.root."
                + queue_info["queue_name"]
                + ".accessible-node-labels."
                + label
                + ".capacity"
            )
            label_capacity_value = str(queue_info["capacity"])
            generate_property_xml(configuration, label_capacity_name, label_capacity_value)

            label_max_capacity_name = (
                "yarn.scheduler.capacity.root."
                + queue_info["queue_name"]
                + ".accessible-node-labels."
                + label
                + ".maximum-capacity"
            )
            label_max_capacity_value = str(queue_info["max_capacity"])
            generate_property_xml(configuration, label_max_capacity_name, label_max_capacity_value)

            max_application = "yarn.scheduler.capacity.root." + queue_info["queue_name"] + ".maximum-applications"
            max_application_value = DEFAULT_MAX_APPLICATIONS
            generate_property_xml(configuration, max_application, max_application_value)

            accessible_node_labels = (
                "yarn.scheduler.capacity.root." + queue_info["queue_name"] + ".accessible-node-labels"
            )
            accessible_node_labels_value = label
            generate_property_xml(configuration, accessible_node_labels, accessible_node_labels_value)

            default_node_label_expression = (
                "yarn.scheduler.capacity.root." + queue_info["queue_name"] + ".default-node-label-expression"
            )
            default_node_label_expression_value = label
            generate_property_xml(
                configuration,
                default_node_label_expression,
                default_node_label_expression_value,
            )
        else:
            default_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".capacity"
            default_capacity_value = str(queue_info["capacity"])
            generate_property_xml(configuration, default_capacity_name, default_capacity_value)

            default_max_capacity_name = "yarn.scheduler.capacity.root." + queue_name + ".maximum-capacity"
            default_max_capacity_value = str(queue_info["max_capacity"])
            generate_property_xml(configuration, default_max_capacity_name, default_max_capacity_value)

            accessible_node_labels = (
                "yarn.scheduler.capacity.root." + queue_info["queue_name"] + ".accessible-node-labels"
            )
            accessible_node_labels_value = DEFAULT_PARTITION
            generate_property_xml(configuration, accessible_node_labels, accessible_node_labels_value)

            max_application = "yarn.scheduler.capacity.root." + queue_info["queue_name"] + ".maximum-applications"
            max_application_value = DEFAULT_MAX_APPLICATIONS
            generate_property_xml(configuration, max_application, max_application_value)

    for lable_name in label_names:
        if lable_name != "default":
            lable_name_capacity = "yarn.scheduler.capacity.root.accessible-node-labels." + lable_name + ".capacity"
            lable_name_capacity_value = "100"
            generate_property_xml(configuration, lable_name_capacity, lable_name_capacity_value)

    root_queue_name = "yarn.scheduler.capacity.root.queues"
    root_queue_value = ",".join(queue_names)
    generate_property_xml(configuration, root_queue_name, root_queue_value)

    tree = ET.ElementTree(configuration)
    tree.write("capacity-scheduler.xml")


def generate_property_xml(parent, name, value):
    property = ET.SubElement(parent, "property")
    name_dom = ET.SubElement(property, "name")
    name_dom.text = name
    value_dom = ET.SubElement(property, "value")
    value_dom.text = value


if __name__ == "__main__":
    queue_infos = [
        {
            "queue_name": "root.dataflow.stream.gem.cluster",
            "capacity": "4",
            "max_capacity": "4",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.gem.session",
            "capacity": "16",
            "max_capacity": "16",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.bkcloud.cluster",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.bkcloud.session",
            "capacity": "6",
            "max_capacity": "6",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.default.cluster",
            "capacity": "6",
            "max_capacity": "6",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.default.session",
            "capacity": "23",
            "max_capacity": "23",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.domino.cluster",
            "capacity": "2",
            "max_capacity": "2",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.domino.session",
            "capacity": "10",
            "max_capacity": "10",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.huanle.cluster",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.huanle.session",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.jungle_alert.cluster",
            "capacity": "3",
            "max_capacity": "3",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.jungle_alert.session",
            "capacity": "20",
            "max_capacity": "20",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.msdk.cluster",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.msdk.session",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.security.cluster",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.security.session",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.wangyou.cluster",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.wangyou.session",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.dataflow.stream.debug.session",
            "capacity": "1",
            "max_capacity": "1",
            "label": "stream",
        },
        {
            "queue_name": "root.maintain",
            "capacity": "100",
            "max_capacity": "100",
            "label": "default",
        },
        {
            "queue_name": "root.dataflow.batch.default",
            "capacity": "70",
            "max_capacity": "100",
            "label": "batch",
        },
        {
            "queue_name": "root.dataflow.batch.debug",
            "capacity": "3",
            "max_capacity": "100",
            "label": "batch",
        },
        {
            "queue_name": "root.dataflow.batch.riot",
            "capacity": "7",
            "max_capacity": "100",
            "label": "batch",
        },
        {
            "queue_name": "root.dataflow.batch.msdk",
            "capacity": "5",
            "max_capacity": "100",
            "label": "batch",
        },
        {
            "queue_name": "root.dataflow.batch.tgpa",
            "capacity": "15",
            "max_capacity": "100",
            "label": "batch",
        },
        {
            "queue_name": "root.algorithm.interactive",
            "capacity": "25",
            "max_capacity": "100",
            "label": "algorithm",
        },
        {
            "queue_name": "root.algorithm.period",
            "capacity": "75",
            "max_capacity": "100",
            "label": "algorithm",
        },
    ]

    generate_capacity_xml(queue_infos)
