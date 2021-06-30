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

import json

from dataflow.shared.storekit.storekit_helper import StorekitHelper


class ConfigHDFS(object):
    def __init__(self, cluster_group="default"):
        self.CONFIGS = ConfigHDFS.get_all_cluster()
        if not self.is_legal_cluster_group(cluster_group):
            raise Exception(
                "不合法的cluster_group: {}.合法cluster_group: {}".format(cluster_group, ",".join(list(self.CONFIGS.keys())))
            )
        self.cluster_group = cluster_group
        self.config = self.CONFIGS[cluster_group]

    def is_legal_cluster_group(self, cluster_group):
        return cluster_group in self.CONFIGS

    @staticmethod
    def get_all_cluster():
        hdfs_config = {}
        cluster_configs = StorekitHelper.get_storage_cluster_configs("hdfs")
        for cluster_config in cluster_configs:
            connection_info = json.loads(cluster_config["connection_info"])
            hosts = connection_info["hosts"].split(",")
            ids = connection_info["ids"].split(",")
            port = connection_info["port"]
            url = connection_info["hdfs_url"]
            hdfs_config[cluster_config["cluster_group"]] = {
                "nn_ids": ids,
                "nn_hosts": hosts,
                "nn_port": port,
                "url": url,
            }
        return hdfs_config

    def get_namenode_ids(self):
        return self.config["nn_ids"]

    # def get_namenode_hosts(self):
    #     return self.config['nn_hosts']

    def get_namenode_hosts(self):
        return self.config["nn_hosts"]

    def get_namenode_port(self):
        return self.config["nn_port"]

    def get_namenode_addrs(self):
        return ["{}:{}".format(host, self.get_namenode_port()) for host in self.get_namenode_hosts()]

    def get_webhdfs_urls(self):
        return ["http://%s/webhdfs/v1" % nn_addr for nn_addr in self.get_namenode_addrs()]

    def get_jmx_namenode_status_urls(self):
        return [
            "http://%s/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" % nn_addr
            for nn_addr in self.get_namenode_addrs()
        ]

    def get_jmx_namenode_info_urls(self):
        return [
            "http://%s/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo" % nn_addr
            for nn_addr in self.get_namenode_addrs()
        ]

    def get_url(self):
        return self.config["url"]
