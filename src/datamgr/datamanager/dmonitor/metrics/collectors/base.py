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
import logging
import time


class BaseOffsetCollector(object):
    DEFAULT_HASH_DIVISOR = 4

    def __init__(self, task):
        super(BaseOffsetCollector, self).__init__()
        self._task = task
        self._clients = {}
        self._clusters = []
        self._topics = {}
        self._last_collect_times = {}
        self._last_refresh_clusters_time = 0
        self._last_offsets = {}
        self._task_topic_hash_value = None
        self._topic_hash_divisor = self.DEFAULT_HASH_DIVISOR
        self._topic_hash_values = {}
        self.init_config()
        self.init_clients(int(time.time()))

    def init_config(self):
        """初始化待统计的kafka的集群信息"""
        logging.info("init kafka clients")
        # 维护好clusters属性
        if not hasattr(self._task, "_action_config"):
            return False
        action_config = self._task._action_config
        self._task_topic_hash_value = action_config.get("topic_hash_value", None)
        self._topic_hash_divisor = action_config.get(
            "topic_hash_divisor", self.DEFAULT_HASH_DIVISOR
        )
        return True

    def init_clients(self, cur_time):
        """初始化所有待统计的数据源相关客户端

        :param cur_time: 当前时间
        """
        raise NotImplementedError()

    def init_cluster_client(self, cluster_name, kafka_config=False):
        """初始化客户端

        :param cluster_name: 集群名称
        :param kafka_config: kafka配置
        """
        raise NotImplementedError()

    def clear_cluster(self, cluster_name):
        """清理过期集群信息

        :param cluster_name: 集群名称
        """
        if cluster_name in self._clients:
            del self._clients[cluster_name]
        if cluster_name in self._topics:
            del self._topics[cluster_name]
        if cluster_name in self._last_collect_times:
            del self._last_collect_times[cluster_name]

    def collect_cluster(self, cluster_name):
        """获取集群kafka元信息

        :param cluster_name: 集群名称
        """
        raise NotImplementedError()

    def get_topic_hash(self, topic):
        """根据topic生成hash信息以做扩展

        :param topic: topic结构体
        """
        topic = str(topic)
        if topic in self._topic_hash_values:
            return self._topic_hash_values[topic]
        cnt = 0
        for char in topic:
            cnt += ord(char)
        try:
            self._topic_hash_values[topic] = cnt % self._topic_hash_divisor
        except Exception:
            logging.error("The config of topic hash divisor is error")
            self._topic_hash_values[topic] = cnt % self.DEFAULT_HASH_DIVISOR
        return self._topic_hash_values[topic]
