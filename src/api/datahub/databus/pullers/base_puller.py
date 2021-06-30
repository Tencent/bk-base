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

from common.exceptions import BaseAPIError
from common.log import logger
from datahub.databus.exceptions import (
    TaskAccessResourceNotFound,
    TaskConfigErr,
    TaskCreateErr,
    TaskGenConfigErr,
)
from datahub.databus.settings import MODULE_PULLER
from datahub.databus.task.puller_task import start_puller_task
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.databus.task.pulsar.topic import get_pulsar_channel_token
from datahub.databus.task.status import set_databus_task_status
from datahub.databus.task.task_utils import (
    _stop_task_without_exception,
    config_factory,
    generate_puller_connector_name,
    get_available_puller_cluster_name,
    is_databus_task_exists,
)

from datahub.databus import model_manager, models, rawdata, settings


class BasePuller(object):
    component = None
    module = MODULE_PULLER

    def __init__(self, data_id, raw_data, storage_channel, bk_username):
        """
        :param rt_id: 该任务的result_table_id
        :param bk_username: 该任务的操作者
        :param storage_params: 此次puller任务创建时的原始参数字符串，用做日志记录
        """
        self.data_id = data_id
        self.raw_data = raw_data
        self.bk_username = bk_username
        self.storage_channel = storage_channel
        self.connector_name = generate_puller_connector_name(self.component, data_id)
        self.cluster_name = self._get_connector_cluster_name()
        self.sink_topic = rawdata.get_topic_name(raw_data)
        self.resource_info, self.resource_json = self._get_resource_info()
        self.config_factory = config_factory[storage_channel.cluster_type]

    def _get_connector_cluster_name(self):
        connectors = model_manager.get_connector_route(self.connector_name)
        if connectors:
            # 若存在记录则使用上次的集群
            return connectors.cluster_name
        else:
            return get_available_puller_cluster_name(
                self.storage_channel.cluster_type,
                self.storage_channel.cluster_name,
                self.component,
            )

    def _get_resource_info(self):
        try:
            resource_info = models.AccessResourceInfo.objects.get(raw_data_id=self.data_id)
            resource_json = json.loads(resource_info.resource)
        except models.AccessResourceInfo.DoesNotExist:
            raise TaskAccessResourceNotFound(message_kv={"data_id": self.data_id})
        except Exception:
            raise TaskConfigErr()
        return resource_info, resource_json

    def start(self):
        try:
            self._before_add_task()
            self._add_databus_puller_task()
            conf = self._get_puller_task_conf()
            if self._need_start_task():
                logger.info(
                    "going to start {} puller {} with conf {}".format(self.component, self.connector_name, conf)
                )
                self._start_databus_puller_task(conf)
        except BaseAPIError as task_error:
            logger.error("start databus puller task failed, exception %s" % task_error.message)
            raise TaskCreateErr(
                message_kv={
                    "data_id": self.data_id,
                    "message": "{}({})".format(task_error.message, task_error.CODE),
                }
            )

    def stop(self):
        obj = model_manager.get_connector_route(self.connector_name)
        if not obj:
            return
        _stop_task_without_exception(self.connector_name, self.cluster_name)

    def _before_add_task(self):
        if self.storage_channel.cluster_type == settings.TYPE_PULSAR:
            topic = rawdata.get_topic_name(self.raw_data)
            pulsar_topic.create_partition_topic_if_not_exist(
                self.storage_channel,
                topic,
                settings.PULSAR_DEFAULT_PARTITION,
                get_pulsar_channel_token(self.storage_channel),
            )

    def _add_databus_puller_task(self):
        """
        添加拉取任务
        """
        logger.info(
            "add %s task: component=%s topic=%s channel_name=%s"
            % (
                self.connector_name,
                self.component,
                self.sink_topic,
                self.storage_channel.cluster_name,
            )
        )

        if not is_databus_task_exists(
            self.connector_name,
            self.storage_channel.cluster_type,
            self.module,
            self.component,
            self.storage_channel.cluster_name,
        ):
            # 注意，puller目前没有rt_id信息，后面可能要确认寻找
            model_manager.add_databus_task(
                "raw_data_id-%s" % self.data_id,
                self.connector_name,
                self.storage_channel.cluster_name,
                "{}#{}".format(self.component, self.data_id),
                self.component,
                "{}#{}".format(self.storage_channel.cluster_name, self.sink_topic),
                self.storage_channel.cluster_type,
                settings.MODULE_PULLER,
            )

    def _get_puller_task_conf(self):
        """
        获取分发任务配置
        """
        raise TaskGenConfigErr(message_kv={"task": self.connector_name, "cluster": self.cluster_name})

    def _need_start_task(self):
        return True

    def _start_databus_puller_task(self, conf):
        # 启动任务
        start_puller_task(
            self.storage_channel.cluster_type,
            self.cluster_name,
            self.connector_name,
            conf,
        )

    def set_databus_task_status(self):
        set_databus_task_status(self.connector_name, self.cluster_name, models.DataBusTaskStatus.RUNNING)

    @classmethod
    def _compare_connector_conf(cls, cluster_name, connector_name, conf, running_conf):
        return False
