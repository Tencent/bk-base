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
from datahub.common.const import (
    BK_BIZ_ID,
    CHANNEL_TYPE,
    CONNECTION_INFO,
    PHYSICAL_TABLE_NAME,
    TABLE_NAME,
)
from datahub.databus.common_helper import find_key_by_value
from datahub.databus.exceptions import (
    ChannelNotFound,
    TaskCountErr,
    TaskCreateErr,
    TaskGenConfigErr,
    TaskNoSuitableCluster,
    TaskStartErr,
    TaskTopicNotFound,
)
from datahub.databus.settings import (
    CHANNEL_CLUSTER_NAME_DEFAULT_VALUE,
    MODULE_SHIPPER,
    PULSAR_DEFAULT_PARTITION,
    TYPE_KAFKA,
    TYPE_PULSAR,
    TYPE_SINK,
    log_item_format,
)
from datahub.databus.task import task_utils
from datahub.databus.task.kafka import task as kafka_task
from datahub.databus.task.pulsar import task as pulsar_task
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.databus.task.status import set_databus_task_status, set_shipper_status

from datahub.databus import channel, exceptions, model_manager, models, rt


class BaseShipper(object):
    # 下游存储类型
    storage_type = None
    component = None
    module = MODULE_SHIPPER

    class OperationType(object):
        ADD_CONNECTOR = "add_connector"
        DELETE_CONNECTOR = "delete_connector"
        ADD_STORAGE = "add_storage"
        UPDATE_STORAGE = "update_storage"
        ADD_CLEAN = "add_clean"
        UPDATE_CLEAN = "update_clean"
        DELETE_CLEAN = "delete_clean"

    def __init__(self, rt_id, bk_username, storage_params):
        """
        :param rt_id: 该任务的result_table_id
        :param bk_username: 该任务的操作者
        :param storage_params: 此次shipper任务创建时的原始参数字符串，用做日志记录
        """
        self.rt_id = rt_id
        self.rt_info = rt.get_databus_rt_info(rt_id)
        self.bk_username = bk_username
        self.storage_params = storage_params

        self._builder_source_info()
        self._builder_sink_info()
        self._builder_task_info()

    def _builder_source_info(self):
        # 当前cluster_type都为kafka/pulsar
        self.source_type = self.rt_info[CHANNEL_TYPE]
        self.source_storage = self.rt_info.get("channel_cluster_name", CHANNEL_CLUSTER_NAME_DEFAULT_VALUE, "")
        # 普通shipper任务从rt关联的topic读取数据
        self.source_channel_topic = rt.get_topic_name(self.rt_id)

    def _builder_sink_info(self):
        # 这里在存储配置中可能指定另外的物理表名，和默认规则拼出来的table_name值不一样
        if PHYSICAL_TABLE_NAME in self.rt_info[self.storage_type]:
            self.physical_table_name = self.rt_info[self.storage_type][PHYSICAL_TABLE_NAME]
        else:
            self.physical_table_name = "{}_{}".format(
                self.rt_info[TABLE_NAME],
                self.rt_info[BK_BIZ_ID],
            )

        self.sink_conn_info = self.rt_info[self.source_storage][CONNECTION_INFO]
        self.sink_storage_conn = json.loads(self.sink_conn_info)

    def _builder_task_info(self):
        self.connector_name = task_utils.generate_connector_name(self.component, self.rt_id)
        self.task_nums = self._get_task_nums()
        self.config_generator = task_utils.config_factory[self.source_storage]

    def _get_task_nums(self):
        if self.source_type == TYPE_KAFKA:
            # 使用topic中的partition数量作为启动的task数量
            task_nums = channel.get_topic_partition_num(self.rt_info["bootstrap.servers"], self.source_channel_topic)
        elif self.source_type == TYPE_PULSAR:
            if not pulsar_topic.exist_partition_topic(self.rt_info["bootstrap.servers"], self.source_channel_topic):
                pulsar_topic.create_partition_topic(
                    self.rt_info["bootstrap.servers"],
                    self.source_channel_topic,
                    PULSAR_DEFAULT_PARTITION,
                )
            task_nums = pulsar_topic.get_partitions(self.rt_info["bootstrap.servers"], self.source_channel_topic)
            if task_nums == 0:
                raise TaskTopicNotFound(message_kv={"topic": self.source_channel_topic})
        else:
            raise ChannelNotFound(errors="Unsupported channel type:%s" % self.source_type)

        if task_nums == 0:
            raise TaskTopicNotFound(message_kv={"topic": self.source_channel_topic})
        return task_nums

    def start(self):
        self.add_task_log(self.OperationType.ADD_CONNECTOR, "prepare to start the task")
        try:
            self._before_add_task()
            self._add_databus_shipper_task()
            if self._need_start_task():
                self._start_databus_shipper_task()
                self.add_task_log(self.OperationType.ADD_CONNECTOR, "successfully started the task")
        except BaseAPIError as task_error:
            self.add_task_log(self.OperationType.ADD_CONNECTOR, "start task failed")
            logger.error("start databus shipper task failed, exception %s" % task_error.message)
            raise TaskCreateErr(
                message_kv={
                    "result_table_id": self.rt_id,
                    "storage": self.source_storage,
                    "message": "{}({})".format(task_error.message, task_error.CODE),
                }
            )

    def _before_add_task(self):
        pass

    def _need_start_task(self):
        return True

    def _add_databus_shipper_task(self):
        """
        添加分发任务
        """
        logger.info(
            "add {} task: {} {} {}".format(self.connector_name, self.storage_type, self.rt_id, self.source_storage)
        )

        if task_utils.is_databus_task_exists(
            self.connector_name,
            self.source_type,
            self.module,
            self.component,
            self.source_storage,
        ):
            logger.info(
                "task %s already exists in one of %s:%s:%s:%s"
                % (
                    self.connector_name,
                    self.source_type,
                    self.module,
                    self.storage_type,
                    self.source_storage,
                )
            )
            return

        cluster_name = self._get_suitable_cluster_name()
        sink_type = self._get_sink_type()

        model_manager.add_databus_task(
            self.rt_id,
            self.connector_name,
            cluster_name,
            "{}#{}".format(self.source_storage, self.rt_id),
            self.source_type,
            "{}#{}".format(sink_type, self.rt_id),
            sink_type,
        )

    def _get_suitable_cluster_name(self):
        cluster_name = task_utils.get_suitable_cluster_for_rt(
            self.rt_info,
            self.source_type,
            self.module,
            self.component,
            self.source_storage,
        )
        if not cluster_name:
            raise TaskNoSuitableCluster(
                message_kv={
                    "task": self.connector_name,
                    "cluster": "%s:%s:%s:%s"
                    % (
                        self.source_type,
                        self.module,
                        self.storage_type,
                        self.source_storage,
                    ),
                }
            )
        return cluster_name

    def _get_sink_type(self):
        return self.storage_type

    def _start_databus_shipper_task(self):
        task = self.__get_shipper_task()
        # 构建启动task的任务配置
        conf = self._get_shipper_task_conf(task["cluster_name"])
        self.set_module_status(models.DataBusTaskStatus.STOPPED)
        self._start_or_create_task(task["cluster_name"], conf)

    def __get_shipper_task(self):
        tasks = model_manager.get_databus_task_info(self.connector_name)
        if len(tasks) != 1:
            self.set_module_status(models.DataBusTaskStatus.FAILED)
            logger.error("{} shipper task count is wrong! {}".format(self.rt_id, tasks))
            raise TaskCountErr(
                message_kv={
                    "result_table_id": self.rt_id,
                    "storage": self.storage_type,
                    "count": len(tasks),
                }
            )
        return tasks[0]

    def _get_shipper_task_conf(self, cluster_name):
        """
        获取分发任务配置
        """
        raise TaskGenConfigErr(message_kv={"task": self.connector_name, "cluster": cluster_name})

    def _start_or_create_task(self, cluster_name, conf):
        # 启动任务
        if self.source_type == TYPE_PULSAR:
            ret = pulsar_task.start_or_create_task(cluster_name, TYPE_SINK, self.connector_name, conf)
            # 若存在则更新配置
            if ret:
                ret = pulsar_task.update_task(cluster_name, TYPE_SINK, self.connector_name, conf)
        else:
            ret = kafka_task.start_task(self.connector_name, conf, cluster_name, self.component)

        if ret:
            logger.info("task {} is started and running in {}".format(self.connector_name, cluster_name))
            self.set_module_status(models.DataBusTaskStatus.STARTED, json.dumps(conf))

        else:
            logger.error("failed to start task {} in {}".format(self.connector_name, cluster_name))
            raise TaskStartErr(message_kv={"task": self.connector_name, "cluster": cluster_name})

    def add_task_log(self, operation_type, content, error_msg=""):
        _content = json.dumps(
            {
                "content": content,
                "error_msg": error_msg,
            }
        )
        try:
            model_manager.add_databus_oplog(
                operation_type,
                log_item_format % (self.storage_type, self.rt_id),
                "",
                self.storage_params,
                _content,
                self.bk_username,
            )
        except Exception as e:
            logger.warning(u"add operation log error:%s" % str(e))

    def set_module_status(self, status, conf=None):
        set_shipper_status(self.rt_id, self.connector_name, status, conf)

    def set_databus_task_status(self):
        set_databus_task_status(
            self.connector_name,
            self.__get_shipper_task()["cluster_name"],
            models.DataBusTaskStatus.RUNNING,
        )

    @classmethod
    def get_storage_config(cls, params):
        """获取不同存储的存储配置"""
        fields = params.get("fields")
        storage_params = StorageParams()

        if fields:
            # 必须要求清洗配置是assign_json
            result_table_id = "{}_{}".format(
                params["bk_biz_id"],
                params["result_table_name"],
            )
            clean_config = model_manager.get_clean_by_processing_id(result_table_id)
            if not clean_config:
                raise exceptions.CleanNotFoundError(message_kv={"result_table_id": result_table_id})
            json_conf = json.loads(clean_config.json_config)

            assign_json_list = []
            assign_json_list = find_key_by_value(json_conf, "assign_json", assign_json_list)
            can_json_fields = []
            if assign_json_list:
                for assign_json in assign_json_list:
                    for filed in assign_json.get("assign"):
                        can_json_fields.append(filed.get("assign_to"))

            storage_params.can_json_fields = can_json_fields

            for field in fields:
                cls._field_handler(field, storage_params)

        return cls._get_storage_config(cls, params, storage_params)

    @classmethod
    def _field_handler(cls, field, storage_params):
        """不同存储中对于每一个field字段的处理"""
        pass

    @classmethod
    def _get_storage_config(cls, params, storage_params):
        return json.dumps({})

    @classmethod
    def _compare_connector_conf(cls, cluster_name, connector_name, conf, running_conf):
        return False


class StorageParams(object):
    analyzed_fields = list()
    doc_values_fields = list()
    json_fields = list()
    key_fields = list()
    pri_key_fields = list()
    value_fields = list()
    dim_fields = list()
    indexed_fields = list()
    can_json_fields = []
