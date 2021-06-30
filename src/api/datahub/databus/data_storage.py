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

from celery import shared_task
from common.bklanguage import BkLanguage
from common.exceptions import ApiRequestError
from common.local import get_request_username
from common.log import logger
from datahub.common.const import (
    BK_USERNAME,
    CHANNEL,
    CHANNEL_CLUSTER_CONFIG_ID,
    CHANNEL_CLUSTER_INDEX,
    CLEAN,
    CLICKHOUSE,
    CLUSTER_TYPE,
    DATA_SET_ID,
    DATA_SET_TYPE,
    DATA_TYPE,
    DESCRIPTION,
    DIMENSION_NAME,
    DIMENSION_TYPE,
    GENERATE_TYPE,
    GEOG_AREA,
    ID,
    INPUTS,
    MESSAGE,
    OUTPUTS,
    PROJECT_ID,
    RAW_DATA_ID,
    RESULT_TABLE,
    RESULT_TABLE_ID,
    SHIPPER,
    STORAGE,
    STORAGE_CLUSTER_CONFIG_ID,
    STORAGE_TYPE,
    TAGS,
    TRANSFERRING_ALIAS,
    TRANSFERRING_ID,
    TRANSFERRING_TYPE,
    USER,
)
from datahub.databus.api import AuthApi, MetaApi, StoreKitApi
from datahub.databus.common_helper import add_task_log
from datahub.databus.exceptions import ClusterPermissionError, NotFoundTaskClassError
from datahub.databus.models import DataBusTaskStatus
from django.utils.translation import ugettext_lazy as _

from datahub.databus import clean, etl_template, exceptions, model_manager, rt, settings

from ..access.raw_data.rawdata import add_storage_channel
from ..common.const import TCAPLUS
from .clean import get_clean_list_using_data_id
from .exceptions import MetaDeleteDTError, NotFoundRtError, RawDataNotExistsError
from .shippers.factory import ShipperFactory
from .task import storage_task
from .task.task import delete_task


def get_expire_time(expires):
    """
    根据英文翻译中文
    :param expires: expires过期时间
    :return: 中文过期时间
    """
    expires_dict = {
        "d": _(u"d"),
        "h": _(u"h"),
        "m": _(u"m"),
        "-1": _(u"-1"),
    }
    if expires == "-1":
        return expires_dict["-1"]

    for key, value in expires_dict.items():
        if key in expires:
            return u"{}{}".format(expires[0:-1], expires_dict[key])

    return u"{}{}".format(expires, expires_dict["d"])


def trans_params(params):
    """
    转换清洗参数
    :param params:
    :return:
    """
    # 查询原始数据信息
    raw_data = model_manager.get_raw_data_by_id(params["raw_data_id"])
    if not raw_data:
        raise RawDataNotExistsError(message_kv={"raw_data_id": params["raw_data_id"]})

    storage_cluster_group = rt.get_storage_cluster(params["storage_type"], params["storage_cluster"])
    # 检查集群组信息
    cluster_group_res = AuthApi.raw_data.cluster_group({"raw_data_id": raw_data.id})
    if not cluster_group_res.is_success() or not cluster_group_res.data or not storage_cluster_group:
        logger.error(
            "data_id:{} has no auth cluster group, storage_cluster_group:{}".format(raw_data.id, storage_cluster_group)
        )
        raise ClusterPermissionError()

    has_permission = False
    for cluster_group in cluster_group_res.data:
        if cluster_group["cluster_group_id"] == storage_cluster_group["cluster_group"]:
            has_permission = True
            break

    if not has_permission:
        logger.error("data_id:%s has no auth cluster group" % raw_data.id)
        raise ClusterPermissionError()

    if params[DATA_TYPE] == CLEAN:
        result_table_id = "{}_{}".format(raw_data.bk_biz_id, params["result_table_name"])
        params["result_table_id"] = result_table_id
        params["bk_biz_id"] = raw_data.bk_biz_id
        clean_params = {
            "raw_data_id": params["raw_data_id"],
            "json_config": etl_template["json_config"],
            "pe_config": "",
            "bk_biz_id": params["bk_biz_id"],
            "result_table_name": params["result_table_name"],
            "result_table_name_alias": params["result_table_name_alias"],
            "description": u"日志原始数据清洗",
            "bk_username": params["bk_username"],
            "fields": params["fields"],
        }

        params["clean"] = clean_params

    return params


def get_storage_type_alias(storage_type):
    """
    获取存储类型别名
    :param storage_type: 存储类型
    :return: 存储类型别名
    """
    storage_scenaria_configs = StoreKitApi.scenarios.list()
    if storage_scenaria_configs:
        for storage_scenaria_config in storage_scenaria_configs.data:
            if storage_scenaria_config["cluster_type"].lower() == storage_type.lower():
                return u"{}-{}".format(
                    storage_scenaria_config["storage_scenario_alias"],
                    storage_type,
                )

    return u"{}-{}".format(_(u"unknown"), storage_type)


def get_storage_config(params):
    """
    获取关于存储的配置项
    :param params: 配置项参数
    :return: 获取不用存储的配置项
    """
    storage_type = params["storage_type"]
    return ShipperFactory.get_shipper_factory().get_shipper(storage_type).get_storage_config(params)


class DataBusHandler(object):
    """
    DataBus处理器
    """

    def __init__(self, raw_data_id=None):
        self.raw_data_id = raw_data_id

    def clean_list(self):
        result = get_clean_list_using_data_id(self.raw_data_id)
        return result

    @classmethod
    def clean_create(cls, param):
        result = clean.create_clean_config(param)
        return result

    @classmethod
    def start_task(cls, result_table_id, storages=[]):
        storage_task.create_storage_task(result_table_id, storages)

    @classmethod
    def stop_task(cls, result_table_id, storages=[]):
        delete_task(result_table_id, storages)


@shared_task(queue="databusapi")
def execute_task(params):
    BkLanguage.set_language(BkLanguage.EN)
    logger.info(u"start execute task: create or update rt storage and restart shipper task")
    # 启动入库任务,需要异步,由于tspider接口耗时很长
    try:
        shipper_task = TASK_FACTORY.get_task_by_storage_type(params["storage_type"])(
            result_table_id=params["result_table_id"], bk_biz_id=params["bk_biz_id"]
        )
        shipper_task.start()
        status = DataBusTaskStatus.STARTED
        content = "task start up successfully"
        error_msg = ""
    except ApiRequestError as e:
        status = DataBusTaskStatus.FAILED
        content = "task start up exception"
        error_msg = str(e)
        logger.error(u"start task exception: %s" % str(e))

    # 更新入库
    model_manager.update_storage_config(
        {
            "raw_data_id": params["raw_data_id"],
            "data_type": params["data_type"],
            "result_table_id": params["result_table_id"],
            "cluster_type": params["storage_type"],
            "cluster_name": params["storage_cluster"],
            "expires": params["expires"],
            "updated_by": params["bk_username"],
            "status": status,
        }
    )
    add_task_log(
        "add_storage",
        params["storage_type"],
        params["result_table_id"],
        params,
        content,
        error_msg,
        params["bk_username"],
    )


class ShipperTask(object):
    """
    该存储节点对应的抽象任务类构造简单，启动前无需建库建表，直接启动数据分发进程
    """

    storage_type = None

    def __init__(self, result_table_id):
        self.result_table_id = result_table_id
        if self.storage_type.lower() == "eslog":
            self.storages = ["es"]
        else:
            self.storages = [self.storage_type.lower()]

    def get_storage_type(self):
        return self.storage_type

    def start(self):
        self.start_before()
        self.start_inner()

    def stop(self):
        self.stop_before()
        self.stop_inner()

    def start_before(self):
        pass

    def stop_before(self):
        pass

    def start_inner(self):
        logger.info(u"启动%s分发进程" % self.get_storage_type())
        DataBusHandler.start_task(self.result_table_id, self.storages)

    def stop_inner(self):
        logger.info(u"停止%s分发进程" % self.get_storage_type())
        DataBusHandler.stop_task(self.result_table_id, self.storages)


class DruidTask(ShipperTask):
    """
    Druid 任务启动主流程
    """

    storage_type = "druid"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(DruidTask, self).__init__(result_table_id)


class HdfsTask(ShipperTask):
    """
    Hdfs 任务启动主流程
    """

    storage_type = "hdfs"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(HdfsTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"prepare rt-associated hdfs storage")
        api_params = {"result_table_id": self.result_table_id}
        res = StoreKitApi.hdfs.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi hdfs prepare error, message: %s" % res.message)
            raise ApiRequestError(message=res.message)


class QueueTask(ShipperTask):
    """
    queue任务启动主流程
    """

    storage_type = "queue"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(QueueTask, self).__init__(result_table_id)


class TredisTask(ShipperTask):
    """
    tredis任务启动主流程
    """

    storage_type = "tredis"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(TredisTask, self).__init__(result_table_id)


class ESTask(ShipperTask):
    """
    ES任务启动主流程
    """

    storage_type = "es"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(ESTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"创建结果表 ES 索引")
        api_params = {"result_table_id": self.result_table_id}
        res = StoreKitApi.es.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi es create_index error,message:%s" % res.message)
            raise ApiRequestError(message=res.message)


class HermesTask(ShipperTask):
    """
    hermes任务启动主流程
    """

    storage_type = "hermes"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(HermesTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"创建 Hermes 表")
        res = StoreKitApi.hermes.prepare({"result_table_id": self.result_table_id})
        if not res.is_success():
            logger.error("StoreKitApi hermes create_table error,message:%s" % res.message)
            raise ApiRequestError(message=res.message)


class MysqlTask(ShipperTask):
    """
    mysql任务启动主流程
    """

    storage_type = "mysql"

    def __init__(self, result_table_id, bk_biz_id):
        res = StoreKitApi.all_result_tables.physical_table_name({"result_table_id": result_table_id})
        if not res.is_success():
            self.db_name = "mapleleaf_%s" % bk_biz_id
            logger.error("StoreKitApi storage_result_tables get_physical_table_name error,message:%s" % res.message)
        else:
            self.db_name = res.data[self.storage_type.lower()].split(".")[0]
        super(MysqlTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"创建 MySQL 数据库")
        api_params = {"result_table_id": self.result_table_id}
        res = StoreKitApi.mysql.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi mysql create db or table error,message: %s" % res.message)
            raise ApiRequestError(message=res.message)


class IgniteTask(ShipperTask):
    """
    ignite任务启动主流程
    """

    storage_type = "ignite"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(IgniteTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"ignite prepare...")
        api_params = {"result_table_id": self.result_table_id}
        res = StoreKitApi.ignite.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi ignite prepare error,message: %s" % res.message)
            raise ApiRequestError(message=res.message)


class IcebergTask(ShipperTask):
    """
    iceberg任务启动主流程
    """

    storage_type = "iceberg"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(IcebergTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"iceberg prepare...")
        api_params = {"result_table_id": self.result_table_id}
        res = StoreKitApi.iceberg.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi iceberg prepare error,message: %s" % res.message)
            raise ApiRequestError(message=res.message)


class TspiderTask(ShipperTask):
    """
    tspider任务启动主流程
    """

    storage_type = "tspider"

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(TspiderTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"创建 Tspider 数据库表")
        api_params = {"result_table_id": self.result_table_id}
        res = StoreKitApi.tspider.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi tspider create db or table error,message: %s" % res.message)
            raise ApiRequestError(message=res.message)


class ClickhouseTask(ShipperTask):
    """
    clickhouse任务启动主流程
    """

    storage_type = CLICKHOUSE

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(ClickhouseTask, self).__init__(result_table_id)

    def start_before(self):
        logger.info(u"创建 Clickhouse 数据库表")
        api_params = {"result_table_id": self.result_table_id}
        res = StoreKitApi.clickhouse.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi clickhouse create db or table error,message: %s" % res.message)
            raise ApiRequestError(message=res.message)


class TcaplusTask(ShipperTask):
    """
    Tcaplus任务启动主流程
    """

    storage_type = TCAPLUS

    def __init__(self, result_table_id, bk_biz_id):
        self.bk_biz_id = bk_biz_id
        super(TcaplusTask, self).__init__(result_table_id)

    def start_before(self):
        api_params = {RESULT_TABLE_ID: self.result_table_id}
        res = StoreKitApi.tcaplus.prepare(api_params)
        if not res.is_success():
            logger.error("StoreKitApi tcaplus prepare error,message: %s" % res.message)
            raise ApiRequestError(message=res.message)


class ShipperTaskFactory(object):
    """
    任务工厂类
    """

    Task_MAP = {}

    def __init__(self):
        pass

    @classmethod
    def register_task_class(cls, storage_type, task_class):
        cls.Task_MAP[storage_type.lower()] = task_class

    @classmethod
    def get_task_by_storage_type(cls, storage_type):
        try:
            return TASK_FACTORY.Task_MAP[storage_type.lower()]
        except KeyError:
            raise NotFoundTaskClassError()

    @property
    def registered_tasks(self):
        return self.Task_MAP


TASK_FACTORY = ShipperTaskFactory()

TASK_LIST = [
    DruidTask,
    HdfsTask,
    QueueTask,
    TredisTask,
    ESTask,
    MysqlTask,
    TspiderTask,
    HermesTask,
    IgniteTask,
    IcebergTask,
    TcaplusTask,
    ClickhouseTask,
]

for _task in TASK_LIST:
    TASK_FACTORY.register_task_class(_task.storage_type, _task)


def create_or_update_data_transferrings(result_table_id, storage_type):
    """
    清洗后入库, 创建DT
    :param result_table_id: result_table_id
    :param storage_type: 存储类型
    """
    # 重新获取RT信息, 此时会包含存储信息
    rt_info = rt.get_databus_rt_info(result_table_id)
    if not rt_info:
        raise NotFoundRtError()

    # 新增DP
    channel_id = rt_info[CHANNEL_CLUSTER_INDEX]
    storage_id = rt_info[storage_type][ID]

    transferring_id = "{}_{}".format(storage_type, result_table_id)
    processing_params = {
        BK_USERNAME: get_request_username(),
        PROJECT_ID: settings.CLEAN_PROJECT_ID,
        TRANSFERRING_ID: transferring_id,
        TRANSFERRING_ALIAS: "{}_{}".format(SHIPPER, result_table_id),
        TRANSFERRING_TYPE: SHIPPER,
        GENERATE_TYPE: USER,
        DESCRIPTION: SHIPPER,
        TAGS: [rt_info[GEOG_AREA]],
        INPUTS: [
            {
                DATA_SET_TYPE: RESULT_TABLE,
                DATA_SET_ID: result_table_id,
                STORAGE_CLUSTER_CONFIG_ID: None,
                CHANNEL_CLUSTER_CONFIG_ID: channel_id,
                STORAGE_TYPE: CHANNEL,
            }
        ],
        OUTPUTS: [
            {
                DATA_SET_TYPE: RESULT_TABLE,
                DATA_SET_ID: result_table_id,
                STORAGE_CLUSTER_CONFIG_ID: storage_id,
                CHANNEL_CLUSTER_CONFIG_ID: None,
                STORAGE_TYPE: STORAGE,
            }
        ],
    }
    respond = MetaApi.data_transferrings.retrieve({TRANSFERRING_ID: transferring_id}, raise_exception=True)
    if not respond.data:
        MetaApi.data_transferrings.create(processing_params, raise_exception=True)
    else:
        MetaApi.data_transferrings.update(processing_params, raise_exception=True)


def delete_data_transferrings(result_table_id, storage_type):
    """
    删除DT, DT删除必须成功, 否则再次创建的时候会报错
    :param result_table_id: result_table_id
    :param storage_type: 存储类型
    """
    transferring_id = "{}_{}".format(storage_type, result_table_id)
    res = MetaApi.data_transferrings.delete({TRANSFERRING_ID: transferring_id})
    if not res.is_success():
        # 1521050:不存在, 忽略
        if res.code == settings.METAAPI_NOT_EXIST_CODE:
            logger.warning("%s not exist" % transferring_id)
            return
        logger.error("delete {} failed, {}".format(transferring_id, res.errors))
        raise MetaDeleteDTError(message_kv={MESSAGE: res.message})


def check_has_storage_config(raw_data_id, result_table_id, storage_type):
    """
    :param raw_data_id: raw_data_id
    :param result_table_id: result_table_id
    :param storage_type: storage_type
    """
    # 检查是否重复入库相同存储
    storage_config = model_manager.get_storage_config_by_cond(
        raw_data_id=raw_data_id,
        result_table_id=result_table_id,
        storage_type=storage_type,
    )
    if storage_config:
        logger.error(u"data_storage has exist, please check")
        raise exceptions.ExistDataStorageConfError()

    return True


def query_storage_config(raw_data_id, result_table_id, storage_type):
    """
    :param raw_data_id: raw_data_id
    :param result_table_id: result_table_id
    :param storage_type: storage_type
    """
    storage_config = model_manager.get_storage_config_by_cond(
        raw_data_id=raw_data_id,
        result_table_id=result_table_id,
        storage_type=storage_type,
    )
    if not storage_config:
        logger.error(u"data storage config does not exist, please check")
        raise exceptions.NotFoundStorageConfError()

    return storage_config


def sync_gse_channel(cluster_type, dimension_type, dimension_name):
    """
    :param cluster_type: 集群类型
    :param dimension_name: 路由名称
    :param dimension_type: 路由类型
    """
    # 同步队列信息到zk上
    param = {
        RAW_DATA_ID: dimension_name,
        CLUSTER_TYPE: cluster_type,
        DIMENSION_NAME: dimension_name,
        DIMENSION_TYPE: dimension_type,
        BK_USERNAME: get_request_username(),
    }
    add_storage_channel(dimension_name, param, get_request_username())
    return True
