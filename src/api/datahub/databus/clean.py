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

import requests
from common.auth import check_perm
from common.base_utils import model_to_dict
from common.exceptions import BaseAPIError, DataAlreadyExistError, DataNotFoundError
from common.local import get_request_username
from common.log import logger
from datahub.databus.api import DataManageApi, MetaApi, StoreKitApi
from datahub.databus.common_helper import add_task_log, delete_task_log
from datahub.databus.exceptions import (
    CreateRtStorageError,
    NoAvailableInnerChannelError,
    RawDataNotExistsError,
    UpdateResultTableError,
)
from datahub.databus.models import DatabusClean
from datahub.databus.task import task
from django.db import IntegrityError
from django.utils.translation import ugettext_lazy as _

from datahub.databus import common_helper, model_manager
from datahub.databus import tag as meta_tag

from ..access.collectors.factory import CollectorFactory
from ..common.const import (
    BK_BIZ_ID,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    CONF,
    CREATED_BY,
    DATA,
    DESCRIPTION,
    EXPIRES,
    FIELD_ALIAS,
    FIELD_INDEX,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    IS_DIMENSION,
    MSG,
    PHYSICAL_TABLE_NAME,
    PRIORITY,
    PROJECT_ID,
    RESULT_TABLE_ID,
    RESULT_TABLE_NAME,
    RESULT_TABLE_NAME_ALIAS,
    STORAGE_CHANNEL_ID,
    STORAGE_CONFIG,
    UPDATED_BY,
)
from . import channel, exceptions, rt, settings
from .settings import CLEAN_RESTFUL_READER_URL
from .utils import ExtendsHandler


def verify_clean_conf_restful(conf, msg):
    """
    调用restful接口对清洗的配置进行检查
    :param conf: 清洗的json配置
    :param msg: 进行清洗处理的样例数据
    :return: 校验清洗规则的输出，json结构文本
    """
    request_param = {CONF: conf, MSG: msg}
    try:
        res = requests.post(CLEAN_RESTFUL_READER_URL, json=request_param)
        if res.status_code not in [200, 204]:
            logger.error(
                "request pulsar tail url failed, status: %s, text: %s",
                (res.status_code, res.text),
            )
        output = json.loads(res.text)
        return True, output[DATA]
    except Exception:
        logger.error(
            "clean restful error, url:{}, param:{}".format(CLEAN_RESTFUL_READER_URL, request_param),
            exc_info=True,
        )


def update_clean_config(node_dict):
    """
    clean清洗配置更改
    :param node_dict: 配置参数列表
    :return: 更改结果 true or false
    """
    # 对于清洗的rt，给默认加上timestamp/timestamp字段，便于后续清洗和计算等地方使用

    processing_id = node_dict["processing_id"]
    node_dict[FIELDS].append(
        {
            FIELD_NAME: "timestamp",
            FIELD_ALIAS: u"内部时间字段",
            FIELD_TYPE: "timestamp",
            IS_DIMENSION: False,
            FIELD_INDEX: 0,
        }
    )

    data = rt.get_rt_fields_storages(processing_id)
    for field in data[FIELDS]:
        for new_field in node_dict[FIELDS]:
            if new_field[FIELD_NAME] == field[FIELD_NAME]:
                new_field[DESCRIPTION] = field[DESCRIPTION]

    rt_params = {
        BK_BIZ_ID: node_dict[BK_BIZ_ID],
        PROJECT_ID: settings.CLEAN_PROJECT_ID,
        RESULT_TABLE_ID: processing_id,
        RESULT_TABLE_NAME: node_dict[RESULT_TABLE_NAME],
        RESULT_TABLE_NAME_ALIAS: node_dict[RESULT_TABLE_NAME_ALIAS],
        UPDATED_BY: get_request_username(),
        DESCRIPTION: node_dict[DESCRIPTION],
        FIELDS: node_dict[FIELDS],
    }
    add_task_log(
        "update_clean",
        "clean",
        node_dict["processing_id"],
        node_dict,
        "start update cleaning configuration",
    )

    raw_data_id = node_dict["raw_data_id"]
    raw_data = model_manager.get_raw_data_by_id(raw_data_id)
    if not raw_data:
        raise RawDataNotExistsError(message_kv={"raw_data_id": raw_data_id})

    res = MetaApi.result_tables.update(rt_params)
    if not res.is_success():
        logger.error("failed to update result table: {} {} {}".format(res.code, res.message, res.data))
        add_task_log(
            "update_clean",
            "clean",
            node_dict["processing_id"],
            node_dict,
            "update cleaning configuration failed",
            res.message,
        )
        raise UpdateResultTableError(message_kv={"message": res.message})

    # db 接入场景的字符编码统一设置为UTF-8
    if raw_data.data_scenario == "db":
        node_dict["json_config"] = set_encoding(node_dict["json_config"], "UTF-8")

    try:
        # 保存清洗配置对象
        obj = DatabusClean.objects.get(processing_id=processing_id)
        obj.raw_data_id = node_dict["raw_data_id"]
        obj.pe_config = node_dict.get("pe_config", "")
        obj.clean_config_name = node_dict.get("clean_config_name", node_dict[RESULT_TABLE_NAME_ALIAS])
        obj.clean_result_table_name = node_dict[RESULT_TABLE_NAME]
        obj.clean_result_table_name_alias = node_dict[RESULT_TABLE_NAME_ALIAS]
        obj.json_config = node_dict["json_config"]
        obj.description = node_dict[DESCRIPTION]
        obj.updated_by = get_request_username()
        obj.save()

        # 标记清洗的配置发生变化
        rt.notify_change(processing_id)
        # 兼容逻辑，停止老的总线任务
        ExtendsHandler.stop_old_databus_task(processing_id, "databus_clean")
        add_task_log(
            "update_clean",
            "clean",
            node_dict["processing_id"],
            node_dict,
            "successfully updated cleaning configuration",
        )
        storage_config_list = model_manager.get_storage_config_by_data_type_rt(
            result_table_id=processing_id, data_type="clean"
        )
        storage_config_array = []
        for data_storage_config in storage_config_list:
            storage_config_array.append(model_to_dict(data_storage_config))
        return storage_config_array
    except DatabusClean.DoesNotExist:
        add_task_log(
            "update_clean",
            "clean",
            node_dict["processing_id"],
            node_dict,
            "update cleaning configuration failed",
            _(u"找不到清洗process id: %(processing_id)s") % {"processing_id": processing_id},
        )
        raise exceptions.ProcessingNotFound(_(u"找不到清洗process id: %(processing_id)s") % {"processing_id": processing_id})


def create_clean_config(node_dict):
    # 临时方案，raise_meta_exception让meta的异常忽略，先创建出清洗，然后再联系meta补录数据; 此参数不对用户开放！只用于后台修复数据
    raise_meta_exception = node_dict.get("raise_meta_exception", True)
    try:
        # 用于rt表的kafka存储创建
        add_task_log(
            "add_clean",
            "clean",
            "{}_{}".format(node_dict[BK_BIZ_ID], node_dict[RESULT_TABLE_NAME]),
            node_dict,
            "start adding cleaning configuration",
        )
        raw_data_id = node_dict["raw_data_id"]
        raw_data = model_manager.get_raw_data_by_id(raw_data_id)
        if not raw_data:
            raise RawDataNotExistsError(message_kv={"raw_data_id": raw_data_id})

        # get tags from meta by raw_data_id
        # 当前一个dataid只有一个区域的tag
        geog_area = meta_tag.get_tag_by_raw_data_id(raw_data_id)
        tags = []  # 创建rt的时候有用到
        if geog_area:
            tags.append(geog_area)

        # get channel by tags
        # 查询meta符合tags的记录, 查询db符合过滤条件的记录, 取交集
        databus_channel = channel.get_inner_channel_to_use(None, node_dict[BK_BIZ_ID], geog_area)
        if not databus_channel:
            raise NoAvailableInnerChannelError()
        logger.info("get inner channel %s" % databus_channel.cluster_name)

        result_table_id = "{}_{}".format(node_dict[BK_BIZ_ID], node_dict[RESULT_TABLE_NAME])
        # 对于清洗的RT，给默认加上timestamp/timestamp字段，便于后续清洗和计算等地方使用
        node_dict[FIELDS].append(
            {
                FIELD_NAME: "timestamp",
                FIELD_ALIAS: u"内部时间字段",
                FIELD_TYPE: "timestamp",
                IS_DIMENSION: False,
                FIELD_INDEX: 0,
            }
        )
        rt_params = {
            BK_BIZ_ID: node_dict[BK_BIZ_ID],
            PROJECT_ID: settings.CLEAN_PROJECT_ID,
            RESULT_TABLE_ID: result_table_id,
            RESULT_TABLE_NAME: node_dict[RESULT_TABLE_NAME],
            RESULT_TABLE_NAME_ALIAS: node_dict[RESULT_TABLE_NAME_ALIAS],
            CREATED_BY: get_request_username(),
            DESCRIPTION: node_dict[DESCRIPTION],
            FIELDS: node_dict[FIELDS],
            "tags": tags,
        }
        processing_params = {
            "bk_username": get_request_username(),
            PROJECT_ID: settings.CLEAN_PROJECT_ID,
            "processing_id": result_table_id,
            "processing_alias": u"清洗任务%s" % result_table_id,
            "processing_type": settings.CLEAN_TRT_CODE,
            "generate_type": "user",
            DESCRIPTION: u"清洗任务",
            "tags": tags,
            "result_tables": [rt_params],
            "inputs": [
                {
                    "data_set_type": "raw_data",
                    "data_set_id": raw_data_id,
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": raw_data.storage_channel_id,
                    "storage_type": "channel",
                }
            ],
            "outputs": [
                {
                    "data_set_type": "result_table",
                    "data_set_id": result_table_id,
                    "storage_cluster_config_id": None,
                    "channel_cluster_config_id": databus_channel.id,
                    "storage_type": "channel",
                }
            ],
        }

        # 创建data processing, 并且创建相关的RT
        logger.info("create processing %s" % result_table_id)
        MetaApi.data_processings.create(processing_params, raise_exception=raise_meta_exception)

        # 创建RT的kafka存储
        rt_storage_params = {
            RESULT_TABLE_ID: result_table_id,
            CLUSTER_NAME: databus_channel.cluster_name,
            CLUSTER_TYPE: databus_channel.cluster_type,
            PHYSICAL_TABLE_NAME: "table_%s" % result_table_id,
            EXPIRES: "3d",
            STORAGE_CHANNEL_ID: databus_channel.id,
            STORAGE_CONFIG: "{}",
            PRIORITY: databus_channel.priority,
            CREATED_BY: get_request_username(),
            DESCRIPTION: u"清洗表存储",
        }

        # 创建RT关联kafka存储，如果失败了，则删除DP回滚
        storage_res = StoreKitApi.result_tables.create(rt_storage_params)
        if not storage_res.is_success():
            logger.error(
                "failed to create kafka storage for result table: %s %s %s"
                % (storage_res.code, storage_res.message, storage_res.data)
            )
            # 删除刚才创建的DP和RT，如果删除失败了，抛出异常人工介入处理
            MetaApi.data_processings.delete(
                {"processing_id": result_table_id, "with_data": True},
                raise_exception=raise_meta_exception,
            )
            raise CreateRtStorageError(message_kv={"message": storage_res.message})

        # db 接入场景的字符编码统一设置为UTF-8
        if raw_data.data_scenario == "db":
            node_dict["json_config"] = set_encoding(node_dict["json_config"], "UTF-8")

        try:
            # 创建清洗记录
            obj = DatabusClean.objects.create(
                processing_id=result_table_id,
                clean_config_name=node_dict.get("clean_config_name", node_dict[RESULT_TABLE_NAME_ALIAS]),
                clean_result_table_name=node_dict[RESULT_TABLE_NAME],
                clean_result_table_name_alias=node_dict[RESULT_TABLE_NAME_ALIAS],
                raw_data_id=raw_data_id,
                pe_config=node_dict.get("pe_config", ""),
                json_config=node_dict["json_config"],
                created_by=get_request_username(),
                updated_by=get_request_username(),
                description=node_dict[DESCRIPTION],
            )

            # 重新读取clean的信息，此时created_at和updated_at会有值
            obj = model_manager.get_clean_by_processing_id(obj.processing_id)
            logger.info("create clean info %s" % obj)

            # 获取清洗对应的rt的信息
            data = rt.get_rt_fields_storages(result_table_id)
            if data:
                result = model_to_dict(obj)
                add_task_log(
                    "add_clean",
                    "clean",
                    "{}_{}".format(node_dict[BK_BIZ_ID], node_dict[RESULT_TABLE_NAME]),
                    node_dict,
                    "successfully added cleaning configuration",
                )
                result["result_table_id"] = result_table_id
                result["result_table"] = data
                result["status_display"] = _(result["status"])
                return result
            else:
                raise DataNotFoundError(
                    message_kv={
                        "table": "result_table",
                        "column": "id",
                        "value": result_table_id,
                    }
                )
        except IntegrityError as e:
            logger.exception(e, "databus clean %s is already exists!" % result_table_id)
            raise DataAlreadyExistError(
                message_kv={
                    "table": "databus_clean",
                    "column": "processing_id",
                    "value": result_table_id,
                }
            )
    except BaseAPIError as task_error:
        logger.error(u"create clean conf error, message:%s" % task_error.message)
        add_task_log(
            "add_clean",
            "clean",
            "{}_{}".format(node_dict[BK_BIZ_ID], node_dict[RESULT_TABLE_NAME]),
            node_dict,
            "add cleaning configuration failed",
            task_error.message,
        )
        raise task_error


def get_json_resource_info(raw_data_id):
    access_resources = model_manager.get_raw_data_resource_by_id(raw_data_id)
    is_json, res = common_helper.is_json(access_resources[0].resource)
    if not is_json:
        raise exceptions.ResourceNotJson(message_kv={"data_id": raw_data_id})
    return res


def generate_etl_config(raw_data_id):
    raw_data = model_manager.get_raw_data_by_id(raw_data_id)
    if not raw_data:
        raise exceptions.RawDataNotExistsError(message_kv={"raw_data_id": raw_data_id})

    try:
        collector = CollectorFactory.get_collector_by_data_scenario(raw_data.data_scenario)()
        return collector.generate_etl_config(raw_data_id, raw_data)
    except Exception:
        return {}


def set_encoding(json_config, encoding):
    """
    设置清洗的编码
    :param json_config: 清洗配置字符串
    :param encoding: 编码
    :return: 清洗字符串
    """
    json_config = json.loads(json_config)
    json_config["conf"]["encoding"] = encoding
    return json.dumps(json_config)


def get_clean_list_using_data_id(raw_data_id):
    objs = DatabusClean.objects.filter(raw_data_id=raw_data_id).values()
    result = []
    if objs:
        for entry in objs:
            entry["status_display"] = _(entry["status"])  # 加上status display字段
            result.append(entry)

    return result


def delete_clean(processing_id, params):
    clean_obj = model_manager.get_clean_by_processing_id(processing_id)
    if not clean_obj:
        raise exceptions.ProcessingNotFound(_(u"找不到清洗process id: %(processing_id)s") % {"processing_id": processing_id})

    # 权限校验，需要有清洗的权限
    check_perm("result_table.manage_task", processing_id)

    # 校验是否存在下游节点
    if rt.has_next_dt_or_dp(processing_id):
        raise exceptions.NotSupportDeleteCleanErr()

    # stop task
    rt_info = rt.get_databus_rt_info(processing_id)
    task.stop_databus_task(rt_info, ["kafka"], delete=True)

    # delete topic
    rt.delete_clean_topic_by_rt(processing_id)

    # 记录删除操作
    logger.info("add delete log event, rt=%s" % processing_id)
    model_manager.log_delete_event(model_manager.OPERATION_TYPE_CLEAN, processing_id)

    add_task_log(
        "delete_clean",
        "clean",
        processing_id,
        {"processing_id": processing_id},
        "start delete cleaning configuration",
    )

    # 删除storage表中存储的信息
    logger.info("delete %s from StoreKitApi" % processing_id)
    StoreKitApi.result_tables.delete({"result_table_id": processing_id, "cluster_type": "kafka"})

    # 然后再删除result_table
    logger.info("delete %s from MetaApi" % processing_id)
    MetaApi.data_processings.delete({"processing_id": processing_id, "with_data": True})

    # 删除路由信息，删除清洗任务信息
    connector = "clean-table_%s" % processing_id
    logger.info("delete %s from databus_connector_task" % connector)
    clean_route = model_manager.get_connector_route(connector)
    if clean_route:
        clean_route.delete()

    # 删除清洗配置信息
    logger.info("delete %s from databus_clean_info" % processing_id)
    clean_obj.delete()

    # 删除数据质量指标, 非关键路径, 不抛异常
    data_quality = params["data_quality"]
    if data_quality:
        logger.info("delete data quality metrics of %s" % processing_id)
        try:
            DataManageApi.delete_data_quality_metrics(
                {
                    "data_set_id": processing_id,
                    "storage": settings.COMPONENT_KAFKA,
                }
            )
        except Exception as e:
            logger.warning("delete data quality metrics failed, %s" % e)

    # 清理task_log
    delete_task_log("clean", processing_id)
