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
import os
from functools import reduce

from common.auth import check_perm
from common.base_utils import model_to_dict
from common.bklanguage import bktranslates
from common.business import Business
from common.log import logger
from common.transaction import auto_meta_sync
from conf import dataapi_settings
from datahub.access import settings, tags
from datahub.access.api import AuthApi, DataManageApi, DataRouteApi, MetaApi
from datahub.access.collectors.utils.access_business import AccessBusiness
from datahub.access.exceptions import (
    AccessError,
    AcessCode,
    ApiRequestError,
    CollectorError,
    CollerctorCode,
)
from datahub.access.handlers import ExtendsHandler, PaasHandler
from datahub.access.handlers.meta import MetaHandler
from datahub.access.models import (
    AccessRawData,
    AccessScenarioConfig,
    AccessScenarioStorageChannel,
    AccessSourceConfig,
    DatabusChannel,
    DataBusClusterRouteConfig,
)
from datahub.access.serializers import serializer_all_category
from datahub.access.settings import (
    DEFAULT_KAFKA_PARTITION_NUM,
    DEFAULT_PULSAR_PARTITION_NUM,
    QUEUE_STREAM_TO_TGDP_TEMPLATE,
    RAW_DATA_MANAGER,
    STREAM_TO_TGDP_TEMPLATE,
    SYNC_GSE_ROUTE_DATA_USE_API,
    USE_V2_UNIFYTLOGC_TAG,
)
from datahub.access.utils import cache, zk_helper
from datahub.common.const import (
    BIZ,
    BIZ_ID,
    BK_APP_CODE,
    BK_BIZ_ID,
    BK_BKDATA_NAME,
    BK_USERNAME,
    CHANNEL_ID,
    CLUSTER_INDEX,
    CONDITION,
    DATA_ENCODING,
    DATA_SCENARIO,
    DATA_SET,
    DEFAULT,
    DESCRIPTION,
    DISABLE,
    EMPTY_STRING,
    ENABLE,
    FILTER_NAME_AND,
    FILTER_NAME_OR,
    FLOW_ID,
    KAFKA,
    LABEL,
    MAINTAINER,
    METADATA,
    MSG_SYSTEM,
    NAME,
    NAMESPACE,
    OPERATION,
    OPERATOR_NAME,
    OUTER,
    PARTITION,
    PLAT_NAME,
    PREASSIGNED_DATA_ID,
    PULSAR,
    RAW_DATA_ALIAS,
    RAW_DATA_ID,
    RAW_DATA_NAME,
    ROLE_ID,
    ROLE_USERS,
    ROUTE,
    SENSITIVITY,
    SERVER_ID,
    SPECIFICATION,
    STORAGE_CHANNEL_ID,
    STORAGE_PARTITIONS,
    STREAM_FILTERS,
    STREAM_TO,
    STREAM_TO_ID,
    TENANT,
    TOPIC,
    TOPIC_NAME,
    USER_IDS,
)
from datahub.databus.channel import (
    get_channel_by_storage_name,
    get_channel_by_stream_to_id,
)
from datahub.databus.exceptions import TaskConnectClusterNoFound
from datahub.databus.settings import (
    PULSAR_OUTER_NAMESPACE,
    PULSAR_OUTER_TENANT,
    TYPE_KAFKA,
    TYPE_PULSAR,
)
from datahub.databus.task.pulsar import topic as pulsar_topic
from datahub.databus.task.pulsar.topic import get_pulsar_channel_token
from datahub.databus.task.task_utils import (
    get_channel_info_by_id,
    get_channel_info_by_stream_to_id,
)
from django.db.models import Q
from django.utils.translation import ugettext as _


def create_access_raw_data(params):
    data_region, new_tags = tags.get_tags(params)
    # 获取该data_id的数据源和channel
    storage_channel_id, storage_channel = generate_raw_data_channel_id(data_region, params)
    # 创建raw_data 并同步给gse
    raw_data = create_raw_data_and_sync_to_gse(params, storage_channel_id, storage_channel)
    # pulsar类型的数据源显式创建分区topic
    if storage_channel.cluster_type == PULSAR:
        topic = raw_data.topic_name
        pulsar_topic.create_partition_topic_if_not_exist(
            storage_channel,
            topic,
            raw_data.storage_partitions,
            get_pulsar_channel_token(storage_channel),
        )
    # add tags
    tags.create_tags(settings.TARGET_TYPE_ACCESS_RAW_DATA, raw_data.id, new_tags)
    # 对新创建的dataid默认打上enable标签，使其直接走节点管理
    if settings.LOG_COLLECTOR_FORCE_USE_NODE_MAN:
        update_raw_data_unifytlogc_state(raw_data.id, ENABLE)
    # 同步权限信息
    sync_auth_info(params, raw_data)
    return raw_data.id


def update_access_raw_data(raw_data_id, params):
    data_region, new_tags = tags.get_tags(params)
    bk_username = params["bk_username"]

    try:
        raw_data = AccessRawData.objects.get(id=raw_data_id)
    except AccessRawData.DoesNotExist:
        raise AccessError(error_code=AcessCode.ACCESS_DATA_NOT_FOUND)

    if raw_data.sensitivity != params["sensitivity"]:
        logger.info("update sensitivity check for raw_data.manage_auth in update")
        check_perm(settings.RAW_DATA_MANAGE_AUTH, raw_data_id)
    # update raw_data
    raw_data.updated_by = bk_username
    raw_data.description = params[DESCRIPTION]
    raw_data.raw_data_alias = params[RAW_DATA_ALIAS]
    raw_data.data_encoding = params.get(DATA_ENCODING)
    raw_data.maintainer = params[MAINTAINER]
    raw_data.sensitivity = params[SENSITIVITY]
    raw_data.topic_name = params.get(TOPIC_NAME, raw_data.topic_name)
    with auto_meta_sync(using=DEFAULT):
        raw_data.save()

    # update tags, delete old tags and add new tags
    # query old tags
    old_tags = MetaHandler.query_target_tags_by_id(raw_data_id, settings.TARGET_TYPE_ACCESS_RAW_DATA)
    # clean old tags
    old_tag_code_list = []
    for tag_type, tag_list in old_tags.items():
        if tag_type != "manage":
            old_tag_code_list += [tag["code"] for tag in tag_list]
    old_tag_code_list.append(data_region)  # data_region 在tag的返回格式中比较特殊，需要单独处理
    tags.delete_tags(settings.TARGET_TYPE_ACCESS_RAW_DATA, raw_data_id, old_tag_code_list)
    # add new tags
    tags.create_tags(settings.TARGET_TYPE_ACCESS_RAW_DATA, raw_data_id, new_tags)

    # 同步权限信息
    update_auth_roles(raw_data_id, params[MAINTAINER])


def retrieve_access_raw_data(raw_data_id, show_display):
    obj = AccessRawData.objects.get(id=raw_data_id)
    # 查询数据管理员
    roles_res = AuthApi.roles.query_role_users({"raw_data_id": raw_data_id})
    if not roles_res.is_success():
        logger.info("query raw data role users faild, raw_data_id:%s" % raw_data_id)
        raise ApiRequestError(message=roles_res.message)

    for role in roles_res.data:
        if role.get("role_id", "") == "raw_data.manager":
            obj.maintainer = ",".join(role.get("users", ""))

    access_source_map = {}
    access_scenario_map = {}
    access_category_map = {}
    obj_dict = model_to_dict(obj)
    obj_dict["bk_biz_name"] = Business(bk_biz_id=obj_dict["bk_biz_id"]).bk_biz_name
    obj_dict["topic"] = (
        obj_dict[TOPIC_NAME] if obj_dict[TOPIC_NAME] else u"%s%d" % (obj_dict["raw_data_name"], obj_dict["bk_biz_id"])
    )
    # 查询标签
    tag_code_dict = MetaHandler.query_target_tags_by_id(obj_dict["id"], settings.TARGET_TYPE_ACCESS_RAW_DATA)
    # 提取数据源分类
    data_source_tag_list, tag_list = tags.pop_data_source_tags(tag_code_dict)
    obj_dict["tags"] = tag_list
    obj_dict["data_source_tags"] = data_source_tag_list

    if show_display:
        access_source_list = AccessSourceConfig.objects.all()
        for _access_source in access_source_list:
            access_source_map[_access_source.data_source_name] = _access_source.data_source_alias

        access_scenario_list = AccessScenarioConfig.objects.all()
        for _access_scenario in access_scenario_list:
            access_scenario_map[_access_scenario.data_scenario_name] = _access_scenario.data_scenario_alias

        # 查询meta数据分类
        resp = MetaApi.meta.list()
        if not resp.is_success():
            raise ApiRequestError(message=resp.message)

        data_category_list = resp.data
        if data_category_list:
            for _data_category in data_category_list:
                serializer_all_category(_data_category, access_category_map)

        logger.info("current data type: " % access_category_map)
        obj_dict["data_source_alias"] = bktranslates(access_source_map.get(obj.data_source, u"未知"))
        obj_dict["data_scenario_alias"] = bktranslates(access_scenario_map.get(obj.data_scenario, u"未知"))
        obj_dict["data_category_alias"] = access_category_map.get(obj.data_category, u"未知")

        if not cache.APP_INFO[obj.bk_app_code]:
            app_name = PaasHandler(obj.bk_app_code).get_app_name_by_code()
            cache.APP_INFO[obj.bk_app_code] = app_name if app_name else u"未知"

        obj_dict["bk_app_code_alias"] = cache.APP_INFO[obj.bk_app_code] if cache.APP_INFO[obj.bk_app_code] else u"未知"
    return obj_dict


def create_or_set_node(zk_hosts, node_path, node_name, data):
    """
    设置ZK节点
    :param zk_hosts: zk地址 按;分割
    :param node_path: node路径
    :param node_name: node名称
    :param data: 数据
    """
    id_node_path = os.path.join(node_path, node_name)
    zk_host_array = zk_hosts.split(";")
    for zk_host in zk_host_array:
        with zk_helper.open_zk(zk_host) as zk:
            zk_helper.ensure_dir(zk, node_path)
            if zk.exists(id_node_path):
                zk.set(id_node_path, json.dumps(data))
            else:
                zk.create(id_node_path, json.dumps(data))


def delete_zk_node(zk_hosts, node_path, node_name):
    """
    删除zk节点
    :param zk_hosts: zk地址 按;分割
    :param node_path: node路径
    :param node_name: node名称
    """
    id_node_path = os.path.join(node_path, node_name)
    zk_host_array = zk_hosts.split(";")
    for zk_host in zk_host_array:
        zk_helper.delete_node(zk_host, id_node_path)


def update_route_data_to_gse(raw_data_id, raw_data, new_storage_channel, old_storage_channel):
    """
    同步raw_data的路由信息给gse，老的逻辑是同步到zk中，新的逻辑是调用接口同步给data_route组件
    :param raw_data_id: data id
    :raw_data: raw_data信息
    :new_storage_channel 需要变更的新的channel信息
    :old_storage_channel 老的channel信息
    """
    if SYNC_GSE_ROUTE_DATA_USE_API:
        logger.info(
            "raw_data:%s channel_id change to channel_id:%d, update route data to gse."
            % (raw_data_id, raw_data.storage_channel_id)
        )
        update_raw_data_route(raw_data_id, raw_data, new_storage_channel, old_storage_channel)
    else:
        sync_zk_data_id(raw_data_id, new_storage_channel.cluster_type, raw_data)


def get_stream_to_id(storage_channel):
    """
    新注册的channel有stream_to_id字段，存量的数据stream_to_id默认为主键id
    """
    return storage_channel.stream_to_id if storage_channel.stream_to_id else storage_channel.id


def sync_zk_data_id(raw_data_id, cluster_type, data_id_obj):
    """
    同步data id信息到zk, pulsar的zk节点中多了下面几个参数TENANT: PUBLIC,NAMESPACE: DATA 并且 MSG_SYSTEM值改为7,
    :param raw_data_id: data id
    :cluster_type: kafka or pulsar
    """
    logger.info("sync zk node, raw_data_id={}, cluster_type={}".format(raw_data_id, cluster_type))

    zk_data = {
        BIZ_ID: data_id_obj.bk_biz_id,
        DATA_SET: data_id_obj.raw_data_name,
        MSG_SYSTEM: settings.CREATE_RAW_DATA_MSG_SYS_KAFKA,
        PARTITION: data_id_obj.storage_partitions,
        SERVER_ID: settings.CREATE_RAW_DATA_SERVER_ID,  # not used
        CLUSTER_INDEX: data_id_obj.storage_channel_id,
    }
    if TYPE_PULSAR == cluster_type:
        zk_data[TENANT] = PULSAR_OUTER_TENANT
        zk_data[NAMESPACE] = PULSAR_OUTER_NAMESPACE
    create_or_set_node(
        dataapi_settings.DATA_ZK_HOST,
        dataapi_settings.PREFIX_DATA_NODE_PATH,
        str(raw_data_id),
        zk_data,
    )

    # 支持多ZK集群，多版本同时存在场景
    if dataapi_settings.V3_DATA_ZK_HOST:
        create_or_set_node(
            dataapi_settings.V3_DATA_ZK_HOST,
            dataapi_settings.V3_PREFIX_DATA_NODE_PATH,
            str(raw_data_id),
            zk_data,
        )


def delete_zk_data_id(raw_data_id):
    """
    删除zk上的raw_data节点
    :param raw_data_id: data id
    """
    node_name = str(raw_data_id)
    delete_zk_node(dataapi_settings.DATA_ZK_HOST, dataapi_settings.PREFIX_DATA_NODE_PATH, node_name)
    if dataapi_settings.V3_DATA_ZK_HOST:
        delete_zk_node(
            dataapi_settings.V3_DATA_ZK_HOST,
            dataapi_settings.V3_PREFIX_DATA_NODE_PATH,
            node_name,
        )


def delete_raw_data(raw_data_id):
    """
    删除源数据信息, 同时删除相关配置
    :param raw_data_id: data id
    """
    logger.info("delete raw_data %d" % raw_data_id)
    raw_data = AccessRawData.objects.get(id=raw_data_id)

    # delete from dmonitor
    res = DataManageApi.dmonitor_alert.delete({FLOW_ID: raw_data_id})
    if not res.is_success():
        # datamanager不作为主要路径, 忽略报错
        logger.warning("delete raw_data_id:%d dmonitor config failed, %s" % (raw_data_id, res.message))

    # delete from authapi
    AuthApi.roles.update_role_users(
        {
            RAW_DATA_ID: raw_data_id,
            ROLE_USERS: [
                {
                    ROLE_ID: RAW_DATA_MANAGER,
                    USER_IDS: [],
                }
            ],
        },
        raise_exception=True,
    )

    # delete from zk, gse routeAPI那边的路由配置，可以暂不删除
    if SYNC_GSE_ROUTE_DATA_USE_API:
        data_delete_params = {
            CONDITION: {CHANNEL_ID: int(raw_data_id), PLAT_NAME: BK_BKDATA_NAME},
            OPERATION: {OPERATOR_NAME: raw_data.updated_by, "method": "all"},
        }
        ret = DataRouteApi.config_delete_route(data_delete_params)
        if not ret.is_success():
            logger.warning("DataRouteApi API failed: params:{}, ret:{}".format(data_delete_params, ret.message))
    else:
        delete_zk_data_id(raw_data_id)

    # delete from db
    with auto_meta_sync(using=DEFAULT):
        AccessRawData.objects.filter(id=raw_data_id).delete()


def set_permission(raw_data_id, permission):
    raw_data = AccessRawData.objects.get(id=raw_data_id)
    with auto_meta_sync(using=DEFAULT):
        raw_data.permission = permission
        raw_data.save()


def update_auth_roles(raw_data_id, maintainers):
    """
    更新权限信息, 非关键路径, 异常忽略
    :param raw_data_id: data id
    :param maintainers: 维护人列表
    """
    logger.info("update auth roles, raw_data_id={}, maintainers={}".format(raw_data_id, maintainers))
    sync_res = AuthApi.roles.update_role_users(
        {
            RAW_DATA_ID: raw_data_id,
            ROLE_USERS: [
                {
                    ROLE_ID: RAW_DATA_MANAGER,
                    USER_IDS: maintainers.split(","),
                }
            ],
        }
    )
    if not sync_res.is_success():
        logger.warning(
            "create error:Sync permissions failed,raw_data_id:{},reason: {}".format(raw_data_id, sync_res.message)
        )


def update_raw_data_unifytlogc_state(raw_data_id, action):
    """
    为data_id 创建或清除相关tag
    """
    if action == ENABLE:
        tags.create_tags(settings.TARGET_TYPE_ACCESS_RAW_DATA, raw_data_id, [USE_V2_UNIFYTLOGC_TAG])
    elif action == DISABLE:
        tags.delete_tags(settings.TARGET_TYPE_ACCESS_RAW_DATA, raw_data_id, [USE_V2_UNIFYTLOGC_TAG])


def create_raw_data_and_sync_to_gse(params, storage_channel_id, storage_channel):
    """
    有预分配的data_id时，只创建raw_data表
    使用gse来同步时，通过gse的data_route创建raw_data以及路由信息和raw_data_id
    旧的逻辑是，先创建raw_data，再将信息同步给到zk上
    data_route_params.route.name="stream_to_${PlatName}_ ${report_mode}_${topic_name}"
    """
    cluster_type = storage_channel.cluster_type
    # 存量channel使用storage_channel_id， 增量的使用stream_to_id
    stream_to_id = storage_channel.stream_to_id if storage_channel.stream_to_id else storage_channel_id
    # 初始化topic分区和topic_name信息

    params[TOPIC_NAME] = params.get(TOPIC_NAME, genrate_topic_name(params[RAW_DATA_NAME], params[BK_BIZ_ID]))
    params[STORAGE_PARTITIONS] = params.get(
        PARTITION,
        DEFAULT_KAFKA_PARTITION_NUM if cluster_type == KAFKA else DEFAULT_PULSAR_PARTITION_NUM,
    )

    if PREASSIGNED_DATA_ID in params.keys():
        # 已有预分配data_id时，使用该id来创建数据源
        with auto_meta_sync(using=DEFAULT):
            data = create_raw_data_use_id(params[PREASSIGNED_DATA_ID], storage_channel_id, params)
    elif SYNC_GSE_ROUTE_DATA_USE_API:
        raw_data_id = raw_data_add_report_route(
            params[BK_BIZ_ID],
            params.get(BK_USERNAME, EMPTY_STRING),
            cluster_type,
            params[RAW_DATA_NAME],
            stream_to_id,
            params.get("odm_name", None),
            params[TOPIC_NAME],
        )
        logger.info("use data_routeAPI get dataid: %d" % raw_data_id)
        # 如果使用gse data_route，则data_id从gse注册获取，创建时指定主键id
        with auto_meta_sync(using=DEFAULT):
            data = create_raw_data_use_id(raw_data_id, storage_channel_id, params)
    else:
        # 如果不使用gse data_route，则id是数据库的自增主键
        with auto_meta_sync(using=DEFAULT):
            data = create_raw_data(storage_channel_id, params)
        logger.info("Synchronize data information to gse_zk, cluster_type: %s" % cluster_type)
        sync_zk_data_id(data.id, cluster_type, data)

    return data


def raw_data_add_report_route(
    bk_biz_id,
    bk_username,
    cluster_type,
    raw_data_name,
    stream_to_id,
    odm_name,
    topic_name,
):
    """
    增加一条gse的数据链路, 用于raw_data创建时
    """
    data_route_params = {
        METADATA: {PLAT_NAME: BK_BKDATA_NAME, LABEL: {BK_BIZ_ID: bk_biz_id}},
        OPERATION: {OPERATOR_NAME: bk_username},
        ROUTE: [
            {
                NAME: STREAM_TO_TGDP_TEMPLATE % (cluster_type, raw_data_name, bk_biz_id),
                STREAM_TO: {STREAM_TO_ID: stream_to_id},
                FILTER_NAME_AND: [],
                FILTER_NAME_OR: [],
            }
        ],
        STREAM_FILTERS: [],
    }
    data_route_params = ExtendsHandler.get_params_by_scenraio(
        odm_name, data_route_params, cluster_type, raw_data_name, bk_biz_id, topic_name
    )
    ret = DataRouteApi.config_add_route(data_route_params)
    if not ret.is_success():
        logger.error("DataRouteApi API failed: params:{}, ret:{}".format(data_route_params, ret.message))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_GSE_DATA_ROUTE_FAIL,
            errors=u"config_add_route调用失败,原因:" + ret.message,
            message=_(u"config_add_route调用失败:{}").format(ret.message),
        )

    return ret.data[CHANNEL_ID]


def add_topic_info_config(stream_to_map, cluster_type, raw_data_name, bk_biz_id, topic_name, partitions=None):
    """
    给gse_route的请求参数补全 kafka、pulsar的topic信息
    """
    if cluster_type == TYPE_KAFKA:
        stream_to_map[cluster_type] = {
            TOPIC_NAME: topic_name,
            DATA_SET: raw_data_name,
            BIZ_ID: bk_biz_id,
            PARTITION: partitions if partitions else DEFAULT_KAFKA_PARTITION_NUM,
        }
    elif cluster_type == TYPE_PULSAR:
        stream_to_map[cluster_type] = {
            TOPIC_NAME: topic_name,
            DATA_SET: raw_data_name,
            BIZ_ID: bk_biz_id,
            NAMESPACE: PULSAR_OUTER_NAMESPACE,
            TENANT: PULSAR_OUTER_TENANT,
            PARTITION: partitions if partitions else DEFAULT_PULSAR_PARTITION_NUM,
        }
    else:
        logger.error("Only support kafka or pulsar cluster_type")
        raise TaskConnectClusterNoFound(message_kv={"cluster_type": "%s" % cluster_type})


def update_raw_data_route(raw_data_id, raw_data, new_storage_channel, old_storage_channel):
    """
    更新gse上数据链路的信息
    """
    # update接口中之前不存在的stream_to_id会新增，之前存在的stream_to_id会修改，不会删除
    # 因此为保证可以在pulsar和kafka channel之间切换，切换集群类型时，需要把老的路由关系删除
    if new_storage_channel.cluster_type != old_storage_channel.cluster_type:
        route_name = STREAM_TO_TGDP_TEMPLATE % (
            old_storage_channel.cluster_type,
            raw_data.raw_data_name,
            raw_data.bk_biz_id,
        )
        delete_raw_data_queue_route(raw_data_id, raw_data, route_name)

    # update 新路由关系
    stream_to_id = get_stream_to_id(new_storage_channel)
    update_params = {
        CONDITION: {CHANNEL_ID: int(raw_data_id), PLAT_NAME: BK_BKDATA_NAME},
        OPERATION: {OPERATOR_NAME: raw_data.updated_by},
        SPECIFICATION: {
            ROUTE: [
                {
                    NAME: STREAM_TO_TGDP_TEMPLATE
                    % (
                        new_storage_channel.cluster_type,
                        raw_data.raw_data_name,
                        raw_data.bk_biz_id,
                    ),
                    STREAM_TO: {STREAM_TO_ID: stream_to_id},
                }
            ]
        },
    }

    add_topic_info_config(
        update_params[SPECIFICATION][ROUTE][0][STREAM_TO],
        new_storage_channel.cluster_type,
        raw_data.raw_data_name,
        raw_data.bk_biz_id,
        raw_data.topic_name,
        raw_data.storage_partitions,
    )
    ret = DataRouteApi.config_update_route(update_params)
    if not ret.is_success():
        logger.error("DataRouteApi API failed: params: {}, ret: {}".format(update_params, ret.message))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_GSE_DATA_ROUTE_FAIL,
            errors=u"config_add_route调用失败,原因:" + ret.message,
            message=_(u"config_add_route调用失败:{}").format(ret.message),
        )


def delete_raw_data_queue_route(raw_data_id, raw_data, route_name):
    """
    删除gse上原始数据入库链路的信息
    """
    channel_id = ExtendsHandler.get_channel_id(raw_data.data_scenario, raw_data_id, raw_data)
    delete_params = {
        CONDITION: {CHANNEL_ID: channel_id, PLAT_NAME: BK_BKDATA_NAME},
        OPERATION: {OPERATOR_NAME: raw_data.updated_by, "method": "specification"},
        SPECIFICATION: {ROUTE: [route_name]},
    }
    ret = DataRouteApi.config_delete_route(delete_params)
    if not ret.is_success():
        logger.error("DataRouteApi API failed: params: {}, ret: {}".format(delete_params, ret.message))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_GSE_DATA_ROUTE_FAIL,
            errors=u"config_add_route调用失败,原因:" + ret.message,
            message=_(u"config_add_route调用失败:{}").format(ret.message),
        )


def add_queue_route(raw_data_id, username, cluster_type, raw_data, queue_stream_id):
    """
    调用gse接口，额外增加一条数据链路
    """
    logger.info("raw_data:%s channel_id change to channel_id:%d" % (raw_data_id, queue_stream_id))
    raw_data_partitions = raw_data.storage_partitions

    queue_route = {
        NAME: QUEUE_STREAM_TO_TGDP_TEMPLATE % (cluster_type, raw_data.raw_data_name, raw_data.bk_biz_id),
        STREAM_TO: {STREAM_TO_ID: queue_stream_id},
    }
    add_topic_info_config(
        queue_route[STREAM_TO],
        cluster_type,
        raw_data.raw_data_name,
        raw_data.bk_biz_id,
        raw_data.topic_name,
        raw_data_partitions,
    )
    channel_id = ExtendsHandler.get_channel_and_update(
        raw_data.data_scenario, raw_data_id, raw_data, cluster_type, queue_route
    )
    update_params = {
        CONDITION: {
            CHANNEL_ID: channel_id,
            PLAT_NAME: ExtendsHandler.get_scenario_plat_name(raw_data.data_scenario),
        },
        OPERATION: {OPERATOR_NAME: username},
        SPECIFICATION: {ROUTE: [queue_route]},
    }

    ret = DataRouteApi.config_update_route(update_params)
    if not ret.is_success():
        logger.error("DataRouteApi API failed: params: {}, ret: {}".format(update_params, ret.message))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_GSE_DATA_ROUTE_FAIL,
            errors=u"config_add_route调用失败,原因:" + ret.message,
            message=_(u"config_add_route调用失败:{}").format(ret.message),
        )


def create_raw_data_use_id(raw_data_id, storage_channel_id, params):
    raw_data = AccessRawData.objects.create(
        id=raw_data_id,
        raw_data_name=params[RAW_DATA_NAME],
        bk_biz_id=params[BK_BIZ_ID],
        data_scenario=params[DATA_SCENARIO],
        created_by=params.get(BK_USERNAME, EMPTY_STRING),
        updated_by=params.get(BK_USERNAME, EMPTY_STRING),
        description=params[DESCRIPTION],
        raw_data_alias=params[RAW_DATA_ALIAS],
        # TODO 暂时仅支持 public/private/confidential, 不支持 topsecret
        sensitivity=params.get(SENSITIVITY),
        bk_app_code=params[BK_APP_CODE],
        data_encoding=params.get(DATA_ENCODING),
        storage_channel_id=storage_channel_id,
        storage_partitions=params.get(STORAGE_PARTITIONS, settings.CREATE_RAW_DATA_ZK_DEFAULT_PART),
        maintainer=params.get(MAINTAINER, EMPTY_STRING),
        topic_name=params.get(TOPIC_NAME),
    )
    return raw_data


def create_raw_data(storage_channel_id, params):
    raw_data = AccessRawData.objects.create(
        raw_data_name=params[RAW_DATA_NAME],
        bk_biz_id=params[BK_BIZ_ID],
        data_scenario=params[DATA_SCENARIO],
        created_by=params.get(BK_USERNAME, EMPTY_STRING),
        updated_by=params.get(BK_USERNAME, EMPTY_STRING),
        description=params[DESCRIPTION],
        raw_data_alias=params[RAW_DATA_ALIAS],
        # TODO 暂时仅支持 public/private/confidential, 不支持 topsecret
        sensitivity=params.get(SENSITIVITY),
        bk_app_code=params[BK_APP_CODE],
        data_encoding=params.get(DATA_ENCODING),
        storage_channel_id=storage_channel_id,
        storage_partitions=params.get(STORAGE_PARTITIONS, settings.CREATE_RAW_DATA_ZK_DEFAULT_PART),
        maintainer=params.get(MAINTAINER, EMPTY_STRING),
    )
    return raw_data


def generate_raw_data_channel_id(data_region, params):
    # 优先使用预分配的data_id
    if PREASSIGNED_DATA_ID in params.keys():
        try:
            preassigned_data_id = params[PREASSIGNED_DATA_ID]
            gse_raw_data = query_gse_raw_data_id(preassigned_data_id, params[BK_BIZ_ID], params[BK_USERNAME])
            stream_to_id = gse_raw_data[ROUTE][0][STREAM_TO][STREAM_TO_ID]
            storage_channel = get_channel_by_stream_to_id(stream_to_id)
            topic_name = gse_raw_data[ROUTE][0][STREAM_TO][storage_channel.cluster_type][TOPIC_NAME]
            partition = gse_raw_data[ROUTE][0][STREAM_TO][storage_channel.cluster_type].get(PARTITION, 1)
            params[TOPIC_NAME] = topic_name
            params[PARTITION] = partition
            return storage_channel.id, storage_channel
        except Exception as e:
            raise CollectorError(
                error_code=CollerctorCode.OUTER_CHANNEL_ERROR,
                message=u"解析关联的channel_id失败，%s" % str(e),
            )

    # 查看是否对部分业务的某些接入场景使用单独的集群
    dimension_type = "biz_%s" % (params[DATA_SCENARIO])
    outer_route_channels = DataBusClusterRouteConfig.objects.filter(
        dimension_type=dimension_type,
        dimension_name=params[BK_BIZ_ID],
        cluster_type=OUTER,
    )
    if outer_route_channels:
        try:
            outer_channel = DatabusChannel.objects.get(cluster_name=outer_route_channels[0].cluster_name)
        except DatabusChannel.DoesNotExist:
            raise CollectorError(error_code=CollerctorCode.OUTER_CHANNEL_ERROR)

        storage_channel_id = outer_channel.id

    # 接入时，选择kafka优先按照业务来选择
    # 239, 397 对于安全审计的数据，集中存放到一个单独的集群kafka-security里
    outer_route_channels = DataBusClusterRouteConfig.objects.filter(
        dimension_type=BIZ, dimension_name=params[BK_BIZ_ID], cluster_type=OUTER
    )
    if outer_route_channels:
        try:
            outer_channel = DatabusChannel.objects.get(cluster_name=outer_route_channels[0].cluster_name)
        except DatabusChannel.DoesNotExist:
            raise CollectorError(error_code=CollerctorCode.OUTER_CHANNEL_ERROR)

        storage_channel_id = outer_channel.id
    else:
        # 如果没有指定集群
        # 获取不同场景的集群列表
        storage_channel_id_list = (
            AccessScenarioStorageChannel.objects.filter(data_scenario=params[DATA_SCENARIO])
            .values_list(STORAGE_CHANNEL_ID, flat=True)
            .order_by("-priority")
        )

        if not storage_channel_id_list:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_KAFKA_CLUSTER_ERROR)

        # 则先选择出该区域集群列表
        target_list = MetaHandler.query_tag_relation_all_target([data_region], settings.TARGET_TYPE_DATABUS_CHANNEL)
        # 确定出该区域关联该场景的集群列表，按照优先级选择汲取
        channel_ids = [channel_id for channel_id in storage_channel_id_list if str(channel_id) in target_list]
        if not channel_ids:
            # 不存在该区域的channel
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_KAFKA_CLUSTER_ERROR)

        storage_channel_id = channel_ids[0]

    storage_channel = get_channel_info_by_stream_to_id(storage_channel_id)
    return storage_channel_id, storage_channel


def query_gse_raw_data_id(raw_data_id, bk_biz_id, username):
    query_params = {
        CONDITION: {
            PLAT_NAME: "tgdp",
            "label": {
                "bk_biz_id": bk_biz_id,
                "bk_biz_name": Business(bk_biz_id=bk_biz_id).bk_biz_name,
            },
            "channel_id": raw_data_id,
        },
        "operation": {"operator_name": username},
    }
    ret = DataRouteApi.config_query_route(query_params)
    if not ret.is_success() and len(ret.data) != 1:
        logger.error("DataRouteApi API failed: params: {}, ret: {}".format(query_params, ret.message))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_GSE_DATA_ROUTE_FAIL,
            errors=u"config_add_route调用失败,原因:" + ret.message,
            message=_(u"config_add_route调用失败:{}").format(ret.message),
        )

    return ret.data[0]


def sync_auth_info(params, raw_data):
    try:
        logger.info("Sync permissions information")
        bk_username = params.get("bk_username", "")

        AuthApi.roles.update_role_users(
            {
                "raw_data_id": raw_data.id,
                "role_users": [
                    {
                        "role_id": "raw_data.manager",
                        "user_ids": params.get("maintainer", bk_username).split(","),
                    }
                ],
            },
            raise_exception=True,
        )

        logger.info("Create alarm configuration details")
        dmonitor_alert_res = DataManageApi.dmonitor_alert.create(
            {"bk_username": params.get("bk_username", ""), "flow_id": raw_data.id}
        )
        if not dmonitor_alert_res.is_success():
            # datamanager不作为主要路径, 忽略报错
            logger.warning(
                "create_raw_data_error:Failed to create alarm configuration, raw_data_id:%s, reason: %s"
                % (raw_data.id, dmonitor_alert_res.message)
            )
    except Exception as e:
        logger.error("Failed to sync information, reason: %s" % str(e))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_CREATE_DATA_ERROR,
            errors=u"同步信息失败，原因：%s" % str(e),
            message=_(u"创建数据源失败，原因: {}").format(str(e)),
        )


def genrate_topic_name(raw_data_name, bk_biz_id):
    return "%s%d" % (raw_data_name, bk_biz_id)


def get_all_scope_conditions(param, raw_data_name__icontains):
    if raw_data_name__icontains:
        id_param = param.copy()
        alias_param = param.copy()
        name_param = param.copy()

        id_param["id__icontains"] = id_param.pop("raw_data_name__icontains")
        alias_param["raw_data_alias__icontains"] = alias_param.pop("raw_data_name__icontains")
        name_param["raw_data_name__icontains"] = name_param.pop("raw_data_name__icontains")

        id_conditions = [Q(**{key: value}) for key, value in id_param.items()]

        alias_conditions = [Q(**{key: value}) for key, value in alias_param.items()]

        name_conditions = [Q(**{key: value}) for key, value in name_param.items()]
        raw_data_conditions = (
            reduce(lambda x, y: x & y, name_conditions)
            | reduce(lambda x, y: x & y, alias_conditions)
            | reduce(lambda x, y: x & y, id_conditions)
        )
    else:
        no_name_param = param.copy()
        if no_name_param:
            no_name_conditions = [Q(**{key: value}) for key, value in no_name_param.items()]
            raw_data_conditions = reduce(lambda x, y: x & y, no_name_conditions)

    return raw_data_conditions


def get_auth_conditions(param, raw_data_name__icontains, bk_biz_ids, raw_data_ids):
    if raw_data_name__icontains:
        if bk_biz_ids:
            id_param = param.copy()
            alias_param = param.copy()
            name_param = param.copy()

            id_param["id__icontains"] = id_param.pop("raw_data_name__icontains")
            alias_param["raw_data_alias__icontains"] = alias_param.pop("raw_data_name__icontains")
            name_param["raw_data_name__icontains"] = name_param.pop("raw_data_name__icontains")

            id_param["bk_biz_id__in"] = bk_biz_ids
            alias_param["bk_biz_id__in"] = bk_biz_ids
            name_param["bk_biz_id__in"] = bk_biz_ids

        if raw_data_ids:
            id_param = param.copy()
            alias_param = param.copy()
            name_param = param.copy()

            id_param["id__icontains"] = id_param.pop("raw_data_name__icontains")
            alias_param["raw_data_alias__icontains"] = alias_param.pop("raw_data_name__icontains")
            name_param["raw_data_name__icontains"] = name_param.pop("raw_data_name__icontains")

            id_param["id__in"] = raw_data_ids
            alias_param["id__in"] = raw_data_ids
            name_param["id__in"] = raw_data_ids
            id_conditions = [Q(**{key: value}) for key, value in id_param.items()]

            alias_conditions = [Q(**{key: value}) for key, value in alias_param.items()]

            name_conditions = [Q(**{key: value}) for key, value in name_param.items()]
            raw_data_conditions = (
                reduce(lambda x, y: x & y, name_conditions)
                | reduce(lambda x, y: x & y, alias_conditions)
                | reduce(lambda x, y: x & y, id_conditions)
            )
    else:
        if bk_biz_ids:
            bk_biz_ids_param = param.copy()
            bk_biz_ids_param["bk_biz_id__in"] = bk_biz_ids

        if raw_data_ids:
            raw_data_ids_param = param.copy()
            raw_data_ids_param["id__in"] = raw_data_ids
            name_conditions = [Q(**{key: value}) for key, value in raw_data_ids_param.items()]
            raw_data_conditions = reduce(lambda x, y: x & y, name_conditions)

    return raw_data_conditions


def get_query_by_conditions(biz_conditions, raw_data_conditions):
    if biz_conditions and raw_data_conditions:
        return AccessRawData.objects.filter((biz_conditions) | (raw_data_conditions))
    elif biz_conditions:
        return AccessRawData.objects.filter(biz_conditions)
    elif raw_data_conditions:
        return AccessRawData.objects.filter(raw_data_conditions)
    else:
        return AccessRawData.objects.all()


def get_data_set_by_conditions(raw_data_ids, bk_biz_ids):
    if raw_data_ids and bk_biz_ids:
        raw_data_mine_param = {"id__in": raw_data_ids, "active": 1}
        biz_mine_param = {"bk_biz_id__in": bk_biz_ids, "active": 1}
        raw_data_conditions = [Q(**{key: value}) for key, value in raw_data_mine_param.items()]
        biz_conditions = [Q(**{key: value}) for key, value in biz_mine_param.items()]
        raw_data_set = AccessRawData.objects.filter(
            reduce(lambda x, y: x & y, raw_data_conditions) | reduce(lambda x, y: x & y, biz_conditions)
        )
    elif raw_data_ids:
        raw_data_set = AccessRawData.objects.filter(id__in=raw_data_ids, active=1)
    elif bk_biz_ids:
        raw_data_set = AccessRawData.objects.filter(bk_biz_id__in=bk_biz_ids, active=1)
    else:
        raw_data_set = AccessRawData.objects.all()

    return raw_data_set


def filter_query_by_conditions(query, created_by, created_begin, created_end):
    if created_by:
        query = query.filter(created_by=created_by)
    if created_begin:
        query = query.filter(created_at__gte=created_begin)
        query = query.filter(created_at__lt=created_end)

    return query


def do_mine_show_display(result, access_source_map, access_scenario_map, access_category_map):
    logger.info("mine_list: start query source show display in db")
    access_source_list = AccessSourceConfig.objects.all()
    for _access_source in access_source_list:
        access_source_map[_access_source.data_source_name] = _access_source.data_source_alias

    logger.info("mine_list: start query scenario show display in db")
    access_scenario_list = AccessScenarioConfig.objects.all()
    for _access_scenario in access_scenario_list:
        access_scenario_map[_access_scenario.data_scenario_name] = _access_scenario.data_scenario_alias

    logger.info("mine_list: start query category show display by url")
    # 查询meta数据分类
    resp = MetaApi.meta.list()
    if not resp.is_success():
        raise ApiRequestError(message=resp.message)

    data_category_list = resp.data
    if data_category_list:
        for _data_category in data_category_list:
            serializer_all_category(_data_category, access_category_map)

    logger.info("mine_list: pend replace show display")
    for raw_data in result:
        if not cache.APP_INFO[raw_data["bk_app_code"]]:
            logger.info("mine_list: bk_app_code no cache, need query")
            app_name = PaasHandler(raw_data["bk_app_code"]).get_app_name_by_code()
            cache.APP_INFO[raw_data["bk_app_code"]] = app_name if app_name else u"未知"
        raw_data["data_source_alias"] = bktranslates(access_source_map.get(raw_data["data_source"], u"未知"))
        raw_data["data_scenario_alias"] = bktranslates(access_scenario_map.get(raw_data["data_scenario"], u"未知"))

        raw_data["data_category_alias"] = access_category_map.get(raw_data.get("data_category"), u"未知")
        raw_data["bk_app_code_alias"] = (
            cache.APP_INFO[raw_data.get("bk_app_code")] if cache.APP_INFO[raw_data.get("bk_app_code")] else u"未知"
        )

    return result


def do_list_show_display(result, access_source_map, access_scenario_map, access_category_map):
    access_source_list = AccessSourceConfig.objects.all()
    for _access_source in access_source_list:
        access_source_map[_access_source.data_source_name] = _access_source.data_source_alias

    access_scenario_list = AccessScenarioConfig.objects.all()
    for _access_scenario in access_scenario_list:
        access_scenario_map[_access_scenario.data_scenario_name] = _access_scenario.data_scenario_alias

    # 查询meta数据分类
    resp = MetaApi.meta.list()
    if not resp.is_success():
        raise ApiRequestError(message=resp.message)

    data_category_list = resp.data
    if data_category_list:
        for _data_category in data_category_list:
            serializer_all_category(_data_category, access_category_map)

    logger.info("Current data type:%s" % access_category_map)

    for raw_data in result:
        if not cache.APP_INFO[raw_data["bk_app_code"]]:
            app_name = PaasHandler(raw_data["bk_app_code"]).get_app_name_by_code()
            cache.APP_INFO[raw_data["bk_app_code"]] = app_name if app_name else u"未知"

        raw_data["data_source_alias"] = access_source_map.get(raw_data["data_source"], _(u"未知"))
        raw_data["data_scenario_alias"] = access_scenario_map.get(raw_data["data_scenario"], _(u"未知"))
        raw_data["data_category_alias"] = access_category_map.get(raw_data.get("data_category"), _(u"未知"))
        raw_data["bk_app_code_alias"] = (
            cache.APP_INFO[raw_data.get("bk_app_code")] if cache.APP_INFO[raw_data.get("bk_app_code")] else _(u"未知")
        )
    return result


def do_show_tags(result):
    raw_data_list = [raw_data["id"] for raw_data in result]
    # 查询标签
    tag_code_dict = MetaHandler.query_target_tags_by_id_list(raw_data_list, settings.TARGET_TYPE_ACCESS_RAW_DATA)
    for raw_data in result:
        # 提取数据源分类
        if raw_data["id"] in tag_code_dict:
            raw_data["data_source_tags"], raw_data["tags"] = tags.pop_data_source_tags(tag_code_dict[raw_data["id"]])
        else:
            raw_data["data_source_tags"] = list()
            raw_data["tags"] = list()

    return result


def get_raw_data_list(
    param,
    raw_data_set,
    page,
    page_size,
    name_conditions,
    alias_conditions,
    id_conditions,
    raw_data_name__icontains,
):
    if page:
        if raw_data_name__icontains:
            total_count = raw_data_set.filter(
                reduce(lambda x, y: x & y, name_conditions)
                | reduce(lambda x, y: x & y, alias_conditions)
                | reduce(lambda x, y: x & y, id_conditions)
            ).count()
            index = (page - 1) * page_size
            last_index = total_count if page * page_size > total_count else page * page_size

            raw_data_list = raw_data_set.filter(
                reduce(lambda x, y: x & y, name_conditions)
                | reduce(lambda x, y: x & y, alias_conditions)
                | reduce(lambda x, y: x & y, id_conditions)
            ).order_by("-created_at")[index:last_index]
        else:
            total_count = raw_data_set.filter(**param).count()
            index = (page - 1) * page_size
            last_index = total_count if page * page_size > total_count else page * page_size

            raw_data_list = raw_data_set.filter(**param).order_by("-created_at")[index:last_index]
    else:
        total_count = 0
        if raw_data_name__icontains:
            raw_data_list = raw_data_set.filter(
                reduce(lambda x, y: x & y, name_conditions)
                | reduce(lambda x, y: x & y, alias_conditions)
                | reduce(lambda x, y: x & y, id_conditions)
            ).order_by("-created_at")
        else:
            raw_data_list = raw_data_set.filter(**param).order_by("-created_at")

    return total_count, raw_data_list


def replenish_raw_data_info(raw_data_list):
    for raw_data in raw_data_list:
        raw_data[TOPIC] = (
            raw_data[TOPIC_NAME]
            if raw_data[TOPIC_NAME]
            else u"%s%d" % (raw_data["raw_data_name"], raw_data["bk_biz_id"])
        )
        if raw_data.get("created_at"):
            raw_data["created_at"] = raw_data.get("created_at").strftime(settings.DATA_TIME_FORMAT)
        if raw_data.get("updated_at"):
            raw_data["updated_at"] = raw_data.get("updated_at").strftime(settings.DATA_TIME_FORMAT)

    # 补充 bk_biz_name 字段
    raw_data_list = AccessBusiness.transfom_biz_name(raw_data_list)

    return raw_data_list


def raw_data_list_sort(query, page, page_size, index):
    total_count = query.count()
    last_index = total_count if page * page_size > total_count else page * page_size
    return total_count, query.order_by("-created_at")[index:last_index]


def generate_condition_param(origin_param):
    condition_param = origin_param.copy()
    condition_param["active"] = 1
    if "created_by" in condition_param:
        condition_param.pop("created_by")

    if "created_begin" in condition_param:
        condition_param.pop("created_begin")
        condition_param.pop("created_end")

    return condition_param


def add_storage_channel(raw_data_id, param, bk_username):
    raw_data = AccessRawData.objects.get(id=raw_data_id)
    rawdata_route_channels = DataBusClusterRouteConfig.objects.filter(
        dimension_type=param["dimension_type"],
        dimension_name=raw_data_id,
        cluster_type=param["cluster_type"],
    )

    if len(rawdata_route_channels) == 0:
        # 恢复只有raw_data一条链路的情况
        raw_data_storage_channel = get_channel_info_by_id(raw_data.storage_channel_id)
        route_name = settings.QUEUE_STREAM_TO_TGDP_TEMPLATE % (
            raw_data_storage_channel.cluster_type,
            raw_data.raw_data_name,
            raw_data.bk_biz_id,
        )
        delete_raw_data_queue_route(raw_data_id, raw_data, route_name)
        detail_info = "remove all gse data-channel"
    elif len(rawdata_route_channels) == 1:
        # 在raw_data一条链路基础上，增加或修改一条直接上报到Queue kafka的链路;
        cluster_name = rawdata_route_channels[0].cluster_name
        queue_storage_channel = get_channel_by_storage_name(cluster_name)
        queue_stream_to_id = get_stream_to_id(queue_storage_channel)
        add_queue_route(
            raw_data_id,
            bk_username,
            queue_storage_channel.cluster_type,
            raw_data,
            queue_stream_to_id,
        )

        detail_info = "add new data-channel, %s" % cluster_name
    else:
        raise CollectorError(
            error_code=CollerctorCode.PARAM_ERR,
            message="shoule be 0 or 1 record, now find %d record" % len(rawdata_route_channels),
        )
    return detail_info
