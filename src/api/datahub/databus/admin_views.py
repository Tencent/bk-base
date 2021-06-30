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

from common.decorators import list_route
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from conf import dataapi_settings
from datahub.common.const import CONF_KEY, CONF_VALUE, DESCRIPTION, TOPIC_NAME
from datahub.databus.api import MetaApi
from datahub.databus.exceptions import JsonFormatError
from datahub.databus.models import (
    AccessRawData,
    DatabusChannel,
    DatabusCluster,
    DatabusConfig,
    StorageResultTable,
)
from datahub.databus.serializers import DatabusConfigSerializer
from django.forms import model_to_dict
from rest_framework.response import Response

from datahub.databus import model_manager
from datahub.databus import tag as meta_tag

default_tag = dataapi_settings.DEFAULT_GEOG_AREA_TAG


class AdminViewSet(APIViewSet):
    """ """

    @list_route(methods=["get"], url_path="search_topic")
    def search_topic(self, request):
        """
        @api {get} /admin/search_topic/ 根据dataid或者result_table_id查询topic列表
        @apiGroup Admin
        @apiDescription  根据dataid或者result_table_id查询topic列表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """

        page = int(request.GET.get("page", 0))
        page_size = int(request.GET.get("page_size", 0))
        params = request.GET.get("params", "")
        channel_id = request.GET.get("channel_id")

        if channel_id is not None:
            channel_id = int(channel_id)

        channels = DatabusChannel.objects.all()

        channel_id2name = {}
        for channel in channels:
            channel_id2name[channel.id] = channel.cluster_name

        if not params:
            # 根据集群过滤
            result = _filter_by_cluster(channel_id, channel_id2name)
        else:
            # 全局过滤（不限定集群）
            result = _get_all_cluster(params, channel_id, channel_id2name)

        page_total = len(result)
        if page != 0:
            result.sort(key=lambda a: hash(a["data_set"]))
            result = result[(page - 1) * page_size : page * page_size]
        else:
            page = 1

        return Response({"page": page, "page_total": page_total, "topics": result})

    @list_route(methods=["get"], url_path="create_connector_tag")
    def create_connector_tag(self, request):
        """
        @api {get} /admin/create_connector_tag/ 创建connector的tag
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """

        target_id = int(request.GET.get("id", 0))
        tag = request.GET.get("tag", "")
        meta_tag.create_connector_tag(target_id, [tag])

        return Response("ok")

    @list_route(methods=["get"], url_path="create_channel_tag")
    def create_channel_tag(self, request):
        """
        @api {get} /admin/create_channel_tag/ 创建channel的tag
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """

        target_id = int(request.GET.get("id", 0))
        tag = request.GET.get("tag", "")
        meta_tag.create_channel_tag(target_id, [tag])

        return Response("ok")

    @list_route(methods=["get"], url_path="delete_connector_tag")
    def delete_connector_tag(self, request):
        """
        @api {get} /admin/delete_connector_tag/ 删除connector的tag
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """

        target_id = int(request.GET.get("id", 0))
        tag = request.GET.get("tag", "")
        meta_tag.delete_connector_tag(target_id, [tag])

        return Response("ok")

    @list_route(methods=["get"], url_path="delete_channel_tag")
    def delete_channel_tag(self, request):
        """
        @api {get} /admin/delete_channel_tag/ 删除channel的tag
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """

        target_id = int(request.GET.get("id", 0))
        tag = request.GET.get("tag", "")
        meta_tag.delete_channel_tag(target_id, [tag])

        return Response("ok")

    @list_route(methods=["get"], url_path="init_channel_role")
    def init_channel_role(self, request):
        """
        @api {get} /admin/init_channel_role/ 给channel打角色标签，outer和op
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        meta_tag.init_channel_role()
        return Response("ok")

    @list_route(methods=["get"], url_path="init_all_tags")
    def init_all_tags(self, request):
        """
        @api {get} /admin/init_all_tags/ 给channel和connector所有节点打默认的tag
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        # 更新存储和connector的tag
        channels = DatabusChannel.objects.all().values()
        for obj in channels:
            meta_tag.create_channel_tag(obj["id"], [default_tag])

        connectors = DatabusCluster.objects.all().values()
        for obj in connectors:
            meta_tag.create_connector_tag(obj["id"], [default_tag])

        # op tags
        tag = "op"
        for obj in channels:
            if obj["cluster_role"] == tag:
                meta_tag.create_channel_tag(obj["id"], [tag])

        # outer tags
        tag = "outer"
        for obj in channels:
            if obj["cluster_role"] == tag:
                meta_tag.create_channel_tag(obj["id"], [tag])

        return Response("ok")

    @list_route(methods=["get"], url_path="conf_value")
    def conf_value(self, request):
        """
        @api {get} v3/databus/admin/json_conf_value/ 获取配置项的值，字符串
        @apiGroup Admin
        @apiDescription 获取配置项的值，字符串
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": "conf value xxx",
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        conf_key = request.GET.get(CONF_KEY, "")
        return Response(model_manager.get_databus_config_value(conf_key, ""))

    @list_route(methods=["get"], url_path="json_conf_value")
    def json_conf_value(self, request):
        """
        @api {get} v3/databus/admin/json_conf_value/ 获取json格式的配置项的值
        @apiGroup Admin
        @apiDescription 获取json格式的配置项的值
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        conf_key = request.GET.get(CONF_KEY, "")
        conf_value = model_manager.get_databus_config_value(conf_key, "{}")
        try:
            return Response(json.loads(conf_value))
        except Exception:
            logger.warning("bad json value {} for key {}".format(conf_value, conf_key))
            raise JsonFormatError()

    @list_route(methods=["post"], url_path="update_conf_value")
    def update_conf_value(self, request):
        """
        @api {POST} /v3/databus/admin/update_conf_value/ 更新或者插入配置项
        @apiGroup Admin
        @apiDescription 更新或者插入databus_config中的配置项
        @apiParam {string{小于255字符}} conf_key 配置项的key，例如：iceberg.transform.rts
        @apiParam {string} conf_value 配置项的值
        @apiParamExample {json} 参数样例:
        {
            "conf_key": "iceberg.transform.rts",
            "conf_value": "[\"rt_1\", \"rt_2\", \"rt_3\"]"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1570200",
            "result": true,
            "data": {
                "id": 1,
                "conf_key": "iceberg.transform.rts",
                "conf_value": "[\"rt_1\", \"rt_2\", \"rt_3\"]",
                "created_by": "xxx",
                "created_at": "2018-10-22 10:10:05",
                "updated_by": "xxx",
                "updated_at": "2018-10-22 10:10:05"
                "description": "xxx"
            }
        }
        """
        params = self.params_valid(serializer=DatabusConfigSerializer)
        databus_config = model_manager.get_databus_config(params[CONF_KEY])
        if databus_config:
            databus_config.conf_value = params[CONF_VALUE]
            databus_config.updated_by = get_request_username()
            databus_config.description = params.get(DESCRIPTION, databus_config.description)
            databus_config.save()
        else:
            databus_config = DatabusConfig.objects.create(
                conf_key=params[CONF_KEY],
                conf_value=params[CONF_VALUE],
                created_by=get_request_username(),
                updated_by=get_request_username(),
                description=params.get(DESCRIPTION, ""),
            )

        return Response(model_to_dict(databus_config))


def _updated_by(obj):
    if obj.get("updated_by"):
        return obj.get("updated_by")
    else:
        return obj.get("created_by")


def _updated_at(obj):
    if obj.get("updated_at"):
        return obj.get("updated_at")
    else:
        return obj.get("created_at")


def _filter_by_cluster(channel_id, channel_id2name):
    result = []
    if channel_id is not None:
        rawdata_list = AccessRawData.objects.filter(storage_channel_id=channel_id)
        rt_list = StorageResultTable.objects.filter(storage_channel_id=channel_id)
    else:
        rawdata_list = AccessRawData.objects.exclude(storage_channel_id=0)
        rt_list = StorageResultTable.objects.exclude(storage_channel_id=0)

    for rawdata in rawdata_list:
        rawdata = model_to_dict(rawdata)
        topic = rawdata[TOPIC_NAME]
        result.append(
            {
                "data_set": rawdata["id"],
                "type": "rawdata",
                "topic": topic,
                "updated_by": _updated_by(rawdata),
                "updated_at": _updated_at(rawdata),
                "channel_id": rawdata["storage_channel_id"],
                "cluster_name": channel_id2name.get(rawdata["storage_channel_id"]),
            }
        )

    for rt in rt_list:
        rt = model_to_dict(rt)
        result.append(
            {
                "data_set": rt["result_table_id"],
                "type": "result_table",
                "topic": rt["physical_table_name"],
                "updated_by": _updated_by(rt),
                "updated_at": _updated_at(rt),
                "channel_id": rt["storage_channel_id"],
                "cluster_name": channel_id2name.get(rt["storage_channel_id"]),
            }
        )
    return result


def _get_all_cluster(params, channel_id, channel_id2name):
    result = []
    dataid_list = [item for item in params.split(",") if item.isdigit()]
    rtid_list = [item for item in params.split(",") if not item.isdigit()]

    if dataid_list:
        rawdata_list = AccessRawData.objects.filter(id__in=dataid_list)
        for rawdata in rawdata_list:
            rawdata = model_to_dict(rawdata)
            topic = rawdata["topic_name"]
            if channel_id is not None and rawdata["storage_channel_id"] != channel_id:
                continue

            result.append(
                {
                    "data_set": rawdata["id"],
                    "type": "rawdata",
                    "topic": topic,
                    "updated_by": _updated_by(rawdata),
                    "updated_at": _updated_at(rawdata),
                    "channel_id": rawdata["storage_channel_id"],
                    "cluster_name": channel_id2name.get(rawdata["storage_channel_id"]),
                }
            )

    if rtid_list:
        ret = MetaApi.result_tables.list({"result_table_ids": rtid_list, "related": "storages"})

        for rt in ret.data:
            if "kafka" in rt["storages"]:
                kafka_config = rt["storages"]["kafka"]

                if channel_id is not None and kafka_config["storage_channel_id"] != channel_id:
                    continue

                result.append(
                    {
                        "data_set": rt["result_table_id"],
                        "type": "result_table",
                        "topic": kafka_config["physical_table_name"],
                        "updated_by": _updated_by(rt),
                        "updated_at": _updated_at(rt),
                        "channel_id": kafka_config["storage_channel_id"],
                        "cluster_name": channel_id2name.get(kafka_config["storage_channel_id"]),
                    }
                )
    return result
