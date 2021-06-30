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

from common.decorators import list_route
from common.exceptions import DataAlreadyExistError, DataNotFoundError
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from datahub.common.const import (
    ACTIVE,
    AVRO,
    BKDATA,
    CHANNEL_NAME,
    CLUSTER_DOMAIN,
    CLUSTER_KAFKA_DEFAULT_PORT,
    CLUSTER_NAME,
    CLUSTER_PORT,
    CLUSTER_ROLE,
    CLUSTER_TYPE,
    CLUSTER_ZK_DEFAULT_PORT,
    DEFAULT,
    INNER,
    KAFKA,
    LIMIT,
    OUTER,
    PARTITION,
    RAW,
    STREAM_TO_ID,
    TAGS,
    TOPIC,
    TYPE,
    ZK_DOMAIN,
    ZK_PORT,
)
from datahub.databus.channel import (
    get_operator_username,
    register_channel_to_gse,
    update_channel_to_gse,
)
from datahub.databus.exceptions import NoAvailableInnerChannelError
from datahub.databus.models import DatabusChannel
from datahub.databus.serializers import (
    ChannelMessageSerializer,
    ChannelOffsetSerializer,
    ChannelSerializer,
    ChannelTailSerializer,
    InnerChannelSerializer,
)
from datahub.databus.settings import SYNC_GSE_ROUTE_DATA_USE_API
from django.forms import model_to_dict
from rest_framework.response import Response

from datahub.databus import channel, model_manager, rt, settings
from datahub.databus import tag as meta_tag


class ChannelViewset(APIViewSet):
    """
    channel相关的接口，包含创建、查看、列表等
    """

    # channel的名称，用于唯一确定一个channel
    lookup_field = "channel_name"
    serializer_class = ChannelSerializer

    def create(self, request):
        """
        @api {post} /databus/channels/ 创建总线channel配置
        @apiGroup Channel
        @apiDescription 创建总线channel配置，当前channel只能为kafka类型
        @apiParam {string{小于32字符}} cluster_name channel的集群名称，全局唯一，且只支持小写字母[a-z]。
        @apiParam {string{小于32字符}} cluster_type channel集群类型，默认为kafka。
        @apiParam {string{小于32字符}} cluster_role channel集群的逻辑分组，默认为inner，目前支持 inner/outer。
        @apiParam {string{小于128字符}} cluster_domain channel集群的域名信息。
        @apiParam {string{小于128字符}} cluster_backup_ips channel集群的ip信息，可为空。
        @apiParam {int{合法的端口号}} cluster_port 集群的端口号，默认为9092。
        @apiParam {string{小于128字符}} zk_domain channel集群连接的zk集群的地址。
        @apiParam {int{合法的端口号}} zk_port zk集群的端口号，默认为2181。
        @apiParam {string{小于128字符}} zk_root_path 连接的zk的root path，默认值为"/"。
        @apiParam {boolean{布尔值}} active 集群是否在工作中，是否有效。
        @apiParam {int{合法的正整数}} priority 优先级，值越高越会被作为新数据的存储队列。
        @apiParam {string{小于128字符}} attribute 属性，默认值为"bkdata"。
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiParam {list} [tags] 标签
        @apiError (错误码) 100102 数据已存在，无法添加。
        @apiParamExample {json} 参数样例:
        {
            "cluster_name":"testouter",
            "cluster_type":"kafka",
            "cluster_role":"outer",
            "cluster_domain":"xx.xx.xx",
            "cluster_backup_ips":"",
            "cluster_port":9092,
            "zk_domain":"xx.xx.xx",
            "zk_port":2181,
            "zk_root_path":"/kafka-test-3",
            "active":true,
            "priority":0,
            "attribute":"bkdata",
            "description":"",
            "tags":[]
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":{
                "priority":0,
                "description":"",
                "cluster_backup_ips":"",
                "cluster_domain":"xx.xx.xx",
                "cluster_type":"kafka",
                "cluster_port":"9092",
                "cluster_role":"inner",
                "created_by":"",
                "zk_domain":"xx.xx.xx",
                "cluster_name":"testouter",
                "zk_port":"2181",
                "attribute":"bkdata",
                "active":true,
                "id":1,
                "zk_root_path":"/",
                "updated_by":""
            },
            "result":true
        }
        """
        # 解析参数，获取相关字段的值，调用ORM写入
        params = self.params_valid(serializer=ChannelSerializer)
        # 先将集群注册到 gse data_route中
        if STREAM_TO_ID in params.keys() and params.get(STREAM_TO_ID):
            stream_to_id = params[STREAM_TO_ID]
        elif SYNC_GSE_ROUTE_DATA_USE_API and params.get(CLUSTER_ROLE, INNER) == OUTER:
            username = get_operator_username(get_request_username())
            stream_to_id = register_channel_to_gse(params, username)
        else:
            stream_to_id = None

        try:
            # 注册到元数据系统(写入数据库，下面的字段个数会小于等于数据库中字段，因为某些字段允许为null，不是必须插入)
            with auto_meta_sync(using=DEFAULT):
                obj = DatabusChannel.objects.create(
                    cluster_name=params[CLUSTER_NAME],
                    cluster_type=params.get(CLUSTER_TYPE, KAFKA),
                    cluster_role=params.get(CLUSTER_ROLE, INNER),
                    cluster_domain=params[CLUSTER_DOMAIN],
                    cluster_backup_ips=params.get("cluster_backup_ips", ""),
                    cluster_port=params.get(CLUSTER_PORT, CLUSTER_KAFKA_DEFAULT_PORT),
                    zk_domain=params.get(ZK_DOMAIN, ""),
                    zk_port=params.get(ZK_PORT, CLUSTER_ZK_DEFAULT_PORT),
                    zk_root_path=params.get("zk_root_path", "/"),
                    active=params.get(ACTIVE, True),
                    priority=params.get("priority", 0),
                    attribute=params.get("attribute", BKDATA),
                    created_by=get_request_username(),
                    updated_by=get_request_username(),
                    description=params.get("description", ""),
                    stream_to_id=stream_to_id,
                    storage_name=params.get("storage_name", None),
                )
            # 重新读取channel的信息，此时created_at和updated_at会有值(读取的是数据库的一条完整记录)
            obj = model_manager.get_channel_by_id(obj.id)

            # 若存在tag, 则加上tag
            tags = params.get(TAGS, [])
            if tags:
                meta_tag.create_channel_tag(obj.id, tags)

            return Response(model_to_dict(obj))
        except Exception as e:
            # meta sync 不会抛出IntegrityError异常，这里用普通异常捕获所有
            logger.exception(e, "failed to add databus channel %s!" % params[CLUSTER_NAME])
            raise DataAlreadyExistError(
                message_kv={
                    "table": "databus_channel",
                    "column": CLUSTER_NAME,
                    "value": params[CLUSTER_NAME],
                }
            )

    def retrieve(self, request, channel_name):
        """
        @api {get} /databus/channels/:channel_name/ 获取总线channel配置
        @apiGroup Channel
        @apiDescription 获取总线channel配置，支持按照名称或者id查询
        @apiError (错误码) 100101 未找到数据。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "priority":0,
                "description":"",
                "cluster_backup_ips":"",
                "cluster_domain":"xx.xx.xx",
                "cluster_type":"kafka",
                "created_at":"2018-10-23T16:00:31",
                "cluster_port":9092,
                "updated_at":"2018-10-23T16:00:31",
                "created_by":"",
                "cluster_role":"inner",
                "cluster_name":"testouter",
                "zk_domain":"xx.xx.xx",
                "zk_port":2181,
                "attribute":"bkdata",
                "active":true,
                "id":1,
                "zk_root_path":"/",
                "updated_by":""
            }
        }
        """
        obj = channel.get_channel_by_name(channel_name)
        return Response(model_to_dict(obj))

    def update(self, request, channel_name):
        """
        @api {put} /databus/channels/:channel_name/ 更新总线的channel配置
        @apiGroup Channel
        @apiDescription 更新总线的channel配置，支持通过名称或者id更新channel配置
        @apiParam {string{小于32字符}} cluster_name channel的集群名称，全局唯一，且只支持小写字母[a-z]。
        @apiParam {string{小于32字符}} cluster_type channel集群类型，默认为kafka。
        @apiParam {string{小于32字符}} cluster_role channel集群的逻辑分组，默认为inner，目前支持 inner/outer。
        @apiParam {string{小于128字符}} cluster_domain channel集群的域名信息。
        @apiParam {string{小于128字符}} cluster_backup_ips channel集群的ip信息，可为空。
        @apiParam {int{合法的端口号}} cluster_port 集群的端口号，默认为9092。
        @apiParam {string{小于128字符}} zk_domain channel集群连接的zk集群的地址。
        @apiParam {int{合法的端口号}} zk_port zk集群的端口号，默认为2181。
        @apiParam {string{小于128字符}} zk_root_path 连接的zk的root path，默认值为"/"。
        @apiParam {boolean{布尔值}} active 集群是否在工作中，是否有效。
        @apiParam {int{合法的正整数}} priority 优先级，值越高越会被作为新数据的存储队列。
        @apiParam {string{小于128字符}} attribute 属性，默认值为"bkdata"。
        @apiParam {string{小于128字符}} [description] 备注信息
        @apiError (错误码) 100101 未找到数据。
        @apiParamExample {json} 参数样例:
        {
            "cluster_name":"testouter",
            "cluster_type":"kafka",
            "cluster_role":"outer",
            "cluster_domain":"xx.xx.xx",
            "cluster_backup_ips":"",
            "cluster_port":9092,
            "zk_domain":"xx.xx.xx",
            "zk_port":2181,
            "zk_root_path":"/kafka-test-3",
            "active":true,
            "priority":0,
            "attribute":"bkdata",
            "description":""
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "priority":0,
                "description":"",
                "cluster_backup_ips":"",
                "cluster_domain":"xx.xx.xx",
                "cluster_type":"kafka",
                "created_at":"2018-10-23T16:00:31",
                "cluster_port":9092,
                "updated_at":"2018-10-23T16:00:31",
                "created_by":"",
                "cluster_role":"outer",
                "cluster_name":"testouter",
                "zk_domain":"xx.xx.xx",
                "zk_port":2181,
                "attribute":"bkdata",
                "active":true,
                "id":1,
                "zk_root_path":"/kafka-test-3",
                "updated_by":""
            }
        }
        """
        params = self.params_valid(serializer=ChannelSerializer)
        try:
            # 参数为整型时，为channel的id，通过id进行查询，否则根据channel_name进行查询
            channel_id = int(channel_name)
            obj = model_manager.get_channel_by_id(channel_id)
        except ValueError:
            obj = model_manager.get_channel_by_name(channel_name)

        if not obj:
            raise DataNotFoundError(
                message_kv={
                    "table": "databus_channel",
                    "column": "id/name",
                    "value": channel_name,
                }
            )

        stream_to_id = obj.stream_to_id if obj.stream_to_id else obj.id
        if SYNC_GSE_ROUTE_DATA_USE_API:
            if params.get(STREAM_TO_ID, None) is not None:
                # 如果update接口需要更新stream_to_id
                stream_to_id = params.get(STREAM_TO_ID)

            username = get_operator_username(get_request_username())
            update_channel_to_gse(params, username, stream_to_id)

        with auto_meta_sync(using=DEFAULT):
            obj.cluster_name = params[CLUSTER_NAME]
            obj.cluster_type = params.get(CLUSTER_TYPE, KAFKA)
            obj.cluster_role = params.get(CLUSTER_ROLE, INNER)
            obj.cluster_domain = params[CLUSTER_DOMAIN]
            obj.cluster_backup_ips = params.get("cluster_backup_ips", "")
            obj.cluster_port = params.get(CLUSTER_PORT, CLUSTER_KAFKA_DEFAULT_PORT)
            obj.zk_domain = params[ZK_DOMAIN]
            obj.zk_port = params.get(ZK_PORT, CLUSTER_ZK_DEFAULT_PORT)
            obj.zk_root_path = params.get("zk_root_path", "/")
            obj.active = params.get(ACTIVE, True)
            obj.priority = params.get("priority", 0)
            obj.attribute = params.get("attribute", BKDATA)
            obj.updated_by = get_request_username()
            obj.description = params.get("description", "")
            obj.stream_to_id = stream_to_id
            obj.storage_name = params.get("storage_name", None)
            obj.save()
        # 重新读取channel的信息，此时created_at和updated_at会有值
        obj = model_manager.get_channel_by_id(obj.id)
        return Response(model_to_dict(obj))

    def destroy(self, request, channel_name):
        """
        @api {delete} /databus/channels/:channel_name/ 删除总线channel配置
        @apiGroup Channel
        @apiDescription 删除总线channel配置，支持通过名称或者id删除
        @apiError (错误码) 100101 未找到数据。
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        obj = channel.get_channel_by_name(channel_name)
        # 将channel删除同步到元数据系统中
        with auto_meta_sync(using=DEFAULT):
            obj.delete()
        return Response(True)

    def list(self, request):
        """
        @api {get} /databus/channels/ 获取总线channel配置列表
        @apiGroup Channel
        @apiDescription 获取总线channel配置列表
        @apiParam {string} [tags] 标签列表, 包含地域标签, 角色标签等
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":[
                {
                    "priority":0,
                    "description":"",
                    "cluster_backup_ips":"",
                    "cluster_domain":"xx.xx.xx",
                    "cluster_type":"kafka",
                    "created_at":"2018-10-23T16:00:31",
                    "cluster_port":9092,
                    "updated_at":"2018-10-23T16:00:31",
                    "created_by":"",
                    "cluster_role":"inner",
                    "cluster_name":"testouter",
                    "zk_domain":"xx.xx.xx",
                    "zk_port":2181,
                    "attribute":"bkdata",
                    "active":true,
                    "id":1,
                    "zk_root_path":"/",
                    "updated_by":""
                },
                {
                    "priority":0,
                    "description":"",
                    "cluster_backup_ips":"",
                    "cluster_domain":"xx.xx.xx",
                    "cluster_type":"kafka",
                    "created_at":"2018-10-23T16:07:04",
                    "cluster_port":9092,
                    "updated_at":"2018-10-23T16:07:04",
                    "created_by":"",
                    "cluster_role":"inner",
                    "cluster_name":"testinner",
                    "zk_domain":"xx.xx.xx",
                    "zk_port":2181,
                    "attribute":"bkdata",
                    "active":true,
                    "id":3,
                    "zk_root_path":"/",
                    "updated_by":""
                }
            ],
            "result":true
        }
        """
        tags = request.GET.getlist("tags", [])
        objs = DatabusChannel.objects.all().values()
        channels = {obj["id"]: obj for obj in objs}

        # 根据tags查询符合的集群
        if tags:
            channel_id_list = meta_tag.get_channels_by_tags(tags)
            result = []
            for channel_id in channel_id_list:
                if channel_id in channels:
                    result.append(channels[channel_id])
            return Response(result)

        return Response(objs)

    @list_route(methods=["get"], url_path="inner_to_use")
    def inner_to_use(self, request):
        """
        @api {get} /databus/channels/inner_to_use/ 获取当前推荐的总线channel
        @apiGroup Channel
        @apiDescription 获取当前推荐的总线channel（priority最高的inner kafka channel）
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "priority":0,
                "description":"",
                "cluster_backup_ips":"",
                "cluster_domain":"xx.xx.xx",
                "cluster_type":"kafka",
                "created_at":"2018-10-23T16:00:31",
                "cluster_port":9092,
                "updated_at":"2018-10-23T16:00:31",
                "created_by":"",
                "cluster_role":"inner",
                "cluster_name":"testouter",
                "zk_domain":"xx.xx.xx",
                "zk_port":2181,
                "attribute":"bkdata",
                "active":true,
                "id":1,
                "zk_root_path":"/",
                "updated_by":""
            }
        }
        """
        # 解析参数
        params = self.params_valid(serializer=InnerChannelSerializer)

        storages = dict()
        rt_id = params.get("result_table_id")
        # 如果存在rt_id, 获取关联存储
        rt_storages = rt.get_rt_fields_storages(rt_id)
        if rt_storages and rt_storages.get("storages"):
            storages = rt_storages.get("storages")

        params["project_id"] = rt_storages.get("project_id")
        params["bk_biz_id"] = rt_storages.get("bk_biz_id")

        # 先判断原数据该rt是否已经有关联了的channel,否则优先使用project , biz纬度来选择,然后再优先级的选择策略
        if "kafka" not in storages.keys():
            geog_area = meta_tag.get_tag_by_rt_id(rt_id)
            obj = channel.get_inner_channel_to_use(params.get("project_id"), params.get("bk_biz_id"), geog_area)
        else:
            # 则返回已经关联的kafka集群信息
            storage_channel_id = storages[KAFKA]["storage_channel_id"]
            obj = model_manager.get_channel_by_id(storage_channel_id)

        if obj:
            return Response(model_to_dict(obj))
        else:
            raise NoAvailableInnerChannelError()

    @list_route(methods=["get"], url_path="tail")
    def tail(self, request):
        """
        @api {get} /databus/channels/tail/ 获取kafka上topic里的最新几条消息
        @apiGroup Channel
        @apiDescription ！！！管理接口，请勿调用！！！获取指定kafka上topic里的最新几条消息
        @apiParam {string{小于128字符}} kafka kafka集群的地址（域名和端口，e.g. xx.xx.xx:9092）。
        @apiParam {string{小于32字符}} topic kafka topic名称。
        @apiParam {string{小于32字符}} type 消息类型，目前支持raw和avro，默认值为raw。
        @apiParam {int{0~50}} partition 分区编号，默认为0。
        @apiParam {int{正整数}} limit 显示的记录条数（至少显示一条kafka msg中的所有记录），默认值10。
        """
        params = self.params_valid(serializer=ChannelTailSerializer)
        type = params.get("type", "raw")
        partition = params.get("partition", 0)
        limit = params.get("limit", 10)
        if type == AVRO:
            result = channel.inner_kafka_tail(params[KAFKA], params[TOPIC], partition, limit)
        type = params.get(TYPE, RAW)
        partition = params.get(PARTITION, 0)
        limit = params.get(LIMIT, 10)
        cluster_type = params.get(CLUSTER_TYPE, settings.TYPE_KAFKA)
        if type == AVRO:
            result = (
                channel.inner_pulsar_tail(params[CHANNEL_NAME], params[TOPIC], partition, limit)
                if cluster_type == settings.TYPE_PULSAR
                else channel.inner_kafka_tail(params[KAFKA], params[TOPIC], partition, limit)
            )
        else:
            result = channel.outer_kafka_tail(params[KAFKA], params[TOPIC], partition, limit)
            result = (
                channel.outer_pulsar_tail(params[CHANNEL_NAME], params[TOPIC], partition, limit)
                if cluster_type == settings.TYPE_PULSAR
                else channel.outer_kafka_tail(params[KAFKA], params[TOPIC], partition, limit)
            )

        return Response(result)

    @list_route(methods=["get"], url_path="message")
    def message(self, request):
        """
        @api {get} /databus/channels/message/ 获取kafka上topic指定的offset的消息内容
        @apiGroup Channel
        @apiDescription ！！！管理接口，请勿调用！！！获取kafka上topic指定的offset的消息内容
        @apiParam {string{小于128字符}} kafka kafka集群的地址（域名和端口，e.g. xx.xx.xx:9092）。
        @apiParam {string{小于32字符}} topic kafka topic名称。
        @apiParam {string{小于32字符}} type 消息类型，目前支持raw和avro，默认值为raw。
        @apiParam {int{0~50}} partition 分区编号，默认为0。
        @apiParam {int{0和正整数}} offset 读取kafka消息的offset，默认为0。
        @apiParam {int{正整数}} count 从offset开始读取的kafka消息数量，默认为1。
        """
        params = self.params_valid(serializer=ChannelMessageSerializer)
        type = params.get("type", "raw")
        partition = params.get("partition", 0)
        offset = params.get("offset", 0)
        count = params.get("count", 1)
        if type == AVRO:
            result = channel.inner_kafka_message(params[KAFKA], params[TOPIC], partition, offset, count)
        else:
            result = channel.outer_kafka_message(params[KAFKA], params[TOPIC], partition, offset, count)

        return Response(result)

    @list_route(methods=["get"], url_path="offsets")
    def offsets(self, request):
        """
        @api {get} /databus/channels/offsets/ 获取kafka上指定的topic的offset信息
        @apiGroup Channel
        @apiDescription ！！！管理接口，请勿调用！！！获取kafka上指定的topic的offset信息
        @apiParam {string{小于128字符}} kafka kafka集群的地址（域名和端口，e.g. xx.xx.xx:9092）。
        @apiParam {string{小于32字符}} topic kafka topic名称。
        """
        params = self.params_valid(serializer=ChannelOffsetSerializer)
        result = channel.get_topic_offsets(params[KAFKA], params[TOPIC])

        return Response(result)
