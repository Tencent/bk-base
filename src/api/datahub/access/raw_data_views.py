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

import datahub.access.raw_data.rawdata as rawdata
from common.auth import check_perm, perm_check
from common.auth.objects import is_sys_scopes
from common.auth.perms import UserPerm
from common.base_utils import model_to_dict
from common.business import Business
from common.decorators import detail_route, list_route
from common.exceptions import ValidationError
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from datahub.access.api import AuthApi
from datahub.access.exceptions import AccessError, AcessCode
from datahub.access.models import AccessManagerConfig, AccessRawData
from django.db.models import Q
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from ..common.const import (
    ACTION,
    DATA_ENCODING,
    DATA_SOURCE,
    DESCRIPTION,
    MAINTAINER,
    OK,
    RAW_DATA_ALIAS,
    SENSITIVITY,
    STORAGE_CHANNEL_ID,
    STORAGE_PARTITIONS,
    TOPIC,
    TOPIC_NAME,
    UPDATED_AT,
    UPDATED_BY,
)
from ..databus.task.task_utils import get_channel_info_by_id
from .collectors.factory import CollectorFactory
from .exceptions import CollectorError, CollerctorCode
from .serializers import (
    AddStorageChannelSerializer,
    DataIdSerializer,
    DataNameSerializer,
    EnableV2UnifytlogcSerializer,
    ListByParamSerializer,
    ListPageByParamSerializer,
    RawDataSerializer,
    RawDataUpdateSerializer,
    RetriveSerializer,
)
from .settings import (
    DATA_TIME_FORMAT,
    RAW_DATA_MANAGE_AUTH,
    SYNC_GSE_ROUTE_DATA_USE_API,
)


class RawDataViewSet(APIViewSet):
    lookup_field = "raw_data_id"

    # @perm_check('raw_data.create', detail=False)
    def create(self, request):
        """
        @apiGroup RawData
        @api {post} /v3/access/rawdata/ 创建RawData
        @apiDescription 申请数据ID并将数据ID同步到GSE DataServer
        @apiParam {string{唯一,小于15字符,符合正则'^[a-zA-Z][a-zA-Z0-9_]*$'}} raw_data_name 数据英文标识。英文标识在业务下唯一，
        重复创建会报错。这个字段会用来在消息队列中创建对应的channel。
        @apiParam {int{CMDB合法的业务ID}} bk_biz_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{小于15字符,中文}} raw_data_alias 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法数据来源}} data_source 数据来源
        @apiParam {string{合法字符编码来源}} data_encoding 字符编码
        @apiParam {string{合法蓝鲸APP标识}} bk_app_code 蓝鲸APP标识
        @apiParam {string{合法敏感度标识}} sensitivity 敏感度。作为数据使用权限审核的依据
        @apiParam {string{合法蓝鲸用户标识}} bk_username 用户名。用户名需要具有<code>bk_biz_id</code>业务的权限。
        @apiParam {string{合法接入场景}} data_scenario 接入场景
        @apiParam {string{小于100字符}} [data_cayegory] 数据分类
        @apiParam {string{小于100字符}} [description] 数据备注

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .
        @apiError (错误码) 1570001  <code>源数据不存在</code> .
        @apiError (错误码) 1570025  <code>同步zk异常</code> .

        @apiParamExample {json} 参数样例:
        {
            "raw_data_name":"bk_test",
            "raw_data_alias":"bk_test",
            "data_source":"svr",
            "data_encoding":"UTF-8",
            "bk_biz_id":10,
            "bk_app_code":"bk_log_search",
            "sensitivity":"private",
            "bk_username":"admin",
            "description":"desc",
            "data_scenario":"log"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
                "raw_data_id": 47
            },
            "result": true
        }
        """
        # 参数校验
        params = self.params_valid(serializer=RawDataSerializer)
        raw_data_id = rawdata.create_access_raw_data(params)
        return Response({"raw_data_id": raw_data_id})

    @perm_check("raw_data.retrieve")
    def retrieve(self, request, raw_data_id):
        """
        @apiGroup RawData
        @api {get} /v3/access/rawdata/:raw_data_id/ 获取单个实例详情
        @apiDescription  根据源数据id获取源数据信息
        @apiParam {int} raw_data_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {boolean}            [show_display] 是否显示
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/rawdata/58/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": {
                "bk_biz_id": 591,
                "created_at": "2018-10-22T16:59:12",
                "data_source": "", // 新版本不在使用这个字段
                "maintainer": "",
                "updated_by": null,
                "raw_data_name": "log_admin_test",
                "topic": "log_admin_test591",
                "sensitivity": "private",
                "storage_channel_id": null,
                "data_encoding": "UTF-8",
                "raw_data_alias": "测试log接入",
                "updated_at": null,
                "bk_app_code": "bk_data",
                "data_scenario": "log",
                "created_by": "",
                "data_category": "", // 新版本不在使用这个字段
                "id": 58,
                "description": "",
                "tags": [],
                "data_source_tags": [],
            },
            "result": true
        }
        """
        param = {
            "raw_data_id": raw_data_id,
            "show_display": 0
            if not self.request.query_params.get("show_display")
            else self.request.query_params.get("show_display"),
        }
        # 参数校验
        serializer = RetriveSerializer(data=param)
        if not serializer.is_valid():
            raise ValidationError(message=_(u"参数校验不通过:raw_data_id,show_display必须为整型"))

        show_display = True if int(param["show_display"]) > 0 else False
        try:
            obj_dict = rawdata.retrieve_access_raw_data(raw_data_id, show_display)
        except AccessRawData.DoesNotExist:
            raise AccessError(error_code=AcessCode.ACCESS_DATA_NOT_FOUND)

        return Response(obj_dict)

    @perm_check("raw_data.update")
    def partial_update(self, request, raw_data_id):
        """
        @apiGroup RawData
        @api {patch} /v3/access/rawdata/:raw_data_id/ 更新源数据
        @apiDescription  根据源数据id更新源数据
        @apiParam {int} raw_data_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{小于15字符,中文}} [raw_data_alias] 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法字符编码来源}} [data_encoding ]字符编码
        @apiParam {string{小于100字符}} [description] 数据备注
        @apiParam {string} [bk_username] 操作人

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .
        @apiError (错误码) 1570001  <code>源数据不存在</code> .
        @apiError (错误码) 1570025  <code>同步zk异常</code> .
        @apiParamExample {json} 参数样例:
        {
            "description": "服务日志aaa34"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": {
                "bk_biz_id": 591,
                "created_at": "2018-10-22T16:59:12",
                "data_source": "svr",
                "maintainer": "",
                "updated_by": null,
                "raw_data_name": "log_admin_test",
                "sensitivity": "private",
                "storage_channel_id": null,
                "data_encoding": "UTF-8",
                "raw_data_alias": "测试log接入",
                "updated_at": null,
                "bk_app_code": "bk_data",
                "data_scenario": "log",
                "created_by": "",
                "id": 58,
                "description": ""
            },
            "result": true
        }
        """
        param = {"raw_data_id": raw_data_id}

        # 参数校验
        serializer = DataIdSerializer(data=param)
        if not serializer.is_valid():
            raise ValidationError(
                message=_(u"参数校验不通过:raw_data_id不合法({})").format(raw_data_id),
                errors={"raw_data_id": [u"raw_data_id不合法"]},
            )

        request.data["updated_by"] = request.data.get("bk_username", "")

        try:
            obj = AccessRawData.objects.get(id=raw_data_id)
        except AccessRawData.DoesNotExist:
            raise AccessError(error_code=AcessCode.ACCESS_DATA_NOT_FOUND)

        need_changed_old_channel = None
        for k, v in request.data.items():
            if k in [
                RAW_DATA_ALIAS,
                DATA_SOURCE,
                DATA_ENCODING,
                UPDATED_AT,
                UPDATED_BY,
                TOPIC_NAME,
                DESCRIPTION,
                SENSITIVITY,
                MAINTAINER,
                STORAGE_PARTITIONS,
                STORAGE_CHANNEL_ID,
            ]:
                # sensitivity需要单独校验权限，raw_data.manage_auth
                if k == "sensitivity":
                    logger.info("update sensitivity check for raw_data.manage_auth in partial_update")
                    check_perm(RAW_DATA_MANAGE_AUTH, raw_data_id)

                # 更新storage_id需要向gse同步
                if k == STORAGE_CHANNEL_ID or k == STORAGE_PARTITIONS:
                    need_changed_old_channel = get_channel_info_by_id(obj.storage_channel_id)

                setattr(obj, k, v)
        # filter 不存在不抛异常
        with auto_meta_sync(using="default"):
            obj.save()

        try:
            raw_data = AccessRawData.objects.get(id=raw_data_id)
        except AccessRawData.DoesNotExist:
            raise AccessError(error_code=AcessCode.ACCESS_DATA_NOT_FOUND)

        # 同步权限信息
        try:
            if need_changed_old_channel:
                collector_factory = CollectorFactory.get_collector_factory()
                collector = collector_factory.get_collector_by_data_scenario(raw_data.data_scenario)
                collector.update_route_info(raw_data_id, raw_data, need_changed_old_channel)

            if request.data.get("maintainer"):
                bk_username = request.data.get("bk_username", "")
                sync_res = AuthApi.roles.update_role_users(
                    {
                        "raw_data_id": raw_data_id,
                        "role_users": [
                            {
                                "role_id": "raw_data.manager",
                                "user_ids": request.data.get("maintainer", bk_username).split(","),
                            }
                        ],
                    }
                )
                if not sync_res.is_success():
                    logger.warning(
                        "create_raw_data_error: Sync permissions faile, raw_data_id:%s, reason: %s"
                        % (raw_data_id, sync_res.message)
                    )

        except Exception as e:
            logger.info("Failed to sync information, reason: %s" % str(e))
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_UPDATE_DATA_ERROR,
                errors=u"同步信息失败，原因：%s" % str(e),
                message=_(u"更新数据源失败，原因: {}").format(str(e)),
            )

        return Response(model_to_dict(raw_data))

    @perm_check("raw_data.update")
    def update(self, request, raw_data_id):
        """
        @apiGroup RawData
        @api {update} /v3/access/rawdata/:raw_data_id/ 更新源数据
        @apiDescription  根据源数据id更新源数据
        @apiParam {int} raw_data_id 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{小于15字符,中文}} [raw_data_alias] 数据别名（中文名）。别名会用在数据展示，请填写可读性强的名字。
        @apiParam {string{合法字符编码来源}} [data_encoding ]字符编码
        @apiParam {string{小于100字符}} [description] 数据备注
        @apiParam {string} [bk_username] 操作人

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code> .
        @apiError (错误码) 1500405  <code>请求方法错误</code> .
        @apiError (错误码) 1570001  <code>源数据不存在</code> .
        @apiError (错误码) 1570025  <code>同步zk异常</code> .
        @apiParamExample {json} 参数样例:
        {
            "description": "服务日志aaa34"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": {
                "bk_biz_id": 591,
                "created_at": "2018-10-22T16:59:12",
                "data_source": "svr",
                "maintainer": "",
                "updated_by": null,
                "raw_data_name": "log_admin_test",
                "sensitivity": "private",
                "storage_channel_id": null,
                "data_encoding": "UTF-8",
                "raw_data_alias": "测试log接入",
                "updated_at": null,
                "bk_app_code": "bk_data",
                "data_scenario": "log",
                "created_by": "",
                "id": 58,
                "description": ""
            },
            "result": true
        }
        """
        params = self.params_valid(serializer=RawDataUpdateSerializer)
        rawdata.update_access_raw_data(raw_data_id, params)
        try:
            raw_data = AccessRawData.objects.get(id=raw_data_id)
        except AccessRawData.DoesNotExist:
            raise AccessError(error_code=AcessCode.ACCESS_DATA_NOT_FOUND)
        return Response(model_to_dict(raw_data))

    @perm_check("raw_data.delete")
    def destroy(self, request, raw_data_id):
        """
        @api {delete}  v3/access/rawdata/:raw_data_id/ 删除源数据
        @apiGroup RawData
        @apiDescription  根据源数据id删除源数据
        @apiParam {int} raw_data_id 源数据ID。
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/rawdata/58/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": "ok",
            "result": true
        }
        """
        param = {"raw_data_id": raw_data_id}
        # 参数校验
        serializer = DataIdSerializer(data=param)
        if not serializer.is_valid():
            raise ValidationError(
                message=_(u"参数校验不通过:raw_data_id不合法({})").format(raw_data_id),
                errors={"raw_data_id": [u"raw_data_id不合法"]},
            )
        rawdata.delete_raw_data(int(raw_data_id))

        return Response("ok")

    def list(self, request, bk_biz_ids=None, raw_data_ids=None):
        """
        @api {get} v3/access/rawdata/?bk_biz_id=&data_scenario=log 获取源数据列表
        @apiGroup RawData
        @apiDescription  获取源数据列表
        @apiParam {string} [raw_data_name__icontains] 源数据名称
        @apiParam {int} [raw_data_id__icontains] 源数据id
        @apiParam {int{CMDB合法的业务ID}} [bk_biz_id] 业务ID。原始数据会归属到指定的业务下，后续可使用业务ID获取原始数据列表。
        @apiParam {string{合法数据来源}} [data_source] 数据来源
        @apiParam {string{合法接入场景}} [data_scenario] 接入场景
        @apiParam {string{小于100字符}} [data_cayegory] 数据分类
        @apiParam {boolean}            [show_display] 是否显示
        @apiParam {boolean}            [show_biz_name] 是否显示, 默认为true

        @apiParam {int{大于0}}                [page] 接入场景
        @apiParam {int{小于100}}        [page_size] 数据分类

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/rawdata?data_source=svr&bk_biz_id=591&page=2&page_size=10&raw_data_name=tp&show_display=true
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "count": 12,
                "results": [
                    {
                        "bk_biz_id": 591,
                        "created_at": "2018-11-07T10:04:57",
                        "data_source": "svr",
                        "maintainer": "admin",
                        "updated_by": null,
                        "data_category_alias": "",
                        "raw_data_name": "http_test",
                        "topic": "http_test591",
                        "storage_partitions": 1,
                        "sensitivity": "private",
                        "storage_channel_id": 11,
                        "data_encoding": "UTF-8",
                        "raw_data_alias": "中文名",
                        "updated_at": "2018-11-07T14:21:28",
                        "bk_app_code": "bk_dataweb",
                        "data_scenario": "http",
                        "created_by": "admin",
                        "data_category": "",
                        "data_scenario_alias": null,
                        "data_source_alias": "svr",
                        "id": 287,
                        "description": "这里填写描述"
                    }
                ]
            },
            "result": true
        }
        """
        param = self.params_valid(serializer=ListByParamSerializer)
        param["active"] = 1

        raw_data_id__icontains = param.get("raw_data_id__icontains")
        if raw_data_id__icontains:
            param["id__icontains"] = param.pop("raw_data_id__icontains")

        page = param.get("page", None)
        page_size = param.get("page_size", None)

        show_display = param.get("show_display", None)
        show_biz_name = param.get("show_biz_name", None)

        # 支持 bk_biz_ids 目前主要是来自 mine-list 调用，按照有权限业务进行过滤
        raw_data_name__icontains = param.get("raw_data_name__icontains")
        id_param = param.copy()
        id_conditions = list()
        alias_param = param.copy()
        alias_conditions = list()
        name_conditions = list()

        if raw_data_name__icontains:
            id_param["id__icontains"] = id_param.pop("raw_data_name__icontains")
            alias_param["raw_data_alias__icontains"] = alias_param.pop("raw_data_name__icontains")

            id_conditions = [Q(**{key: value}) for key, value in id_param.items()]

            alias_conditions = [Q(**{key: value}) for key, value in alias_param.items()]

            name_conditions = [Q(**{key: value}) for key, value in param.items()]

        raw_data_set = rawdata.get_data_set_by_conditions(raw_data_ids, bk_biz_ids)
        total_count, raw_data_list = rawdata.get_raw_data_list(
            param,
            raw_data_set,
            page,
            page_size,
            name_conditions,
            alias_conditions,
            id_conditions,
            raw_data_name__icontains,
        )

        result = raw_data_list.values()
        for raw_data in result:
            raw_data[TOPIC] = (
                raw_data[TOPIC_NAME]
                if raw_data[TOPIC_NAME]
                else u"%s%d" % (raw_data["raw_data_name"], raw_data["bk_biz_id"])
            )
            if raw_data.get("created_at"):
                raw_data["created_at"] = raw_data.get("created_at").strftime(DATA_TIME_FORMAT)
            if raw_data.get("updated_at"):
                raw_data["updated_at"] = raw_data.get("updated_at").strftime(DATA_TIME_FORMAT)

        if show_display:
            result = rawdata.do_list_show_display(result)

        # 补充 bk_biz_name 字段
        if show_biz_name:
            result = Business.wrap_biz_name(result)

        if page:
            rsp = {"results": result, "count": total_count}
        else:
            rsp = result

        return Response(rsp)

    @list_route(methods=["get"], url_path="mine")
    def mine_list(self, request):
        """
        @api {get} /v3/access/rawdata/mine 我有管理权限的源数据列表
        @apiGroup RawData
        @apiDescription  必须传递 bk_username 参数，其他说明请参照普通列表接口
        """
        param = self.params_valid(serializer=ListPageByParamSerializer)
        condition_param = rawdata.generate_condition_param(param)

        raw_data_name__icontains = param.get("raw_data_name__icontains", None)
        page = param.get("page", None)
        page_size = param.get("page_size", None)
        biz_conditions = None

        action_id = request.query_params.get("action_id", "raw_data.update")
        scopes = UserPerm(get_request_username()).list_scopes(action_id)
        # 判断是否全局范围，若是，则返回全部对象
        logger.info("mine_list: start parsing query conditions and query data from db")
        if is_sys_scopes(scopes):
            raw_data_conditions = rawdata.get_all_scope_conditions(condition_param, raw_data_name__icontains)
        else:
            # 组装全部有权限的 bk_biz_ids
            bk_biz_ids = [s["bk_biz_id"] for s in scopes if s.get("bk_biz_id")]
            raw_data_ids = [s["raw_data_id"] for s in scopes if s.get("raw_data_id")]
            # 返回有权限的 raw_data 列表
            if bk_biz_ids or raw_data_ids:
                raw_data_conditions = rawdata.get_auth_conditions(
                    condition_param, raw_data_name__icontains, bk_biz_ids, raw_data_ids
                )
            else:
                return Response({"results": [], "count": 0})

        index = (page - 1) * page_size
        query = rawdata.get_query_by_conditions(biz_conditions, raw_data_conditions)
        filterd_query = rawdata.filter_query_by_conditions(
            query, param.get("created_by", None), param.get("created_begin", None), param.get("created_end", None)
        )
        total_count, sorted_query = rawdata.raw_data_list_sort(filterd_query, page, page_size, index)

        # 查询接入来源列表
        logger.info("mine_list: start cnversion standard time format,  time format: %s" % DATA_TIME_FORMAT)

        result = rawdata.replenish_raw_data_info(sorted_query.values())
        if param.get("show_display", False):
            result = rawdata.do_mine_show_display(result)

        # fill tags
        if param.get("show_tags", None):
            try:
                result = rawdata.do_show_tags(result)
            except Exception as e:
                logger.error("query tags error, %s" % e)

        if page:
            return Response({"results": result, "count": total_count})
        else:
            return Response(result)

    @list_route(methods=["get"], url_path="app_code")
    def app_code(self, request):
        """
        @api {get} v3/access/rawdata/app_code 获取bk_app_code列表
        @apiGroup RawData
        @apiDescription  查询所有源数据bk_app_code列表
        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/rawdata/app_code/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": [
                "bk_data"
            ],
            "result": true
        }
        """
        raw_data_list = AccessRawData.objects.all()

        app_codes = list()
        for item in raw_data_list:
            if item.bk_app_code not in app_codes:
                app_codes.append(item.bk_app_code)
        return Response(app_codes)

    @list_route(methods=["get"], url_path="query_by_name")
    def query_by_name(self, request):
        param = self.params_valid(serializer=DataNameSerializer)
        try:
            raw_data = AccessRawData.objects.get(raw_data_name=param["raw_data_name"], bk_biz_id=param["bk_biz_id"])
            return Response(model_to_dict(raw_data))
        except AccessRawData.DoesNotExist:
            logger.warning(
                "raw_data_name:{}, bk_biz_id:{} does not exist".format(param["raw_data_name"], param["bk_biz_id"])
            )
            raise AccessError(error_code=AcessCode.ACCESS_DATA_NOT_FOUND)

    @list_route(methods=["get"], url_path="editable_apps")
    def editable_apps(self, request):
        """
        @api {get}  /v3/access/rawdata/editable_apps/ 返回可编辑的app列表
        @apiGroup RawData
        @apiDescription  返回可编辑的app列表
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/rawdata/editable_apps/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": "data,data2",
            "result": true
        }
        """
        apps = AccessManagerConfig.objects.get(type="editable").names
        try:
            apps = apps.split(",")
            return Response(apps)
        except Exception:
            return Response([])

    @list_route(methods=["post"], url_path="enable_v2_unifytlogc")
    def enable_v2_unifytlogc(self, request):
        """
        @api {post}  /v3/access/rawdata/enable_v2_unifytlogc/ 标记使用bkunifylogbeat,并使用v1版输出格式来兼容unifytlogc采集器
        @apiGroup RawData
        @apiDescription  在业务切换为cc3.0后，标记数据源使用节点管理托管的新版bkunifylogbeat采集器
        @apiParam {list{一组需要变更的数据源id}} raw_data_list。一组需要变更的数据源id，变更时需要要求一次性变更所有相关联的data_id
        @apiParam {string{进行变动的行为}} bk_biz_id。对数据源需要进行变更的动作，包括enable和disable

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/rawdata/enable_v2_unifytlogc/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": "ok",
            "result": true
        }
        """
        param = self.params_valid(serializer=EnableV2UnifytlogcSerializer)
        # enable disable
        action = param[ACTION]
        for raw_data_id in param["raw_data_list"]:
            rawdata.update_raw_data_unifytlogc_state(raw_data_id, action)

        return Response(OK)

    @detail_route(methods=["post"], url_path="add_storage_channel")
    def add_storage_channel(self, request, raw_data_id):
        """
        @api {post}  /v3/access/rawdata/:raw_data_id/add_storage_channel/ 增加原始数据上报的链路
        @apiGroup RawData
        @apiDescription  增加原始数据到存储的链路，将原始数据上报到队列服务的kafka/pulsar
        @apiParam {Int{需要变更的数据源id}} raw_data_id。需要变更的数据源id
        @apiParam {Int{需要写入的channel_id}} channel_name。需要写入的channel_id

        @apiError (错误码) 1500001  <code>参数</code> 校验不通过.
        @apiError (错误码) 1500500  <code>服务异常</code>
        @apiParamExample {json} 参数样例:
        http://x.x.x.x:xxxx/v3/access/rawdata/:raw_data_id/add_storage_channel/
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "message": "ok",
            "code": "1500200",
            "data": "ok",
            "result": true
        }
        """
        if not SYNC_GSE_ROUTE_DATA_USE_API:
            raise AccessError(
                error_code=AcessCode.ACCESS_PARAM_ERR,
                errors="SYNC_GSE_ROUTE_DATA_USE_API未启用，暂不支持该操作",
            )

        param = self.params_valid(serializer=AddStorageChannelSerializer)
        bk_username = request.data.get("bk_username", "")
        detail_info = rawdata.add_storage_channel(raw_data_id, param, bk_username)

        return Response({"operation": OK, "message": detail_info})
