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
from auth.audit.audit_stats import AUDIT_TYPE, Stats
from auth.core.permission import UserPermission
from auth.exceptions import NoFunctionErr, ParameterErr
from auth.handlers.object_classes import ObjectFactory, is_sys_scopes
from auth.models.base_models import ActionConfig
from auth.models.others import OperationConfig, OperationStatus
from auth.models.outer_models import AccessRawData
from auth.views.auth_serializers import (
    DgraphScopesBatchCheckSerializer,
    PermissionBatchCheckSerializer,
    PermissionCheckSerializer,
    PermissionScopesDimensionSerializer,
    PermissionScopesSerializer,
    UserHandoverSerializer,
)
from common.auth import require_indentities
from common.business import Business
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from django.core.cache import cache
from django.forms.models import model_to_dict
from django.utils.translation import ugettext as _
from gevent.pool import Pool
from rest_framework.response import Response

try:
    from auth.extend.core.tdw.backend import DEFAULT_TDW_BIZ_ID
    from auth.extend.models.tdw_models import TdwUser
except ImportError as e:
    logger.info("环境中不存在 TDW 模块：{}".format(e))
    DEFAULT_TDW_BIZ_ID = 0
    TdwUser = None


class UserPermViewSet(APIViewSet):
    lookup_field = "user_id"
    lookup_value_regex = r"[\w._-]+"

    @detail_route(methods=["GET"], url_path="scopes")
    @params_valid(PermissionScopesSerializer, add_params=False)
    def scopes(self, request, user_id):
        """
        @api {get} /auth/users/:user_id/scopes/  获取用户有权限的对象范围
        @apiName get_scopes
        @apiGroup UserPerm
        @apiParam {String} action_id 操作类型，例如 result_table.query_data
        @apiParam {Boolean} [show_admin_scopes] 是否显示管理员范围
        @apiParam {Boolean} [show_display] 是否添加名称字段
        @apiParam {Boolean} [add_tdw] 是否考虑 TDW 标准化结果表
        @apiParam {String} [tdw_filter] TDW 过滤参数
        @apiParam {Int} [page] 分页参数
        @apiParam {Int} [page_size] 分页参数
        @apiParam {Int} [bk_biz_id] 支持业务字段过滤
        @apiParamExample {json} 校验用户对结果表是否有查询数据权限
            {
                "action_id": "result_table.query_data"
            }
        @apiSuccessExample {json} object_class=result_table
            [
                {"project_id": 111},
                {"bk_biz_id": 122},
                {"*": "*"}
            ]
        @apiSuccessExample {json} object_class=project
            [
                {"project_id": 11},
                {"*": "*"}
            ]
        @apiSuccessExample {json} object_class=raw_data
            [
                {"bk_biz_id": 111},
                {"*": "*"}
            ]
        """
        action_id = request.cleaned_params["action_id"]
        show_admin_scopes = request.cleaned_params.get("show_admin_scopes", False)
        show_display = request.cleaned_params.get("show_display", False)
        add_tdw = request.cleaned_params.get("add_tdw", False)
        tdw_filter = request.cleaned_params.get("tdw_filter")
        bk_biz_id = request.cleaned_params.get("bk_biz_id")

        page = request.cleaned_params.get("page", 0)
        page_size = request.cleaned_params.get("page_size", 10)

        data = UserPermission(user_id=user_id).get_scopes_all(
            action_id, show_admin_scopes=show_admin_scopes, add_tdw=add_tdw
        )

        # 目前这段代码支持按照业务过滤，较为定制化，不推荐使用 bk_biz_id 参数进行过滤
        if bk_biz_id:
            filtered_data = []
            biz_prefix = f"{bk_biz_id}_"
            for d in data:
                if "result_table_id" in d and d["result_table_id"] and d["result_table_id"].startswith(biz_prefix):
                    filtered_data.append(d)
                elif "bk_biz_id" in d and d["bk_biz_id"] == bk_biz_id:
                    filtered_data.append(d)

            data = filtered_data

        if add_tdw and tdw_filter:
            filtered_data = []
            for d in data:
                if (
                    "result_table_id" in d
                    and d["result_table_id"]
                    and tdw_filter.lower() in d["result_table_id"].lower()
                ):
                    filtered_data.append(d)
            data = filtered_data

        if page:
            data = data[page_size * (page - 1) : page_size * page]

        if show_display:
            data = ObjectFactory.init_object(action_id).wrap_display(data)
        return Response(data)

    @staticmethod
    def list_all_biz_response():
        bizs = [
            {
                "bk_biz_id": app["bk_biz_id"],
                "bk_biz_name": app["bk_biz_name"],
            }
            for app in Business.list()
        ]
        return Response(bizs)

    @list_route(url_path="scope_dimensions")
    @params_valid(PermissionScopesDimensionSerializer, add_params=False)
    def get_scope_dimensions(self, request):
        """
        @api {get} /auth/users/scope_dimensions/  获取用户有权限的维度列表
        @apiName get_scope_dimensions
        @apiGroup UserPerm
        @apiParam {string} action_id 操作类型，例如 result_table.query_data
        @apiParam {string} dimension 维度字段，例如 bk_biz_id
        @apiParam {Boolean} [add_tdw] 是否考虑 TDW 标准化结果表
        @apiParamExample {json} 校验用户对结果表是否有查询数据权限
            {
                "action_id": "result_table.query_data",
                "dimension": "biz_id"
            }
        @apiSuccessExample {json} object_class=result_table
            [
                {"bk_biz_id": 111, "bk_biz_name": "DNF"},
                {"biz_id": 122, "bk_biz_name": "LOL"}
            ]
        """
        user_id = get_request_username()
        action_id = request.cleaned_params["action_id"]
        dimension = request.cleaned_params["dimension"]

        # todo 需要换成通用的
        valid_actions = ["result_table.query_data", "raw_data.update"]
        valid_dimensions = ["bk_biz_id"]
        if not (action_id in valid_actions and dimension in valid_dimensions):
            raise NoFunctionErr(_(f"目前 actoin_id 仅支持 {valid_actions} 和 {valid_dimensions} 维度"))

        scopes = UserPermission(user_id=user_id).get_scopes_all(action_id)
        if is_sys_scopes(scopes):
            return self.list_all_biz_response()

        bk_biz_ids = set()
        if action_id == "raw_data.update":
            raw_data_arr = AccessRawData.objects.only("bk_biz_id", "id")
            mapping = {d.id: d.bk_biz_id for d in raw_data_arr}
            for scope in scopes:
                if "raw_data_id" in scope and scope["raw_data_id"] in mapping:
                    bk_biz_ids.add(mapping[scope["raw_data_id"]])
        elif action_id == "result_table.query_data":
            for scope in scopes:
                # 投机取巧，默认 RT 表名前缀为业务名称，避免再去关联 RT 表
                if "result_table_id" in scope:
                    _result_table_id = scope["result_table_id"]
                    try:
                        bk_biz_ids.add(int(_result_table_id.split("_", 1)[0]))
                    except Exception as err:
                        logger.error(f"NotExcepted result_table_id {_result_table_id}, {err}")

            if DEFAULT_TDW_BIZ_ID != 0 and TdwUser.get_tdw_user(raise_exception=False):
                bk_biz_ids.add(DEFAULT_TDW_BIZ_ID)

        bk_biz_ids = sorted(bk_biz_ids)
        return Response(Business.wrap_biz_name([{"bk_biz_id": bk_biz_id} for bk_biz_id in bk_biz_ids]))

    @detail_route(methods=["POST"])
    @params_valid(PermissionCheckSerializer, add_params=False)
    def check(self, request, user_id):
        """
        @api {post} /auth/users/:user_id/check/  校验用户与对象权限
        @apiName check_perm
        @apiGroup UserPerm
        @apiParam {string} action_id 操作类型，例如 result_table.query_data
        @apiParam {string} object_id 对象ID
        @apiParam {boolean} [display_detail] 是否展示鉴权详细结果
        @apiParamExample {json} 校验用户对结果表是否有查询数据权限
            {
                "action_id": "result_table.query_data",
                "object_id": "100107_cpu_parse"
            }
        @apiSuccessExample {json} Succees-Response.data
            True | False
        """
        action_id = request.cleaned_params["action_id"]
        object_id = request.cleaned_params.get("object_id", None)
        display_detail = request.cleaned_params.get("display_detail", False)

        # 缓存加快返回速度
        cache_key = f"user_check:{user_id}_{action_id}_{object_id}_{display_detail}"

        cache_value = cache.get(cache_key)
        if cache_value is not None:
            logger.info(f"[USER CHECK] Hit, {cache_key}: {cache_value}")
            return Response(cache_value)

        ret = UserPermission(user_id=user_id).check(
            action_id, object_id, raise_exception=False, display_detail=display_detail
        )

        # 通过缓存时间设置长一点，不通过不加缓存
        CACHE_PASS_TIME = 600
        CACHE_DENY_TIME = 5

        logger.info(f"[USER CHECK] Direct, {cache_key}: {ret}")
        cache.set(cache_key, ret, CACHE_PASS_TIME if ret else CACHE_DENY_TIME)

        return Response(ret)

    @detail_route(methods=["GET"], url_path="bizs")
    def list_biz(self, request, user_id):
        """
        @api {get} /auth/users/:user_id/bizs/  获取用户有权限的业务
        @apiName list_bizs
        @apiGroup UserPerm
        @apiSuccessExample {json} Succees-Response.data
            [
                {'bk_biz_id': 11, 'bk_biz_name': 'DNF'}
            ]
        """
        action_id = request.query_params.get("action_id", "biz.manage")
        bizs = UserPermission(user_id=user_id).get_scopes(action_id)
        if is_sys_scopes(bizs):
            return_data = Business.list()
            for _data in return_data:
                _data["bk_biz_id"] = int(_data["bk_biz_id"])
            return Response(return_data)

        return Response(Business.wrap_biz_name(bizs))

    @list_route(methods=["POST"])
    @params_valid(PermissionBatchCheckSerializer, add_params=False)
    def batch_check(self, request):
        """
        @api {post} /auth/users/batch_check/  批量校验用户与对象权限
        @apiName batch_check_perm
        @apiGroup UserPerm
        @apiParam {Dict[]} permissions 待鉴权列表
        @apiParam {string} permissions.user_id 待鉴权用户
        @apiParam {string} permissions.action_id 待鉴权功能
        @apiParam {string} permissions.object_id 待鉴权对象，如果不存在绑定对象，则传入 None
        @apiParam {boolean} [display_detail] 是否展示鉴权详细结果
        @apiParamExample {json} 校验用户对结果表是否有查询数据权限
            {
                permissions: [
                    {
                        "user_id": "admin",
                        "action_id": "result_table.query_data",
                        "object_id": "100107_cpu_parse",
                    }
                ]
            }
        @apiSuccessExample {json} Succees-Response.data
            [
                {
                    "user_id": "admin",
                    "action_id": "result_table.query_data",
                    "object_id": "100107_cpu_parse",
                    "result": true
                }
            ]
        """
        display_detail = request.cleaned_params.get("display_detail")

        pool = Pool(50)

        def _check(kwargs):
            user_id = kwargs["user_id"]
            action_id = kwargs["action_id"]
            object_id = kwargs["object_id"]
            display_detail = kwargs["display_detail"]

            ret = UserPermission(user_id=user_id).check(
                action_id, object_id, raise_exception=False, display_detail=display_detail
            )
            return {"user_id": user_id, "action_id": action_id, "object_id": object_id, "result": ret}

        kwargs_list = [
            {
                "user_id": perm["user_id"],
                "action_id": perm["action_id"],
                "object_id": perm.get("object_id", None),
                "display_detail": display_detail,
            }
            for perm in request.cleaned_params["permissions"]
        ]

        results = pool.map(_check, kwargs_list)
        return Response(results)

    @list_route(methods=["GET"])
    def operation_configs(self, request):
        """
        @api {post} /auth/users/operation_configs/ 返回用户有权限的功能开关
        @apiName operation_configs
        @apiGroup UserPerm

        @apiSuccessExample {json} Succees-Response.data
            [
                {
                    status: "active",
                    description: "标识数据开发服务是否存在",
                    operation_alias: "数据开发服务",
                    operation_id: "dataflow",
                    operation_name: "dataflow"
                }
            ]
        """
        user_id = get_request_username()

        configs = OperationConfig.objects.all()
        content = []
        for config in configs:
            if config.users is not None and user_id not in config.user_list:
                config.status = OperationStatus.DISABLED

            item = model_to_dict(config)
            del item["users"]
            content.append(item)

        return Response(content)

    @list_route(methods=["POST"], url_path="dgraph_scopes")
    @params_valid(DgraphScopesBatchCheckSerializer, add_params=False)
    def dgraph_scopes(self, request):
        """
        @api {post} /auth/users/dgraph_scopes/  批量用户权限范围（dgrpah格式）
        @apiName dgraph_scopes
        @apiGroup UserPerm
        @apiParam {Dict[]} permissions 参数列表
        @apiParam {string} permissions.user_id 用户
        @apiParam {string} permissions.action_id 功能
        @apiParam {string} permissions.variable_name 生成 dgraph 语句的变量名
        @apiParam {string} permissions.metadata_type 元数据类型
        @apiParamExample {json} 校验用户对结果表是否有查询数据权限
            {
                'permissions': [
                    {
                        'action_id': 'raw_data.update',
                        'user_id': self.USER1,
                        'variable_name': 'AAA',
                        'metadata_type': 'access_raw_data'
                    }
                ]
            }
        @apiSuccessExample {json} Succees-Response.data
            [
                {
                    'action_id': 'raw_data.update',
                    'variable_name': u'AAA',
                    'user_id': u'user01',
                    'result': {
                        'result': True,
                        'statement': 'AAA as var(func: has(AccessRawData.typed))'
                                     '@filter(  eq(AccessRawData.bk_biz_id, [591]) )'
                    },
                    'metadata_type': u'access_raw_data'
                },
                {
                    'action_id': 'raw_data.update',
                    'variable_name': u'AAA',
                    'user_id': u'user01',
                    'result': {
                        'result': False,
                        'statement': 'Invalid Parameters'
                    },
                    'metadata_type': u'access_raw_data'
                },
                {
                    'action_id': 'raw_data.update',
                    'variable_name': u'AAA',
                    'user_id': u'user01',
                    'result': {
                        'result': True,
                        'statement': '*'
                    },
                    'metadata_type': u'access_raw_data'
                }
            ]
        """
        pool = Pool(10)

        def _build_scope(perm):
            user_id = perm["user_id"]
            action_id = perm["action_id"]
            variable_name = perm["variable_name"]
            metadata_type = perm["metadata_type"]

            try:
                object_class = ObjectFactory.init_object_by_metadata_type(metadata_type)

                # 目前要求 action_id 必须与 object_class 强一致，比如 result_table.query_data & result_table
                if ActionConfig.objects.get(pk=action_id).object_class_id != object_class.auth_object_class:
                    raise ParameterErr(_("操作方式 action_id 与对象类型 metadata_type 不一致"))

                scopes = UserPermission(user_id=user_id).get_scopes(action_id)
                statement = object_class.to_graph_statement(scopes, variable_name)
                result = True
            except Exception as e:
                logger.exception(e.message)
                statement = e.message
                result = False

            return {
                "user_id": user_id,
                "action_id": action_id,
                "variable_name": variable_name,
                "metadata_type": metadata_type,
                "result": {"result": result, "statement": statement},
            }

        results = pool.map(_build_scope, request.cleaned_params["permissions"])
        return Response(results)

    @list_route(methods=["POST"], url_path="handover")
    @params_valid(UserHandoverSerializer, add_params=False)
    @require_indentities(["user", "inner"])
    def handover(self, request):
        """
        @api {post} /auth/users/handover/  交接权限
        @apiName handover_permission
        @apiGroup UserPerm
        @apiParam {String} receiver 被交接人
        @apiSuccessExample {json} Response.data
            {
                'handover_num': 33
            }
        """
        username = get_request_username()
        receiver = request.cleaned_params["receiver"]
        num, objects = UserPermission(user_id=username).handover(receiver)

        if num > 0:
            Stats.add_to_audit_action(
                Stats.gene_audit_id(), username, AUDIT_TYPE.USER_ROLE_HANDOVER, num, objects, operator=username
            )

        return Response({"handover_num": num})
