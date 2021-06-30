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
import uuid

from django.conf import settings
from django.db import transaction
from django.forms import model_to_dict
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.views import APIViewSet
from common.decorators import params_valid, list_route, detail_route
from common.local import get_request_username
from resourcecenter.auth.auth_helper import AuthHelper
from resourcecenter.meta.meta_helper import MetaHelper
from resourcecenter.error_code.errorcodes import DataapiResourceCenterCode
from resourcecenter.exceptions.base_exception import ResourceCenterException

from resourcecenter.handlers import resource_group_info, resource_cluster_config
from resourcecenter.serializers.serializers import (
    CreateResourceGroupInfoSerializer,
    PartialUpdateResourceGroupInfoSerializer,
    CommonApproveResultSerializer,
    UpdateResourceGroupInfoSerializer,
)
from resourcecenter.services import resource_group_svc


class ResourceGroupViewSet(APIViewSet):
    """
    @apiDefine ResourceCenter_Group
    资源组管理接口
    """

    lookup_field = "resource_group_id"

    def list(self, request):
        """
        @api {get} /resourcecenter/resource_groups/ 全部资源组（不含指标）
        @apiName all_resource_groups
        @apiGroup ResourceCenter_Group
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [group_name] 资源组名称
        @apiParam {int} [bk_biz_id] 所属业务
        @apiParam {string} [status] 状态（approve:审批中,succeed:审批通过,reject:拒绝的,delete:删除的）
        @apiParam {string} [group_type] 资源组类型（多个用逗号分隔；public：公开,protected：业务内共享,private：私有）
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "",
              "group_name": "",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "bk_biz_id": 10000,
                            "resource_group_id": "default_test1",
                            "description": "测试使用资源组4444",
                            "created_at": "2020-02-19T13:39:11",
                            "updated_at": "2020-02-19T13:39:11",
                            "created_by": "xx",
                            "group_name": "测试默认资源组",
                            "status": "approve",
                            "group_type": "public",
                            "updated_by": null
                        },
                        {
                            "bk_biz_id": 10000,
                            "resource_group_id": "default_test2",
                            "description": "测试使用资源组222",
                            "created_at": "2020-02-19T13:17:14",
                            "updated_at": "2020-02-19T14:00:11",
                            "created_by": "xx",
                            "group_name": "202",
                            "status": "approve",
                            "group_type": "public",
                            "updated_by": "xx"
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        if "resource_group_id" in request.query_params and request.query_params["resource_group_id"]:
            query_params["resource_group_id"] = request.query_params["resource_group_id"]
        if "group_name" in request.query_params and request.query_params["group_name"]:
            query_params["group_name__icontains"] = request.query_params["group_name"]
        if "bk_biz_id" in request.query_params and request.query_params["bk_biz_id"]:
            query_params["bk_biz_id"] = request.query_params["bk_biz_id"]
        if "status" in request.query_params and request.query_params["status"]:
            query_params["status"] = request.query_params["status"]
        if "group_type" in request.query_params and request.query_params["group_type"]:
            query_params["group_type__in"] = request.query_params["group_type"].split(",")

        query_list = resource_group_info.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @params_valid(serializer=CreateResourceGroupInfoSerializer)
    @transaction.atomic(using="bkdata_basic")
    def create(self, request, params):
        """
        @api {post} /resourcecenter/resource_groups/ 创建资源组
        @apiName create_resource_group
        @apiGroup ResourceCenter_Group
        @apiParam {string} bk_username 提交人
        @apiParam {string[]} admin 资源组管理员（数组）
        @apiParam {string} resource_group_id 资源组英文标识（只能字母数字下划线字符）
        @apiParam {string} group_name 资源组名称
        @apiParam {string} group_type 资源组类型（public：公开,protected：业务内共享,private：私有）
        @apiParam {int} bk_biz_id 所属业务ID
        @apiParam {string} [description] 描述说明
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "admin": ["user1", "user2" , "user3"],
                "resource_group_id": "default_test",
                "group_name": "测试默认资源组",
                "group_type": "public",
                "bk_biz_id": 10000,
                "description": "测试使用资源组"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "resource_group_id": "default_test"
                    },
                    "result": true
                }
        """
        process_id = uuid.uuid1().hex
        resource_group_info.save(
            resource_group_id=params["resource_group_id"],
            group_name=params["group_name"],
            group_type=params["group_type"],
            bk_biz_id=params["bk_biz_id"],
            status="approve",
            process_id=process_id,
            description=params.get("description", None),
            created_by=get_request_username(),
        )
        # 更新auth
        AuthHelper.update_scope_role("resource_group.manager", params["resource_group_id"], params["admin"])

        # 添加单据
        reason = _("创建资源组")
        content = [
            {"name": _("资源组ID"), "value": params["resource_group_id"]},
            {"name": _("资源组名称"), "value": params["group_name"]},
            {"name": _("所属业务"), "value": params["bk_biz_id"]},
            {"name": _("资源组类型"), "value": params.get("group_type", "")},
            {"name": _("描述"), "value": params.get("description", "")},
        ]
        AuthHelper.tickets_create_resource_group(
            params["resource_group_id"], process_id, params["bk_biz_id"], reason, json.dumps(content)
        )

        return Response({"resource_group_id": params["resource_group_id"]})

    @params_valid(serializer=UpdateResourceGroupInfoSerializer)
    @transaction.atomic(using="bkdata_basic")
    def update(self, request, resource_group_id, params):
        """
        @api {put} /resourcecenter/resource_groups/:resource_group_id/ 更新资源组
        @apiName update_resource_group
        @apiGroup ResourceCenter_Group
        @apiParam {string} bk_username 提交人
        @apiParam {string} group_name 资源组名称
        @apiParam {string} group_type 资源组类型（public：公开,protected：业务内共享,private：私有）
        @apiParam {int} bk_biz_id 所属业务ID
        @apiParam {string} [description] 描述说明
        @apiParamExample {json} 参数样例:
            {
                bk_username: "xx",
                group_name: "测试默认资源组",
                group_type: "public",
                bk_biz_id: 10000,
                description: "测试使用资源组"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "resource_group_id": "default_test"
                    },
                    "result": true
                }
        """

        # 权限校验
        check_ret = AuthHelper.check_user_manager_resource_group(get_request_username(), resource_group_id)
        if not check_ret:
            raise ResourceCenterException(message=_("不是资源组管理员，没有权限修改。"), code=DataapiResourceCenterCode.ILLEGAL_AUTH_EX)

        resource_group_info.update(resource_group_id, **params)
        return Response({"resource_group_id": resource_group_id})

    @detail_route(methods=["put"], url_path="update_biz_id")
    def update_biz_id(self, request, resource_group_id):
        """
        @api {put} /resourcecenter/resource_groups/:resource_group_id/update_biz_id/ 更新资源组业务ID
        @apiName update_biz_id
        @apiGroup ResourceCenter_Group
        @apiParam {string} bk_biz_id 业务ID
        @apiParamExample {json} 参数样例:
            {
                bk_biz_id: 10000,
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {},
                    "result": true
                }
        """

        bk_biz_id = request.data.get("bk_biz_id", -1)
        _resource_group_id = request.data.get("resource_group_id", "")
        if bk_biz_id == -1:
            raise ResourceCenterException(message=_("业务ID有不正确。"), code=DataapiResourceCenterCode.ILLEGAL_ARGUMENT_EX)
        if _resource_group_id != resource_group_id:
            raise ResourceCenterException(message=_("资源组ID有不正确。"), code=DataapiResourceCenterCode.ILLEGAL_ARGUMENT_EX)
        params = {"bk_biz_id": bk_biz_id}
        resource_group_info.update(resource_group_id, **params)
        obj = resource_group_info.get(resource_group_id)
        _dict = model_to_dict(obj)
        return Response(_dict)

    @params_valid(serializer=PartialUpdateResourceGroupInfoSerializer)
    def partial_update(self, request, resource_group_id, params):
        """
        @api {patch} /resourcecenter/resource_groups/:resource_group_id/ 部分更新资源组
        @apiName partial_update_resource_group
        @apiGroup ResourceCenter_Group
        @apiParam {string} resource_group_id 资源套餐单元ID
        @apiParam {string} group_name 资源组名称
        @apiParam {string} [description] 描述说明
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test",
              "bk_username": "xxx",
              "group_name": "资源组名称",
              "description": "描述说明"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "resource_group_id": "default_test"
                    },
                    "result": true
                }
        """

        # 权限校验
        check_ret = AuthHelper.check_user_manager_resource_group(get_request_username(), resource_group_id)
        if not check_ret:
            raise ResourceCenterException(message=_("不是资源组管理员，没有权限修改。"), code=DataapiResourceCenterCode.ILLEGAL_AUTH_EX)

        resource_group_info.update(
            resource_group_id,
            group_name=params["group_name"],
            description=params["description"],
            updated_by=get_request_username(),
        )
        return Response({"resource_group_id": resource_group_id})

    def retrieve(self, request, resource_group_id):
        """
        @api {get} /resourcecenter/resource_groups/:resource_group_id/ 查看资源组
        @apiName view_resource_group
        @apiGroup ResourceCenter_Group
        @apiParam {string} resource_group_id 资源组ID
        @apiParamExample {json} 参数样例:
            {
                "resource_group_id": "default_test"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                          "resource_group_id": "default_test",
                          "admin": ["user1", "user2" , "user3"],
                          "group_name": "默认测试资源组",
                          "group_type": 'public',
                          "bk_biz_id": 10000,
                          "status": "生效中",
                          "description": "测试资源组"
                    },
                    "result": true
                }
        """
        obj = resource_group_info.get(resource_group_id)
        _dict = model_to_dict(obj)
        users = AuthHelper.get_resource_group_manager(resource_group_id)
        _dict["admin"] = users
        return Response(_dict)

    @detail_route(methods=["get"], url_path="auth_list")
    def auth_list(self, request, resource_group_id):
        """
        @api {get} /resourcecenter/resource_groups/:resource_group_id/auth_list/ 查看资源组授权主体
        @apiName auth_list
        @apiGroup ResourceCenter_Group
        @apiParam {string} resource_group_id 资源组ID
        @apiParamExample {json} 参数样例:
            {
                "resource_group_id": "default_test"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "resource_group_id": "bkdata",
                            "subject_type": "project_info",
                            "description": null,
                            "created_at": "2020-03-30 11:55:22",
                            "created_by": "XXX",
                            "subject_id": "1",
                            "subject_name": "subject_1",
                            "subject_type_name": "项目",
                            "id": 2
                        }
                    ],
                    "result": true
                }
        """
        data = AuthHelper.get_resource_group_auth_subjects(resource_group_id, check_success=False)
        project_ids = []
        for subject in data:
            if "project_info" == subject["subject_type"]:
                project_ids.append(subject["subject_id"])
        projects = MetaHelper.get_projects(project_ids)
        projects_name = {}
        for proj in projects:
            projects_name[str(proj["project_id"])] = proj["project_name"]
        for subject in data:
            subject["subject_type_name"] = self.get_subject_type_name(subject["subject_type"])
            if "project_info" == subject["subject_type"]:
                subject["subject_name"] = projects_name.get(subject["subject_id"], subject["subject_id"])

        return Response(data)

    @detail_route(methods=["post"], url_path="approve_result")
    @params_valid(serializer=CommonApproveResultSerializer)
    def approve_result(self, request, resource_group_id, params):
        """
        @api {post} /resourcecenter/resource_groups/:resource_group_id/approve_result/ 更新审核结果
        @apiName approve_success_resource_group
        @apiGroup ResourceCenter_Group
        @apiParam {string} resource_group_id 资源组ID
        @apiParamExample {json} 参数样例:
            {
                "operator": "user01",
                "message": "ok",       # 审批意见
                "process_id": "111",   # 流程单ID
                "status": "succeeded"  # succeeded=通过 | failed=拒绝 | stopped=中止
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {},
                    "result": true
                }
        """
        operator = request.data.get("operator", None)
        # message = request.data.get("message", None)
        change_status = request.data.get("status", None)

        resource_group = resource_group_info.get(resource_group_id)
        if resource_group:
            # 1. 通过
            if change_status == "succeeded":
                if resource_group.status in ["approve", "reject"]:
                    resource_group_info.update(resource_group_id, status="succeed", updated_by=operator)
                else:
                    raise ResourceCenterException(
                        message=_("资源组(%(resource_group_id)s)当前状态为: %(status)s，审批无效。")
                        % {"resource_group_id": resource_group_id, "status": resource_group.status},
                        code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX,
                    )
            # 2. 拒绝
            elif change_status == "failed":
                if resource_group.status in ["approve"]:
                    resource_group_info.update(resource_group_id, status="reject", updated_by=get_request_username())
                else:
                    raise ResourceCenterException(
                        message=_("资源组(%(resource_group_id)s)当前状态为: %(status)s，审批无效。")
                        % {"resource_group_id": resource_group_id, "status": resource_group.status},
                        code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX,
                    )
            # 3. 终止
            elif change_status == "stopped":
                pass

        return Response()

    @transaction.atomic(using="bkdata_basic")
    def destroy(self, request, resource_group_id):
        """
        @api {delete} /resourcecenter/resource_groups/:resource_group_id/ 删除资源组
        @apiName delete_resource_group
        @apiGroup ResourceCenter_Group
        @apiParam {string} resource_group_id 资源组ID
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "resource_group_i": "default_test"
                    },
                    "result": true
                }
        """
        # 权限校验
        check_ret = AuthHelper.check_user_manager_resource_group(get_request_username(), resource_group_id)
        if not check_ret:
            raise ResourceCenterException(message=_("不是资源组管理员，没有权限修改。"), code=DataapiResourceCenterCode.ILLEGAL_AUTH_EX)
        resource_group = resource_group_info.get(resource_group_id)
        if resource_group and resource_group.status not in ["approve", "reject"]:
            raise ResourceCenterException(
                message=_("资源组(%s)生效中，不可以删除。") % resource_group_id, code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX
            )

        # 删除撤单
        if resource_group and resource_group.status in ["approve"]:
            AuthHelper.tickets_withdraw(resource_group.process_id, "deleted by " + get_request_username())

        resource_group_info.delete(resource_group_id)
        return Response({"resource_group_id": resource_group_id})

    @list_route(methods=["get"], url_path="my_list")
    def my_resource_groups(self, request):
        """
        @api {get} /resourcecenter/resource_groups/my_list/ 我管理的资源组
        @apiName my_resource_groups
        @apiGroup ResourceCenter_Group
        @apiParam {string} bk_username 用户
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [group_name] 资源组名称
        @apiParam {int} [bk_biz_id] 所属业务
        @apiParamExample {json} 参数样例:
            {
              "bk_username": "bk_username",
              "resource_group_id": "",
              "group_name": "",
              "bk_biz_id": "",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": [
                        {
                            "bk_biz_id": 10000,
                            "resource_group_id": "default_test1",
                            "description": "测试使用资源组4444",
                            "created_at": "2020-02-19T13:39:11",
                            "updated_at": "2020-02-19T13:39:11",
                            "created_by": "xx",
                            "group_name": "测试默认资源组",
                            "status": "approve",
                            "group_type": "public",
                            "updated_by": null,
                            "metrics": {
                                "processing": {
                                    "load": "90%",
                                    "status": "red",
                                    "name": "计算资源",
                                    "trend": "up",
                                    "memory": "10TB",
                                    "gpu": "",
                                    "cpu": "1000 Core"
                                },
                                "storage": {
                                    "load": "80%",
                                    "status": "yellow",
                                    "name": "存储资源",
                                    "trend": "up",
                                    "memory": "512 GB",
                                    "disk": "10 TB",
                                    "cpu": "100 Core"
                                },
                                "databus": {
                                    "load": "40%",
                                    "status": "green",
                                    "name": "总线资源",
                                    "trend": "down",
                                    "memory": "512 GB",
                                    "disk": "10 TB",
                                    "cpu": "100 Core"
                                }
                            }
                        },
                        {
                            "bk_biz_id": 10000,
                            "resource_group_id": "default_test2",
                            "description": "测试使用资源组222",
                            "created_at": "2020-02-19T13:17:14",
                            "updated_at": "2020-02-19T14:00:11",
                            "created_by": "xx",
                            "group_name": "202",
                            "status": "approve",
                            "group_type": "public",
                            "updated_by": "xx",
                            "metrics": {
                                "processing": {
                                    "load": "90%",
                                    "status": "red",
                                    "name": "计算资源",
                                    "trend": "up",
                                    "memory": "10TB",
                                    "gpu": "",
                                    "cpu": "1000 Core"
                                },
                                "storage": {
                                    "load": "80%",
                                    "status": "yellow",
                                    "name": "存储资源",
                                    "trend": "up",
                                    "memory": "512 GB",
                                    "disk": "10 TB",
                                    "cpu": "100 Core"
                                },
                                "databus": {
                                    "load": "40%",
                                    "status": "green",
                                    "name": "总线资源",
                                    "trend": "down",
                                    "memory": "512 GB",
                                    "disk": "10 TB",
                                    "cpu": "100 Core"
                                }
                            }
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        query_capacity_params = {}
        if "resource_group_id" in request.query_params and request.query_params["resource_group_id"]:
            query_params["resource_group_id"] = request.query_params["resource_group_id"]
            query_capacity_params["resource_group_id"] = request.query_params["resource_group_id"]
        if "group_name" in request.query_params and request.query_params["group_name"]:
            query_params["group_name__icontains"] = request.query_params["group_name"]
        if "bk_biz_id" in request.query_params and request.query_params["bk_biz_id"]:
            query_params["bk_biz_id"] = request.query_params["bk_biz_id"]
            # query_capacity_params['bk_biz_id'] = request.query_params['bk_biz_id']

        result_list = []
        auth_ret = AuthHelper.get_user_manager_resource_groups()
        if auth_ret and len(auth_ret) > 0:
            my_resource_group_ids = []
            for e in auth_ret:
                if "resource_group_id" in e:
                    my_resource_group_ids.append(e["resource_group_id"])
            query_params["resource_group_id__in"] = my_resource_group_ids
            query_capacity_params["resource_group_id__in"] = my_resource_group_ids
            query_list = resource_group_info.filter_list(**query_params)
            cluster_list = resource_cluster_config.filter_list(**query_capacity_params)
            result_list = resource_group_svc.merge_metrics(query_list, cluster_list)

        return Response(result_list)

    @list_route(methods=["get"], url_path="all_list")
    def all_resource_groups(self, request):
        """
        @api {get} /resourcecenter/resource_groups/all_list/ 全部资源组(含指标）
        @apiName all_list_resource_groups
        @apiGroup ResourceCenter_Group
        @apiParam {string} bk_username 用户
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [group_name] 资源组名称
        @apiParam {int} [bk_biz_id] 所属业务
        @apiParamExample {json} 参数样例:
            {
              "bk_username": "bk_username",
              "resource_group_id": "",
              "group_name": "",
              "bk_biz_id": ,
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": [
                        {
                            "bk_biz_id": 10000,
                            "resource_group_id": "default_test1",
                            "description": "测试使用资源组4444",
                            "created_at": "2020-02-19T13:39:11",
                            "updated_at": "2020-02-19T13:39:11",
                            "created_by": "xx",
                            "group_name": "测试默认资源组",
                            "status": "approve",
                            "group_type": "public",
                            "updated_by": null,
                            "metrics": {
                                "processing": {
                                    "load": "90%",
                                    "status": "red",
                                    "name": "计算资源",
                                    "trend": "up",
                                    "memory": "10TB",
                                    "gpu": "",
                                    "cpu": "1000 Core"
                                },
                                "storage": {
                                    "load": "80%",
                                    "status": "yellow",
                                    "name": "存储资源",
                                    "trend": "up",
                                    "memory": "512 GB",
                                    "disk": "10 TB",
                                    "cpu": "100 Core"
                                },
                                "databus": {
                                    "load": "40%",
                                    "status": "green",
                                    "name": "总线资源",
                                    "trend": "down",
                                    "memory": "512 GB",
                                    "disk": "10 TB",
                                    "cpu": "100 Core"
                                }
                            }
                        },
                        {
                            "bk_biz_id": 10000,
                            "resource_group_id": "default_test2",
                            "description": "测试使用资源组222",
                            "created_at": "2020-02-19T13:17:14",
                            "updated_at": "2020-02-19T14:00:11",
                            "created_by": "xx",
                            "group_name": "202",
                            "status": "approve",
                            "group_type": "public",
                            "updated_by": "xx",
                            "metrics": {
                                "processing": {
                                    "load": "90%",
                                    "status": "red",
                                    "name": "计算资源",
                                    "trend": "up",
                                    "memory": "10TB",
                                    "cpu": "1000Core"
                                },
                                "storage": {
                                    "load": "80%",
                                    "status": "yellow",
                                    "name": "存储资源",
                                    "trend": "up",
                                    "memory": "512GB",
                                    "disk": "10TB",
                                    "cpu": "100 Core"
                                },
                                "databus": {
                                    "load": "40%",
                                    "status": "green",
                                    "name": "总线资源",
                                    "trend": "down",
                                    "memory": "512GB",
                                    "disk": "10TB",
                                    "cpu": "100Core"
                                }
                            }
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        query_capacity_params = {}
        if "resource_group_id" in request.query_params and request.query_params["resource_group_id"]:
            query_params["resource_group_id"] = request.query_params["resource_group_id"]
            query_capacity_params["resource_group_id"] = request.query_params["resource_group_id"]
        if "group_name" in request.query_params and request.query_params["group_name"]:
            query_params["group_name__icontains"] = request.query_params["group_name"]
        if "bk_biz_id" in request.query_params and request.query_params["bk_biz_id"]:
            query_params["bk_biz_id"] = request.query_params["bk_biz_id"]

        query_list = resource_group_info.filter_list(**query_params)
        cluster_list = resource_cluster_config.filter_list(**query_capacity_params)
        ret = resource_group_svc.merge_metrics(query_list, cluster_list)

        return Response(ret)

    def metrics(self, request, resource_group_id):
        """
        @api {get} /resourcecenter/resource_groups/:resource_group_id/metrics 资源组总体指标
        @apiName metrics_resource_group
        @apiGroup ResourceCenter_Group
        @apiParam {string} resource_group_id 资源组ID
        @apiParamExample {json} 参数样例:
            {
              "resource_group_id": "default_test"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        resource_group_i: "default_test",
                        group_name: "测试资源组",
                        group_type: "private",
                        status: "succeed",
                        description: "default_test",
                        metrics: {
                            processing: {
                                "name": "计算资源",
                                "cpu": "1000Core",
                                "memory": "10TB",
                                "gpu": "",
                                "load": "50%"
                            },
                            storage: {
                                "name": "存储资源",
                                "cpu": "100Core",
                                "memory": "512GB",
                                "disk": "10 TB",
                                "load": "40%"
                            },
                            databus: {
                                "name": "总线资源",
                                "cpu": "100Core",
                                "memory": "512GB",
                                "disk": "10TB",
                                "load": "40%"
                            },
                            auth: {
                                "name": "授权信息",
                                "project_num": "80",
                                "bk_biz_num": "2",
                                "data_id_num": "55"
                             }
                        }
                    },
                    "result": true
                }
        """
        return Response()

    @list_route(methods=["get"], url_path="default")
    def default(self, request):
        """
        @api {get} /resourcecenter/resource_groups/default/ 默认资源组
        @apiName default_resource_group
        @apiGroup ResourceCenter_Group
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                          "resource_group_id": "default_test",
                          "admin": ["user1", "user2" , "user3"],
                          "group_name": "默认测试资源组",
                          "group_type": 'public',
                          "bk_biz_id": 10000,
                          "status": "生效中",
                          "description": "测试资源组"
                    },
                    "result": true
                }
        """
        default_resource_group = model_to_dict(resource_group_info.get(settings.DEFAULT_RESOURCE_GROUP_ID))

        return Response(default_resource_group)

    def get_subject_type_name(self, auth_type):
        auth_type_dict = {"project_info": _("项目"), "bk_biz": _("业务"), "access_raw_data": _("数据 dataId")}
        return auth_type_dict.get(auth_type, auth_type)
