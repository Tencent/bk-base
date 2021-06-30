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

from django.db import transaction
from django.forms import model_to_dict
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.local import get_request_username
from common.views import APIViewSet
from common.decorators import params_valid, detail_route
from resourcecenter.auth.auth_helper import AuthHelper
from resourcecenter.error_code.errorcodes import DataapiResourceCenterCode
from resourcecenter.exceptions.base_exception import ResourceCenterException
from resourcecenter.handlers import (
    resource_group_capacity_apply_form,
    resource_group_info,
    resource_unit_config,
    resource_service_config,
)
from resourcecenter.serializers.serializers import (
    CreateResourcesGroupCapacityApplyFormSerializer,
    CommonApproveResultSerializer,
)
from resourcecenter.services import resource_group_geog_branch_svc, resource_service_config_svc
from resourcecenter.utils.format_utils import covert_mb_size


class GroupCapacityApplyViewSet(APIViewSet):
    """
    @apiDefine ResourceCenter_CapacityApply
    资源组扩缩容接口
    """

    lookup_field = "apply_id"

    def list(self, request):
        """
        @api {get} /resourcecenter/group_capacity_applys/ 全部资源扩缩容申请
        @apiName all_capacity_applys
        @apiGroup ResourceCenter_CapacityApply
        @apiParam {int} [apply_id] 申请单号
        @apiParam {string} [geog_area_code] 地区
        @apiParam {string} [resource_group_id] 资源组ID
        @apiParam {string} [resource_type] 资源分类
        @apiParam {string} [service_type] 服务类型
        @apiParam {string} [cluster_id] 集群ID
        @apiParam {string} [status] 状态
        @apiParam {string} [created_by] 申请人
        @apiParamExample {json} 参数样例:
            {
              "apply_id": "123"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                            "geog_area_code": "inland",
                            "resource_group_id": "default_test",
                            "description": "接入xx业务数据，数据量比较大",
                            "apply_type": "increase",
                            "created_at": "2020-02-19T15:54:59",
                            "resource_unit_id": 100,
                            "updated_at": "2020-02-19T15:54:59",
                            "created_by": "xx",
                            "status": "approve",
                            "num": 100,
                            "apply_id": 12,
                            "operate_result": null,
                            "service_type": "stream",
                            "cluster_id": null,
                            "resource_type": "processing",
                            "updated_by": null
                        },
                        {
                            "geog_area_code": "inland",
                            "resource_group_id": "default_test",
                            "description": "接入xx业务数据，数据量比较大",
                            "apply_type": "increase",
                            "created_at": "2020-02-19T15:55:02",
                            "resource_unit_id": 100,
                            "updated_at": "2020-02-19T15:55:02",
                            "created_by": "xx",
                            "status": "approve",
                            "num": 100,
                            "apply_id": 13,
                            "operate_result": null,
                            "service_type": "stream",
                            "cluster_id": null,
                            "resource_type": "processing",
                            "updated_by": null
                        }
                    ],
                    "result": true
                }
        """
        query_params = {}
        # TODO：查询参数处理
        query_list = resource_group_capacity_apply_form.filter_list(**query_params)
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))

        return Response(ret)

    @params_valid(serializer=CreateResourcesGroupCapacityApplyFormSerializer)
    @transaction.atomic(using="bkdata_basic")
    def create(self, request, params):
        """
        @api {post} /resourcecenter/group_capacity_applys/ 创建资源扩缩容申请
        @apiName create_group_capacity_apply
        @apiGroup ResourceCenter_CapacityApply
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_group_id 资源组
        @apiParam {string} geog_area_code 地区
        @apiParam {string} apply_type 单据类型
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型
        @apiParam {int} resource_unit_id 资源套餐ID
        @apiParam {int} num 数量
        @apiParam {string} description 申请原因
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_group_id": "default_test",
                "geog_area_code": "inland",
                "apply_type": "increase",
                "resource_type": "processing",
                "service_type": "stream",
                "resource_unit_id": 100,
                "num": 100,
                "description": "接入xx业务数据，数据量比较大"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "apply_id": 123
                    },
                    "result": true
                }
        """
        process_id = uuid.uuid1().hex

        # 权限校验
        check_ret = AuthHelper.check_user_manager_resource_group(
            get_request_username(), params["resource_group_id"]
        )
        if not check_ret:
            raise ResourceCenterException(
                message=_("不是资源组管理员，没有权限进行资源扩缩容申请。"), code=DataapiResourceCenterCode.ILLEGAL_AUTH_EX
            )

        resource_group = resource_group_info.get(params["resource_group_id"])
        if not resource_group:
            raise ResourceCenterException(
                message=_("资源组不存在，资源组ID=%s。") % params["resource_group_id"],
                code=DataapiResourceCenterCode.ILLEGAL_AUTH_EX,
            )
        if resource_group.status not in ["succeed"]:
            raise ResourceCenterException(
                message=_("资源组未生效，不允许申请资源。"), code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX
            )

        # 判断集群组是否存在, 不存在则创建。
        cluster_group_id = resource_group_geog_branch_svc.check_and_make_cluster_group(
            params["resource_group_id"], params["geog_area_code"]
        )

        # 验证meta cluster_group_config 是否存在，不存在则创建
        # 提示：如果是资源系统创建的资源组，在meta是不存在cluster_group_config的
        resource_group_geog_branch_svc.create_cluster_group_in_meta(
            cluster_group_id, params["geog_area_code"], resource_group.group_name, resource_group.group_type
        )

        if params["resource_type"] == "storage":
            resource_service_config_svc.add_service_type_for_storage(service_type=params["resource_type"])

        ret = resource_group_capacity_apply_form.save(
            resource_group_id=params["resource_group_id"],
            geog_area_code=params["geog_area_code"],
            apply_type=params["apply_type"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            resource_unit_id=params["resource_unit_id"],
            num=params["num"],
            status="approve",
            process_id=process_id,
            description=params["description"],
            created_by=get_request_username(),
        )
        unit_config = resource_unit_config.get(params["resource_unit_id"])
        if unit_config is None:
            raise ResourceCenterException(message=_("资源单元型号，不存在。"), code=DataapiResourceCenterCode.UNEXPECT_EX)
        cpu = unit_config.cpu if unit_config.cpu is not None else 0.0
        if cpu == 0.0:
            cpu = " - "
        else:
            cpu = params["num"] * cpu

        memory = unit_config.memory if unit_config.memory is not None else 0.0
        if memory == 0.0:
            memory = " - "
        else:
            memory = params["num"] * memory

        disk = unit_config.disk if unit_config.disk is not None else 0.0
        if disk == 0.0:
            disk = " - "
        else:
            disk = params["num"] * disk

        service_info = resource_service_config.get(params["resource_type"], params["service_type"])

        # 添加单据
        reason = _("资源申请")
        content = [
            {"name": _("资源组ID"), "value": params["resource_group_id"]},
            {"name": _("所属业务"), "value": resource_group.bk_biz_id},
            {"name": _("资源分类"), "value": params["resource_type"]},
            {"name": _("服务类型"), "value": params["service_type"] + "(" + service_info.service_name + ")"},
            {"name": _("集群组ID"), "value": str(cluster_group_id)},
            {"name": "CPU", "value": str(cpu)},
            {"name": _("内存"), "value": covert_mb_size(memory)},
            {"name": _("磁盘"), "value": covert_mb_size(disk)},
            {"name": _("申请原因"), "value": params["description"]},
        ]
        # print json.dumps(content)
        AuthHelper.tickets_create_capacity_apply(
            ret.apply_id, process_id, resource_group.bk_biz_id, reason, json.dumps(content)
        )

        return Response({"apply_id": ret.apply_id})

    @params_valid(serializer=CreateResourcesGroupCapacityApplyFormSerializer)
    @transaction.atomic(using="bkdata_basic")
    def update(self, request, apply_id, params):
        """
        @api {put} /resourcecenter/group_capacity_applys/:apply_id/ 更新资源扩缩容申请
        @apiName update_group_capacity_apply
        @apiGroup ResourceCenter_CapacityApply
        @apiParam {int} apply_id 申请单号
        @apiParam {string} bk_username 提交人
        @apiParam {string} resource_group_id 资源组
        @apiParam {string} geog_area_code 地区
        @apiParam {string} apply_type 单据类型
        @apiParam {string} resource_type 资源分类
        @apiParam {string} service_type 服务类型
        @apiParam {int} resource_unit_id 资源套餐ID
        @apiParam {int} num 数量
        @apiParam {string} description 申请原因
        @apiParamExample {json} 参数样例:
            {
                "bk_username": "xx",
                "resource_group_id": "default_test",
                "geog_area_code": "inland",
                "apply_type": "increase",
                "resource_type": "processing",
                "service_type": "stream",
                "resource_unit_id": 100,
                "num": 1000,
                "description": "接入xx业务数据，数据量比较大"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                            "apply_id": 123
                    },
                    "result": true
                }
        """
        if params["resource_type"] == "storage":
            resource_service_config_svc.add_service_type_for_storage(service_type=params["resource_type"])
        # 只有在特定状态下，才可以更新这些信息。
        resource_group_capacity_apply_form.update(
            apply_id,
            resource_group_id=params["resource_group_id"],
            geog_area_code=params["geog_area_code"],
            apply_type=params["apply_type"],
            resource_type=params["resource_type"],
            service_type=params["service_type"],
            resource_unit_id=params["resource_unit_id"],
            num=params["num"],
            description=params["description"],
            updated_by=get_request_username(),
        )
        return Response({"apply_id": apply_id})

    def retrieve(self, request, apply_id):
        """
        @api {get} /resourcecenter/group_capacity_apply/:apply_id/ 查看资源扩缩容申请
        @apiName view_capacity_apply
        @apiGroup ResourceCenter_CapacityApply
        @apiParam {int} apply_id 申请单号
        @apiParamExample {json} 参数样例:
            {
              "apply_id": "123"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "geog_area_code": "inland",
                        "resource_group_id": "default_test",
                        "description": "接入xx业务数据，数据量比较大",
                        "apply_type": "increase",
                        "created_at": "2020-02-19T15:55:02",
                        "resource_unit_id": 100,
                        "updated_at": "2020-02-19T15:55:02",
                        "created_by": "xx",
                        "status": "approve",
                        "num": 100,
                        "apply_id": 13,
                        "operate_result": null,
                        "service_type": "stream",
                        "cluster_id": null,
                        "resource_type": "processing",
                        "updated_by": null
                    },
                    "result": true
                }
        """
        obj = resource_group_capacity_apply_form.get(apply_id)
        return Response(model_to_dict(obj))

    def destroy(self, request, apply_id):
        """
        @api {delete} /resourcecenter/group_capacity_applys/:apply_id/ 删除资源扩缩容申请
        @apiName delete_group_capacity_apply
        @apiGroup ResourceCenter_CapacityApply
        @apiParam {int} apply_id 申请单号
        @apiParamExample {json} 参数样例:
            {
              "apply_id": "123"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "apply_id": "123"
                    },
                    "result": true
                }
        """
        # 特定状态下才能删除，草稿状态吧。
        resource_group_capacity_apply_form.delete(int(apply_id))
        return Response({"apply_id": apply_id})

    @detail_route(methods=["post"], url_path="approve_result")
    @params_valid(serializer=CommonApproveResultSerializer)
    @transaction.atomic(using="bkdata_basic")
    def approve_result(self, request, apply_id, params):
        """
        @api {post} /resourcecenter/group_capacity_applys/:apply_id/approve_result/ 更新资源扩缩容申请结果
        @apiName approve_result_group_capacity_applys
        @apiGroup ResourceCenter_CapacityApply
        @apiParam {string} apply_id 申请单ID
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
        # process_id = request.data.get("process_id", None)
        change_status = request.data.get("status", None)

        apply_form = resource_group_capacity_apply_form.get(apply_id)
        if apply_form:
            # 1. 通过
            if change_status == "succeeded":
                if apply_form.status in ["approve", "reject"]:
                    resource_group_capacity_apply_form.update(apply_id, status="succeed", updated_by=operator)
                    # TODO： 更新容量信息（当前版本，按申请容量进行更新，后续与管理系统结合，使用管理系统进行更新）
                    pass
                else:
                    raise ResourceCenterException(
                        message=_("资源扩缩容申请单(%(apply_id)s)当前状态为: %(status)s，审批无效。")
                        % {"apply_id": apply_id, "status": apply_form.status},
                        code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX,
                    )
            # 2. 拒绝
            elif change_status == "failed":
                if apply_form.status in ["approve"]:
                    pass
                    resource_group_capacity_apply_form.update(
                        apply_id, status="reject", updated_by=get_request_username()
                    )
                else:
                    raise ResourceCenterException(
                        message=_("资源扩缩容申请单(%(apply_id)s)当前状态为: %(status)s，审批无效。")
                        % {"apply_id": apply_id, "status": apply_form.status},
                        code=DataapiResourceCenterCode.ILLEGAL_STATUS_EX,
                    )
            # 3. 终止
            elif change_status == "stopped":
                pass

        return Response()
