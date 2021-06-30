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
from auth.handlers.object_classes import oFactory
from auth.models.base_models import ObjectConfig
from auth.views.auth_serializers import ObjectsScopeActionSerializer, ScopeSerializer
from common.decorators import list_route, params_valid
from common.views import APIViewSet
from rest_framework.response import Response


class ObjectViewSet(APIViewSet):
    @list_route()
    @params_valid(ObjectsScopeActionSerializer, add_params=False)
    def objects_scope_action(self, request):
        """
        @api {get} /v3/auth/objects/objects_scope_action/ 对象及权限列表
        @apiName object_list
        @apiGroup Object
        @apiParam {String} is_user_mode 是否用户模式
        @apiSuccessExample {json}
        [
          {
            scope_id_key: "bk_biz_id",
            scope_object_classes: [
              {
                scope_name_key: "bk_biz_name",
                scope_id_key: "bk_biz_id",
                scope_object_class: "biz",
                scope_object_name: "业务"
              }
            ],
            object_class_name: "业务",
            scope_name_key: "bk_biz_name",
            actions: [
              {
                has_instance: true,
                action_name: "数据接入",
                scope_object_classes: [
                  {
                    scope_name_key: "bk_biz_name",
                    scope_id_key: "bk_biz_id",
                    scope_object_class: "biz",
                    scope_object_name: "业务"
                  }
                ],
                action_id: "biz.access_raw_data",
                order: "1.1"
              }
            ],
            has_object: true,
            object_class: "biz"
          }
        ]
        """
        is_user_mode = request.cleaned_params.get("is_user_mode")
        return Response(ObjectConfig.get_object_scope_action(is_user_mode=is_user_mode))

    @params_valid(ScopeSerializer, add_params=False)
    def list(self, request):
        """
        @api {post} /v3/auth/objects/ 对象列表
        @apiName object_list
        @apiGroup Object
        @apiParam {String} object_class 对象类型
        @apiSuccessExample {json}
            {
              "message": "",
              "code": "00",
              "data": [
                {
                  "project_name": "项目1",
                  "id": 1,
                  "name": "[1]项目1",
                  "description": "test"
                },
                {
                  "project_name": "项目2",
                  "id": 2,
                  "name": "[2]项目2",
                  "description": "test"
                }
              ],
              "result": true
            }
        """
        object_class = request.cleaned_params["object_class"]
        bk_biz_id = request.cleaned_params.get("bk_biz_id")
        project_id = request.cleaned_params.get("project_id")
        filters = {}
        if bk_biz_id:
            filters["bk_biz_id"] = bk_biz_id
        if project_id:
            filters["project_id"] = project_id
        try:
            o_object_class = oFactory.init_object_by_class(object_class)
            data = o_object_class.list(
                output_val_key="id", display_key_maps={"display_name_key": "name"}, filters=filters
            )
        except NotImplementedError:
            data = []
        return Response(data)
