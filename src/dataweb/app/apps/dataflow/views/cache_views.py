# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from rest_framework import serializers
from rest_framework.response import Response

from apps.exceptions import CacheKeyError
from apps.generic import APIViewSet


class CacheViewSet(APIViewSet):
    serializer_class = serializers.Serializer

    def create(self, request):
        """
        @api {post} /cache/  设置cache内容
        @apiName set_cache
        @apiGroup Cache

        @apiParam {String} key cache内容的key
        @apiParam {Object} content cache内容

        @apiSuccess {Boolean} data 表示是否存储cache成功，成功为True，失败为False
        """
        key = request.data.get("key")
        content = request.data.get("content")

        request.session[key] = content

        return Response(True)

    def list(self, request):
        """
        @api {get} /cache/  获取cache内容
        @apiName get_cache
        @apiGroup Cache

        @apiParam {String} key cache内容的key

        @apiSuccess {Object} data cache的内容
        """
        key = request.query_params.get("key")

        if key not in request.session:
            raise CacheKeyError()

        return Response(request.session[key])
