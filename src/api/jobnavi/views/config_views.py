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
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.base_utils import model_to_dict
from common.decorators import params_valid
from common.django_utils import DataResponse
from common.log import sys_logger
from common.views import APIViewSet
from jobnavi.handlers import dataflow_jobnavi_cluster_config
from jobnavi.exception.exceptions import ClusterInfoError
from jobnavi.models.bkdata_flow import DataflowJobNaviClusterConfig
from jobnavi.views.serializers import (
    CreateConfigSerializer,
    UpdateConfigSerializer,
    CommonSerializer,
)


class ConfigViewSet(APIViewSet):
    """
    @apiDefine cluster_config
    集群配置API
    """

    lookup_field = "cluster_name"
    lookup_value_regex = "\\w+"

    @params_valid(serializer=CreateConfigSerializer)
    def create(self, request, params):
        """
        @api {post} /jobnavi/cluster_config 添加集群配置
        @apiName healthz
        @apiGroup cluster_config
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        args = request.data
        try:
            tags = args["tags"][0]
            cluster_name = args["cluster_name"]
            cluster_domain = args["cluster_domain"]
            version = args["version"]
            created_by = args["created_by"]
            if DataflowJobNaviClusterConfig.objects.filter(cluster_name=cluster_name, geog_area_code=tags).exists():
                dataflow_jobnavi_cluster_config.update(
                    cluster_name=cluster_name,
                    geog_area_code=tags,
                    cluster_domain=cluster_domain,
                    version=version,
                    created_by=created_by,
                )
            else:
                dataflow_jobnavi_cluster_config.save(
                    tag=tags,
                    cluster_name=cluster_name,
                    cluster_domain=cluster_domain,
                    version=version,
                    geog_area_code=tags,
                    created_by=created_by,
                )
            return Response()
        except Exception as e:
            sys_logger.exception(e)
            return DataResponse(message=_("配置jobnavi集群信息失败，请联系管理员"))

    @params_valid(serializer=UpdateConfigSerializer)
    def update(self, request, cluster_name, params):
        """
        @api {put} /jobnavi/cluster_config/:cluster_name 更新集群配置
        @apiName healthz
        @apiGroup cluster_config
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        args = request.data
        try:
            tags = args["tags"][0]
            cluster_domain = args["cluster_domain"]
            version = args["version"]
            created_by = args["created_by"]
            if not DataflowJobNaviClusterConfig.objects.filter(cluster_name=cluster_name, geog_area_code=tags).exists():
                raise ClusterInfoError(message=_("集群不存在%s") % cluster_name)
            dataflow_jobnavi_cluster_config.update(
                cluster_name=cluster_name,
                cluster_domain=cluster_domain,
                version=version,
                created_by=created_by,
                geog_area_code=tags,
            )
            return Response()
        except Exception as e:
            sys_logger.exception(e)
            return DataResponse(message=_("配置jobnavi集群信息失败，请联系管理员"))

    @params_valid(serializer=CommonSerializer)
    def destroy(self, request, cluster_name, params):
        """
        @api {delete} /jobnavi/cluster_config/:cluster_name 配置删除
        @apiName cluster_config
        @apiGroup jobnavi
        @apiVersion 1.0.0
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                   "result": true,
                   "data": "ok",
                   "message": "",
                   "code": "1500200",
               }
        """
        args = request.data
        tags = args["tags"][0]
        dataflow_jobnavi_cluster_config.delete(cluster_name=cluster_name, geog_area_code=tags)
        return Response()

    def list(self, request):
        """
        @api {get} /jobnavi/cluster_config/ 获取 jobnavi 集群列表
        @apiName get_cluster
        @apiGroup cluster_config
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK

            {
                "message": "ok",
                "code": "1500200",
                "data": [

                ],
                "result": true
            }
        """
        tags = request.query_params.getlist("tags", [])
        filter_params = {}
        if tags:
            filter_params["geog_area_code"] = tags[0]
        cluster_configs = dataflow_jobnavi_cluster_config.filter(**filter_params)
        ret = [model_to_dict(cluster_config) for cluster_config in cluster_configs]
        return Response(ret)
