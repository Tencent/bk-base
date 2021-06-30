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

from common.decorators import params_valid
from common.views import APIViewSet
from django.db import transaction
from rest_framework.response import Response

from dataflow.component.exceptions.comp_execptions import ClusterConfigException
from dataflow.component.handlers.cluster_config_handler import (
    create_cluster_config,
    destroy_cluster_config,
    retrieve_cluster_config,
)
from dataflow.component.serializer.serializers import ClusterConfigOperationSerializer, ClusterConfigSerializer
from dataflow.shared.log import component_logger as logger


class ClusterConfigViewSet(APIViewSet):
    lookup_field = "cluster_name"
    # 由英文字母、数字、下划线、小数点组成，且需以字母开头，不需要增加^表示开头和加$表示结尾
    lookup_value_regex = r"[a-zA-Z]+(_|[a-zA-Z0-9]|\.)*"

    @params_valid(serializer=ClusterConfigSerializer)
    @transaction.atomic()
    def create(self, request, params):
        """
        @api {post} /dataflow/component/cluster_configs/ 配置添加，已存在则更新
        @apiName cluster_config
        @apiGroup component
        @apiVersion 1.0.0
        @apiParam {string} geog_area_code 地区code
        @apiParam {string} component_type 离线集群组件类型
        @apiParam {string} cluster_name 集群名称（离线一般使用队列名称）
        @apiParam {string} cluster_domain 集群域名（用于区分不同集群）
        @apiParam {string} cluster_group 集群组
        @apiParam {string} version 版本
        @apiParam {string} cluster_label 集群标签
        @apiParam {string} priority 集群优先级
        @apiParam {string} belong 集群归属者
        @apiParam {string} description 集群描述
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland",
                "component_type": "xxx",
                "cluster_name": "root.dataflow.batch.xxxx",
                "cluster_domain": "sz-spark-01.xxx:80",
                "cluster_group": "test_1",
                "version": "2.3.1",
                "cluster_label": "xxx_label",
                "priority": 1,
                "belong": "xxx",
                "description": "xxx"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "ok",
                    "message": "",
                    "code": "1500200",
                }
        """
        create_cluster_config(params)
        return Response()

    @params_valid(serializer=ClusterConfigOperationSerializer)
    def destroy(self, request, cluster_name, params):
        """
        @api {delete} /dataflow/component/cluster_configs/:cluster_name/ 配置删除
        @apiName cluster_config
        @apiGroup component
        @apiVersion 1.0.0
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland",
                "component_type": "xxx",
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                   "result": true,
                   "data": "ok",
                   "message": "",
                   "code": "1500200",
               }
        """
        if not params.get("component_type") or not params.get("geog_area_code") or not cluster_name:
            raise ClusterConfigException("cluster config 参数异常，component_type/geog_area_code/cluster_name不能为空")
        destroy_cluster_config(params, cluster_name)
        logger.info(
            "destroy cluster config, cluster_name(%s), component_type(%s), geog_area_code(%s)"
            % (cluster_name, params.get("component_type"), params.get("geog_area_code"))
        )
        return Response()

    @params_valid(serializer=ClusterConfigOperationSerializer)
    def retrieve(self, request, cluster_name, params):
        """
        @api {get}  /dataflow/component/cluster_configs/:cluster_name/ 获取集群信息
        @apiName list_cluster_config
        @apiGroup component
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland",
                "component_type": "xxx",
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
                            "priority": 1,
                            "updated_by": "xxx",
                            "component_type": "spark",
                            "cluster_domain": "xxxx.hadoop.xx.xx:8088",
                            "created_at": "2019-03-23 21:05:36",
                            "belong": "bkdata",
                            "updated_at": "2019-06-26 14:43:55",
                            "created_by": "xxx",
                            "cluster_name": "root.dataflow.batch.xxx",
                            "version": "hadoo-2.6.0-cdh5.4.1",
                            "cluster_group": "xxx",
                            "cluster_label": "",
                            "id": 38,
                            "description": "yarn队列"
                        }
                    ],
                    "result": true
                }
        """
        ret = retrieve_cluster_config(params, cluster_name)
        return Response(ret)
