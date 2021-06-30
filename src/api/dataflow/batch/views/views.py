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

from common.decorators import list_route, params_valid
from common.errorcodes import ErrorCode
from common.views import APIViewSet
from django.db import transaction
from django.forms import model_to_dict
from rest_framework.response import Response

from dataflow.batch.check.check_driver import check
from dataflow.batch.deploy.deploy_driver import deploy
from dataflow.batch.error_code.errorcodes import DataapiBatchCode
from dataflow.batch.serializer.serializers import ConfigSerializer, DeploySerializer
from dataflow.shared.handlers import processing_cluster_config
from dataflow.shared.resourcecenter.resourcecenter_helper import ResourceCenterHelper
from dataflow.shared.utils import error_codes


class HealthCheckView(APIViewSet):
    def healthz(self, request):
        """
        @api {get} /dataflow/batch/healthz 健康状态
        @apiName HealthCheck
        @apiGroup batch
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
        rtn = check()
        return Response(rtn)


class DeployView(APIViewSet):
    @params_valid(serializer=DeploySerializer)
    def deploy(self, request, params):
        """
        @api {get} /dataflow/batch/deploy 部署发布
        @apiName deploy
        @apiGroup batch
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
        geog_area_code = params["geog_area_code"]
        deploy(geog_area_code)
        return Response()


class BatchErrorCodesView(APIViewSet):
    def errorcodes(self, request):
        """
        @api {get} /dataflow/batch/errorcodes/ 获取batch的errorcodes
        @apiName errorcodes
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [[
                        {
                        message: "Batch非法参数异常",
                        code: "1572001",
                        name: "ILLEGAL_ARGUMENT_EX",
                        solution: "-",
                        }]
                    "result": true
                }
        """
        return Response(error_codes.get_codes(DataapiBatchCode, ErrorCode.BKDATA_DATAAPI_BATCH))


class ConfigViewSet(APIViewSet):
    lookup_field = "cluster_name"
    # 由英文字母、数字、下划线、小数点组成，且需以字母开头，不需要增加^表示开头和加$表示结尾
    lookup_value_regex = r"[a-zA-Z]+(_|[a-zA-Z0-9]|\.)*"

    @params_valid(serializer=ConfigSerializer)
    @transaction.atomic()
    def create(self, request, params):
        """
        @api {post} /dataflow/batch/cluster_config 配置添加
        @apiName cluster_config
        @apiGroup batch
        @apiVersion 1.0.0
        @apiParam {string} geog_area_code 地区code
        @apiParam {string} component_type 离线集群组件类型
        @apiParam {string} cluster_name 集群名称（离线一般使用队列名称）
        @apiParam {string} cluster_domain 集群域名（用于区分不同集群）
        @apiParam {string} cluster_group 集群组
        @apiParam {string} version 版本
        @apiParam {string} [cpu] CPU（单位：core）
        @apiParam {string} [memory] 内存（单位：MB）
        @apiParam {string} [disk] 磁盘（单位：MB）
        @apiParamExample {json} 参数样例:
        {
                "geog_area_code": "inland",
                "component_type": "spark",
                "cluster_name": "root.dataflow.batch.xxxx",
                "cluster_domain": "sz-spark-01.xxx:80",
                "cluster_group": "test_1",
                "version": "2.3.1",
                "cpu": 50.0,
                "memory": 102400.0,
                "disk": 0.0
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
        capacity = {}
        if params.get("cpu", -1) > 0.0:
            capacity["cpu"] = params["cpu"]
        if params.get("memory", -1) > 0.0:
            capacity["memory"] = params["memory"]
        if params.get("disk", -1) > 0.0:
            capacity["disk"] = params["disk"]
        args = request.data
        geog_area_code = args["geog_area_code"]
        if processing_cluster_config.where(
            geog_area_code=geog_area_code,
            component_type=args["component_type"],
            cluster_name=args["cluster_name"],
        ).exists():
            processing_cluster_config.update(
                geog_area_code=geog_area_code,
                cluster_name=args["cluster_name"],
                component_type=args["component_type"],
                cluster_domain=args["cluster_domain"],
                cluster_group=args["cluster_group"],
                version=args["version"],
            )
        else:
            processing_cluster_config.save(
                tag=geog_area_code,
                cluster_domain=args["cluster_domain"],
                cluster_group=args["cluster_group"],
                cluster_name=args["cluster_name"],
                version=args["version"],
                component_type=args["component_type"],
                geog_area_code=geog_area_code,
                priority=1,
            )

        # 注册到资源中心
        self.__register_into_resource_center(args, capacity)
        return Response()

    @params_valid(serializer=ConfigSerializer)
    @transaction.atomic()
    def update(self, request, cluster_name, params):
        """
        @api {put} /dataflow/batch/cluster_config/:cluster_name 配置更新
        @apiName update_cluster_config
        @apiGroup batch
        @apiVersion 1.0.0
        @apiParam {string} geog_area_code 地区code
        @apiParam {string} component_type 离线集群组件类型
        @apiParam {string} cluster_name 集群名称（离线一般使用队列名称）
        @apiParam {string} cluster_domain 集群域名（用于区分不同集群）
        @apiParam {string} cluster_group 集群组
        @apiParam {string} version 版本
        @apiParam {string} [cpu] CPU（单位：core）
        @apiParam {string} [memory] 内存（单位：MB）
        @apiParam {string} [disk] 磁盘（单位：MB）
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland",
                "component_type": "spark",
                "cluster_name": "root.dataflow.batch.xxxx",
                "cluster_domain": "sz-spark-01.xxx:80",
                "cluster_group": "test_1",
                "version": "2.3.1",
                "cpu": 50.0,
                "memory": 102400.0,
                "disk": 0.0
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
        capacity = {}
        if params.get("cpu", -1) > 0.0:
            capacity["cpu"] = params["cpu"]
        if params.get("memory", -1) > 0.0:
            capacity["memory"] = params["memory"]
        if params.get("disk", -1) > 0.0:
            capacity["disk"] = params["disk"]

        args = request.data
        processing_cluster_config.update(
            geog_area_code=args["geog_area_code"],
            cluster_name=cluster_name,
            component_type=args["component_type"],
            cluster_domain=args["cluster_domain"],
            cluster_group=args["cluster_group"],
            version=args["version"],
        )
        # 注册到资源中心
        self.__register_into_resource_center(args, capacity)

        return Response()

    def __register_into_resource_center(self, args, capacity):
        """
        注册离线计算集群到资源中心
        :param args:
        :param capacity:
        :return:
        """
        if "yarn" != args.get("component_type", "yarn"):
            # yarn 是资源管理系统，不是具体的任务类型集群。
            # 离线计算component_type=spark时，cluster_name就是队列名称
            src_cluster_id = args["cluster_domain"] + "__" + args["cluster_name"]

            ResourceCenterHelper.create_or_update_cluster(
                resource_type="processing",
                service_type="batch",
                src_cluster_id=src_cluster_id,
                cluster_type=args["component_type"],
                cluster_name=args["cluster_name"],
                component_type=args["component_type"],
                geog_area_code=args["geog_area_code"],
                update_type="full",
                cluster_group=args["cluster_group"],
                **capacity
            )

    def destroy(self, request, cluster_name):
        """
        @api {delete} /dataflow/batch/cluster_config/:cluster_name 配置删除
        @apiName cluster_config
        @apiGroup batch
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
        processing_cluster_config.delete(cluster_name=cluster_name)
        return Response()

    def list(self, request):
        """
        @api {get}  /dataflow/batch/cluster_config/ 全部离线集群(仅Spark集群）
        @apiName list_batch_cluster_config
        @apiGroup batch
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
        query_list = processing_cluster_config.where(component_type="spark")
        ret = []
        if query_list:
            for obj in query_list:
                ret.append(model_to_dict(obj))
        return Response(ret)

    @list_route(methods=["get"], url_path="get_yarn_cluster")
    def get_yarn_cluster(self, request):
        """
        @api {get} /dataflow/batch/cluster_config/get_yarn_cluster/ 返回全部yarn集群地址
        @apiName get_yarn_cluster
        @apiGroup Batch
        @apiParam {string} [geog_area_code] 地区code
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} 成功返回yarn集群列表
        {

        }
        :return:
        """
        params = {"component_type": "yarn"}
        geog_area_code = request.query_params.get("geog_area_code", None)
        if geog_area_code:
            params["geog_area_code"] = geog_area_code

        cluster_configs = processing_cluster_config.where(**params)
        ret = []
        if cluster_configs:
            for obj in cluster_configs:
                ret.append(model_to_dict(obj))

        return Response(ret)
