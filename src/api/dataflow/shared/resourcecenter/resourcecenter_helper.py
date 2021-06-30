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

from dataflow.shared.api.modules.resourcecenter import ResourceCenterApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class ResourceCenterHelper(object):
    @staticmethod
    def create_cluster(
        resource_type,
        service_type,
        src_cluster_id,
        cluster_type,
        cluster_name,
        component_type,
        geog_area_code,
        **kwargs
    ):
        """
        集群注册到资源系统
        :param resource_type:
        :param service_type:
        :param src_cluster_id:
        :param cluster_type:
        :param cluster_name:
        :param component_type:
        :param geog_area_code:
        :param kwargs:
        :return:
        """
        kwargs.update(
            {
                "resource_type": resource_type,
                "service_type": service_type,
                "src_cluster_id": src_cluster_id,
                "cluster_type": cluster_type,
                "cluster_name": cluster_name,
                "component_type": component_type,
                "geog_area_code": geog_area_code,
            }
        )
        res = ResourceCenterApi.cluster_register.create_cluster(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_cluster_register(
        resource_type,
        service_type,
        src_cluster_id,
        cluster_type,
        cluster_name,
        component_type,
        geog_area_code,
        update_type,
        **kwargs
    ):
        """
        更新集群注册到资源系统
        :param resource_type:
        :param service_type:
        :param src_cluster_id:
        :param cluster_type:
        :param cluster_name:
        :param component_type:
        :param geog_area_code:
        :param update_type:
        :param kwargs:
        :return:
        """
        kwargs.update(
            {
                "resource_type": resource_type,
                "service_type": service_type,
                "src_cluster_id": src_cluster_id,
                "cluster_type": cluster_type,
                "cluster_name": cluster_name,
                "component_type": component_type,
                "geog_area_code": geog_area_code,
                "update_type": update_type,
            }
        )
        # sys_logger.info(message=json.dumps(kwargs))
        res = ResourceCenterApi.cluster_register.update_cluster(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_capacity(
        geog_area_code, resource_type, service_type, cluster_type, src_cluster_id, update_type, **kwargs
    ):
        """
        更新集群在资源系统容量
        :param geog_area_code:
        :param resource_type:
        :param service_type:
        :param cluster_type:
        :param src_cluster_id:
        :param update_type:
        :param kwargs:
        :return:
        """
        kwargs.update(
            {
                "resource_type": resource_type,
                "service_type": service_type,
                "src_cluster_id": src_cluster_id,
                "cluster_type": cluster_type,
                "geog_area_code": geog_area_code,
                "update_type": update_type,
            }
        )
        res = ResourceCenterApi.cluster_register.update_capacity(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_cluster(src_cluster_id, geog_area_code, resource_type, service_type, cluster_type):
        """
        查看集群在资源系统信息
        :param src_cluster_id:
        :param geog_area_code:
        :param resource_type:
        :param service_type:
        :param cluster_type:
        :return:
        """

        request_params = {
            "src_cluster_id": src_cluster_id,
            "resource_type": resource_type,
            "service_type": service_type,
            "cluster_type": cluster_type,
            "geog_area_code": geog_area_code,
        }
        res = ResourceCenterApi.cluster_register.get_cluster(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_or_update_cluster(
        geog_area_code,
        resource_type,
        service_type,
        cluster_type,
        src_cluster_id,
        update_type,
        cluster_name,
        component_type,
        **kwargs
    ):
        """
        创建或更新集群在资源系统信息
        不存在则创建
        :param geog_area_code:
        :param resource_type:
        :param service_type:
        :param cluster_type:
        :param src_cluster_id:
        :param update_type:
        :param cluster_name:
        :param component_type:
        :return:
        """
        kwargs.update(
            {
                "src_cluster_id": src_cluster_id,
                "resource_type": resource_type,
                "service_type": service_type,
                "cluster_type": cluster_type,
                "geog_area_code": geog_area_code,
                "update_type": update_type,
                "component_type": component_type,
                "cluster_name": cluster_name,
            }
        )
        resource_cluster = ResourceCenterHelper.get_cluster(
            src_cluster_id=src_cluster_id,
            geog_area_code=geog_area_code,
            resource_type=resource_type,
            service_type=service_type,
            cluster_type=cluster_type,
        )
        # sys_logger.info(message=json.dumps(kwargs))
        if resource_cluster is None:
            return ResourceCenterHelper.create_cluster(**kwargs)
        else:
            return ResourceCenterHelper.update_cluster_register(**kwargs)

    @staticmethod
    def get_res_group_info(cluster_group_id, geog_area_code=None):
        """
        获取resource group相关信息，因为cluster group在全部地域保证唯一，故此处地域信息不强制要求传递
        :param cluster_group_id: cluster_group_id
        :param geog_area_code: geog_area_code
        :return:
        """
        resource_param = {"cluster_group": cluster_group_id}

        if geog_area_code:
            resource_param["geog_area_code"] = geog_area_code

        res = ResourceCenterApi.resource_group.list(resource_param)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_res_group_id_by_cluster_group(cluster_group, geog_area_code):
        """
        获取特定类型（resource_type）的resource group相关信息
        :param cluster_group: cluster_group
        :param geog_area_code: geog_area_code
        :return:
        """
        resource_param = {
            "cluster_group": cluster_group,
            "geog_area_code": geog_area_code,
        }
        res = ResourceCenterApi.resource_group.list(resource_param)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_cluster_info(
        geog_area_code,
        resource_group_id,
        resource_type,
        service_type=None,
        component_type=None,
        cluster_type=None,
        active=None,
        cluster_name=None,
    ):
        """
        获取cluster资源相关信息，因为resource group内可能存在不同地域的集群，必须通过地域信息获取
        :param geog_area_code: 地域信息
        :param resource_group_id: 资源组
        :param resource_type: 资源类型，processing为计算集群
        :param service_type: 服务类型，batch/stream
        :param component_type: 组件类型，spark/flink
        :param cluster_type: 集群类型，yarn/flink-yarn-cluster/flink-yarn-session/yarn-session..
        :param active: 集群是否生效可用，1可用
        :param cluster_name: 集群名称
        :return:
        """
        cluster_param = {}
        if resource_group_id:
            cluster_param["resource_group_id"] = resource_group_id
        if geog_area_code:
            cluster_param["geog_area_code"] = geog_area_code
        if resource_type:
            cluster_param["resource_type"] = resource_type
        if service_type:
            cluster_param["service_type"] = service_type
        if component_type:
            cluster_param["component_type"] = component_type
        if cluster_type:
            cluster_param["cluster_type"] = cluster_type
        if active is not None:
            cluster_param["active"] = active
        if cluster_name:
            cluster_param["cluster_name"] = cluster_name
        res = ResourceCenterApi.cluster_resource.list(cluster_param)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_cluster(one_cluster):
        """
        删除集群
        :param cluster_id: 集群唯一ID
        :return:
        """
        res = ResourceCenterApi.cluster_resource.delete(one_cluster)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def update_cluster(one_cluster, active=None, priority=None):
        """
        更新集群
        :param cluster_id: 集群唯一ID
        :param priority: 优先级
        :return:
        """
        one_cluster["description"] = "update cluster"
        if active is not None:
            one_cluster["active"] = active
        if priority is not None:
            one_cluster["priority"] = priority
        res = ResourceCenterApi.cluster_resource.update(one_cluster)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_default_resource_group():
        res = ResourceCenterApi.default_resource_group.list()
        res_util.check_response(res)
        return res.data
