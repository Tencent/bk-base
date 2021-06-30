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
from common.local import get_request_username
from resourcecenter.handlers import resource_service_config
from resourcecenter.storekit.storekit_helper import StorekitHelper


def add_service_type_for_storage(service_type):
    """
    从存储场景同步service_type
    :param service_type:
    :return:
    """
    exists_service_type = resource_service_config.filter_list(
        resource_type="storage", service_type=service_type
    ).exists()
    if not exists_service_type:
        sync_cluster_type_to_service_config()


def sync_cluster_type_to_service_config():
    """
    从存储同步存储集群到资源服务类型配置
    :return:
    """
    # 从存储获取全量存储场景，添加不存在的service_type
    scenarios = StorekitHelper.get_scenarios()
    storage_service_type_list = resource_service_config.filter_list(active=1, resource_type="storage")
    if scenarios:
        for storage_scenario in scenarios:
            _exists = False
            if storage_service_type_list:
                for _conf in storage_service_type_list:
                    if storage_scenario["cluster_type"] == _conf.service_type:
                        _exists = True
                        break
            if (
                not _exists
                and not resource_service_config.filter_list(
                    service_type=storage_scenario["cluster_type"], resource_type="storage"
                ).exists()
            ):
                resource_service_config.save(
                    resource_type="storage",
                    service_type=storage_scenario["cluster_type"],
                    active=1,
                    service_name=storage_scenario["formal_name"],
                    created_by=get_request_username(),
                )
