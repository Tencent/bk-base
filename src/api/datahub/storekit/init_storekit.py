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

import requests
from common.decorators import list_route
from common.log import logger
from common.views import APIViewSet
from datahub.storekit import model_manager, settings
from datahub.storekit.serializers import InitFileSerializer
from rest_framework.response import Response


class InitStorekitViewset(APIViewSet):
    """
    databus初始化
    """

    @list_route(methods=["get"], url_path="init_clusters")
    def init_clusters(self, request):
        """
        @api {get} v3/storekit/init/init_clusters 初始化存储集群配置
        @apiGroup InitStorekit
        @apiDescription ！！！管理接口，请勿调用！！！初始化存储集群配置信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":{},
            "result":true
        }
        """
        # 通过参数获取集群初始化配置文件
        params = self.params_valid(serializer=InitFileSerializer)
        if params.get("file_name"):
            file_name = params.get("file_name", "init_clusters.json")
            # 读取配置文件中内容，解析，确保是json格式的列表
            file_content = read_conf_file(file_name)
        else:
            file_content = params.get("cluster_config")

        json_list = parse_json_list(file_content)
        for cluster in json_list:
            # 逐个集群处理，如果集群已存在，则更新，如果不存在，则添加
            name = cluster["cluster_name"]
            type = cluster["cluster_type"]

            if type not in settings.SUPPORT_STORAGE_TYPE:
                continue

            # 获取集群信息
            exist_cluster = model_manager.get_cluster_obj_by_name_type(name, type)
            if not exist_cluster:
                # 创建存储集群
                add_cluster(cluster)
            else:
                # 更新存储集群
                logger.info(f"cluster {name}({type}) exists, going to update.")
                cluster["id"] = exist_cluster.id
                update_cluster(cluster)

        return Response(True)

    @list_route(methods=["get"], url_path="init_scenarios")
    def init_scenarios(self, request):
        """
        @api {get} v3/storekit/init/init_scenarios 初始化存储场景配置
        @apiGroup InitStorekit
        @apiDescription ！！！管理接口，请勿调用！！！初始化存储集群场景配置
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data":{},
            "result":true
        }
        """
        # 通过参数获取集群初始化配置文件
        params = self.params_valid(serializer=InitFileSerializer)
        file_name = params.get("file_name", "init_scenarios.json")
        # 读取配置文件中内容，解析，确保是json格式的列表
        file_content = read_conf_file(file_name)
        json_list = parse_json_list(file_content)
        for scenario in json_list:
            # 逐个存储场景处理，如果已存在，则更新，如果不存在，则添加
            type = scenario["cluster_type"]
            if type not in settings.SUPPORT_STORAGE_TYPE:
                continue

            name = scenario["storage_scenario_name"]
            alias = scenario["storage_scenario_alias"]
            description = scenario.get("description", "")
            storage_scenario_priority = scenario.get("storage_scenario_priority", 0)
            cluster_type_priority = scenario.get("cluster_type_priority", 0)
            # 获取场景配置信息
            objs = model_manager.get_scenario_objs_by_name_type(name, type)
            if objs:
                # 更新场景配置信息
                logger.info(f"storage scenario {name}({type}) exists, going to update.")
                update_scenario(name, type, alias, description, storage_scenario_priority, cluster_type_priority)
            else:
                # 创建存储场景配置
                add_scenario(name, type, alias, description, storage_scenario_priority, cluster_type_priority)

        return Response(True)


def read_conf_file(file_name):
    """
    获取指定文件的内容
    :param file_name: 文件名称
    :return: 文件内容
    """
    try:
        f = open(file_name)
        return f.read()
    except Exception:
        logger.warning(f"read file {file_name} failed.", exc_info=True)
        raise Exception(f"file {file_name} not exists!")


def parse_json_list(json_str):
    """
    解析json格式的字符串为list
    :param json_str: json格式的字符串
    :return: 列表
    """
    try:
        json_list = json.loads(json_str)
        if type(json_list) == list:
            return json_list
    except Exception:
        logger.warning(f"config is not a valid json: {json_str}", exc_info=True)
    # 代码逻辑走到这里，说明配置文件的内容非json的list
    raise Exception(f"config is not a valid json list: {json_str}")


def add_cluster(cluster_config):
    """
    添加存储集群
    :param cluster_config: 存储集群信息
    :return: 是否添加集群成功
    """
    url = f'{settings.STOREKIT_API_URL}/clusters/{cluster_config["cluster_type"]}/'
    data = {
        "cluster_name": cluster_config["cluster_name"],
        "cluster_group": cluster_config.get("group", "default"),  # 默认组名称为default,
        "connection_info": cluster_config["connection_info"],
        "expires": cluster_config["expires"],
        "tags": cluster_config["tags"],
        "version": cluster_config["version"],
        "description": cluster_config.get("description", ""),
        "belongs_to": cluster_config.get("belongs_to", "bkdata"),
    }
    res = requests.post(url, data=json.dumps(data), headers={"Content-Type": "application/json"})
    if res.status_code == 200:
        res_json = res.json()
        if res_json["result"]:
            logger.info(f'add cluster {cluster_config["cluster_name"]}({cluster_config["cluster_type"]}) success.')
            return True
    # 请求失败，抛出异常
    logger.warning(
        f'add cluster {cluster_config["cluster_name"]}({cluster_config["cluster_type"]}) failed. '
        f"{res.status_code} {res.text}"
    )
    raise Exception(
        f'add cluster {cluster_config["cluster_name"]}({cluster_config["cluster_type"]}) failed. '
        f'{res.json()["message"]}'
    )


def update_cluster(cluster_config):
    """
    添加存储集群
    :param cluster_config: 存储集群信息
    :return: 是否添加集群成功
    """
    url = f'{settings.STOREKIT_API_URL}/clusters/{cluster_config["cluster_type"]}/{ cluster_config["cluster_name"]}/'
    data = {
        "cluster_group": cluster_config.get("group", "default"),
        "connection_info": cluster_config["connection_info"],
        "expires": cluster_config["expires"],
        "version": cluster_config["version"],
        "description": cluster_config.get("description", ""),
        "belongs_to": cluster_config.get("belongs_to", "bkdata"),
    }
    res = requests.put(url, data=json.dumps(data), headers={"Content-Type": "application/json"})
    if res.status_code == 200 and res.json()["result"]:
        logger.info(f'update cluster {cluster_config["cluster_name"]}({cluster_config["cluster_type"]}) success.')
    else:
        # 请求失败，抛出异常
        logger.warning(
            f'update cluster {cluster_config["cluster_name"]}({cluster_config["cluster_type"]}) failed. '
            f"{res.status_code} {res.text}"
        )
        raise Exception(
            f'update cluster { cluster_config["cluster_name"]}({cluster_config["cluster_type"]}) '
            f'failed. {res.json()["message"]}'
        )


def add_scenario(name, type, alias, desc, scenario_priority, type_priority):
    """
    添加存储场景
    :param name: 集群名称
    :param type: 集群类型
    :param alias: 场景别名
    :param desc: 集群描述信息
    :param scenario_priority: 场景优先级
    :param type_priority: 存储类型优先级
    :return: 是否添加存储场景成功
    """
    url = f"{settings.STOREKIT_API_URL}/scenarios/{type}/"
    data = {
        "storage_scenario_name": name,
        "storage_scenario_alias": alias,
        "description": desc,
        "storage_scenario_priority": scenario_priority,
        "cluster_type_priority": type_priority,
    }
    res = requests.post(url, data=json.dumps(data), headers={"Content-Type": "application/json"})
    if res.status_code == 200:
        res_json = res.json()
        if res_json["result"]:
            logger.info(f"add storage scenario {name}({type}) success.")
            return True
    # 请求失败，抛出异常
    logger.warning(f"add storage scenario {name}({type}) failed. {res.status_code} {res.text}")
    raise Exception(f'add storage scenario {name}({type}) failed. {res.json()["message"]}')


def update_scenario(name, type, alias, desc, scenario_priority, type_priority):
    """
    更新存储场景
    :param name: 集群名称
    :param type: 集群类型
    :param alias: 场景别名
    :param desc: 集群描述信息
    :param scenario_priority: 场景优先级
    :param type_priority: 存储类型优先级
    :return: 是否更新存储场景成功
    """
    url = f"{settings.STOREKIT_API_URL}/scenarios/{type}/{name}/"
    data = {
        "storage_scenario_alias": alias,
        "description": desc,
        "storage_scenario_priority": scenario_priority,
        "cluster_type_priority": type_priority,
    }
    res = requests.put(url, data=json.dumps(data), headers={"Content-Type": "application/json"})
    if res.status_code == 200:
        res_json = res.json()
        if res_json["result"]:
            logger.info(f"update storage scenario {name}({type}) success.")
            return True
    # 请求失败，抛出异常
    logger.warning(f"update storage scenario {name}({type}) failed. {res.status_code} {res.text}")
    raise Exception(f'update storage scenario {name}({type}) failed. {res.json()["message"]}')
