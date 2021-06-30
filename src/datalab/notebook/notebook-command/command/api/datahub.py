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
import time

import requests
from command.constants import (
    CLUSTER_OBJECT_NOT_FOUND,
    CODE,
    DATA,
    HEADERS,
    HTTP_STATUS_OK,
    MESSAGE,
    RESULT,
    SINK_RT_ID,
    SINK_TYPE,
    SOURCE_RT_ID,
    SOURCE_TYPE,
    STATUS,
    STOPPED,
    SUCCESS,
    TRANSPORT_WAIT_INTERVAL,
)
from command.exceptions import ApiRequestException, ExecuteException
from command.settings import DATAHUB_API_ROOT
from command.utils import api_retry, extract_error_message, parse_response


def transport_data(source_rt_id, source_type, sink_rt_id, sink_type):
    """
    迁移数据

    :param source_rt_id: 源表
    :param source_type: 源表的存储
    :param sink_rt_id: 目标表
    :param sink_type: 目标表的存储
    :return: 迁移任务id
    """
    url = "%s/databus/tasks/transport/" % DATAHUB_API_ROOT
    data = json.dumps(
        {SOURCE_RT_ID: source_rt_id, SOURCE_TYPE: source_type, SINK_RT_ID: sink_rt_id, SINK_TYPE: sink_type}
    )
    res = requests.post(url=url, headers=HEADERS, data=data)
    return parse_response(res, "迁移数据失败")


def get_transport_status(transport_id):
    """
    获取数据迁移任务状态

    :param transport_id: 迁移任务id
    :return: 迁移状态
    """
    url = "{}/databus/tasks/transport/{}/".format(DATAHUB_API_ROOT, transport_id)
    res = requests.get(url=url)
    if res.status_code == HTTP_STATUS_OK and res.json().get(RESULT):
        return True, res.json()
    else:
        return False, "获取数据迁移任务状态失败，迁移id：{}；失败原因：{}".format(transport_id, extract_error_message(res))


def get_transport_result(transport_id):
    """
    获取数据迁移任务结果

    :param transport_id: 迁移任务id
    :return: 获取迁移执行结果
    """
    while True:
        time.sleep(TRANSPORT_WAIT_INTERVAL)
        status_flag, status_result = get_transport_status(transport_id)
        if not status_flag:
            retry_flag, status_result = api_retry(get_transport_status, transport_id)
            if not retry_flag:
                return status_result
        status = status_result[DATA][STATUS]
        if status == SUCCESS:
            return "创建成功"
        elif status == STOPPED:
            return "迁移任务已被人工停止，任务id：%s" % transport_id
        else:
            continue


def retrieve_cluster_info(cluster_type, cluster_name):
    """
    获取指定集群的信息

    :param cluster_type: 集群类型
    :param cluster_name: 集群名
    :return: 集群信息
    """
    url = "{}/storekit/clusters/{}/{}/".format(DATAHUB_API_ROOT, cluster_type, cluster_name)
    res = requests.get(url=url)
    if res.status_code == HTTP_STATUS_OK:
        res_json = res.json()
        if res_json[RESULT]:
            return res_json[DATA]
        else:
            message = (
                "save操作失败：集群不存在，请检查集群名是否正确"
                if res_json[CODE] == CLUSTER_OBJECT_NOT_FOUND
                else "获取集群信息失败：%s" % res_json[MESSAGE]
            )
            raise ExecuteException(message)
    else:
        raise ApiRequestException("storekit接口异常: %s" % extract_error_message(res))


def get_hdfs_conf(result_table_id):
    """
    获取hdfs存储配置

    :param result_table_id: 结果表id
    :return: 类型和表名
    """
    url = "{}/storekit/hdfs/{}/hdfs_conf/".format(DATAHUB_API_ROOT, result_table_id)
    response = requests.get(url=url)
    return parse_response(response, "获取结果表%s关联的hdfs存储配置失败，失败原因" % result_table_id)


def get_file_config(raw_data_id, file_name):
    """
    获取文件配置

    :param raw_data_id: 数据源id
    :param file_name: 文件名
    :return: 文件配置
    """
    url = "{}/access/collector/upload/{}/get_hdfs_info/?file_name={}".format(DATAHUB_API_ROOT, raw_data_id, file_name)
    response = requests.get(url=url)
    return parse_response(response, "获取文件配置失败，数据源id：{}，文件名：{}，失败原因".format(raw_data_id, file_name))


def destroy_storage(storage, result_table_id):
    """
    删除存储

    :param storage: 存储类型
    :param result_table_id: 结果表id
    """
    url = "{}/storekit/{}/{}/".format(DATAHUB_API_ROOT, storage, result_table_id)
    response = requests.delete(url=url, headers=HEADERS)
    return parse_response(response, "删除存储失败")


def destroy_rt_storage_relation(result_table_id, storage):
    """
    删除结果表和存储的关联关系

    :param result_table_id: 结果表id
    :param storage: 存储类型
    """
    url = "{}/storekit/result_tables/{}/{}/".format(DATAHUB_API_ROOT, result_table_id, storage)
    response = requests.delete(url=url, headers=HEADERS)
    return parse_response(response, "结果表存储关系删除失败")
