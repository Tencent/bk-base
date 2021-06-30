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

from dataflow.flow.tasklog.yarn_api_client.hadoop_conf import get_nodemanager_endpoint, get_nodemanager_webapp_endpoint
from dataflow.shared.log import flow_logger as logger

from . import yarn_api_client


class YarnUtil(object):
    @staticmethod
    def get_rm(geog_code="inland"):
        rm = yarn_api_client.ResourceManager(geog_code=geog_code)
        return rm

    @staticmethod
    def get_nm_webapp_port():
        """
        返回node manager webapp 的端口信息
        :return: node manager webapp的端口信息
        """
        nm = get_nodemanager_webapp_endpoint()
        return nm[nm.find(":") + 1 :]

    @staticmethod
    def get_nm_port():
        """
        返回node manager的端口信息
        :return: node manager的端口信息
        """
        nm = get_nodemanager_endpoint()
        return nm[nm.find(":") + 1 :]

    @staticmethod
    def get_containers_log_list(rm, app_id):
        """
        获取该app id所有appAttempt中的container的logUrl信息
        :param rm: resource manager
        :param app_id: yarn application id
        :return: 该app id所有appAttempt中的container的logUrl信息列表
        """
        ret = []
        try:
            attempts = rm.cluster_application_attempts(app_id)
            if attempts and attempts.data["appAttempts"]["appAttempt"]:
                app_attempt_id = attempts.data["appAttempts"]["appAttempt"][0]["appAttemptId"]
                all_containers_for_one_app_id = rm.cluster_app_attempt_containers(app_id, app_attempt_id)
                if all_containers_for_one_app_id and all_containers_for_one_app_id.data:
                    for one_container in all_containers_for_one_app_id.data["container"]:
                        ret.append(one_container["logUrl"])
        except Exception:
            # maybe we can not get app info from yarn the moment
            # just return nothing
            pass
        return ret

    @staticmethod
    def get_am_info(rm, app_id):
        """
        获取该appid对应的(original_tracking_url, tracking_url, logs_link, container_id, node_id)信息
        :param rm: resource manager
        :param app_id: yarn application id
        :return: (original_tracking_url, tracking_url, logs_link, container_id, node_id)
        """
        am_info_tuple = None
        try:
            attempts = rm.cluster_application_attempts(app_id)
            if attempts and attempts.data["appAttempts"]["appAttempt"]:
                attempts_list = attempts.data["appAttempts"]["appAttempt"]
                sorted_attempts_list = sorted(
                    attempts_list,
                    key=lambda one_attempt: one_attempt["id"],
                    reverse=True,
                )
                current_attempt_id = sorted_attempts_list[0]["appAttemptId"]
                logs_link = sorted_attempts_list[0]["logsLink"]
                container_id = sorted_attempts_list[0]["containerId"]
                node_id = sorted_attempts_list[0]["nodeId"]
                current_attempt_info = rm.cluster_application_attempt_info(app_id, current_attempt_id)
                if current_attempt_info and current_attempt_info.data:
                    # get jobmanager rest api
                    # "trackingUrl":"http://hadoop-xxx:8xxx/proxy/application_1584501917228_6365/"
                    # "originalTrackingUrl":"http://hadoop-xxx:4xxxx"
                    original_tracking_url = current_attempt_info.data["originalTrackingUrl"]
                    tracking_url = current_attempt_info.data["trackingUrl"]

                    am_info_tuple = (
                        original_tracking_url,
                        tracking_url,
                        logs_link,
                        container_id,
                        node_id,
                    )
                    logger.info(
                        "original_tracking_url(jm url)(%s), tracking_url(webappproxy url)(%s), "
                        "logs_link(jm log url)(%s), container_id(jm container id)(%s) "
                        "node_id(jm container's host_port)(%s)"
                        % (
                            original_tracking_url,
                            tracking_url,
                            logs_link,
                            container_id,
                            node_id,
                        )
                    )
        except Exception:
            # maybe we can not get app info from yarn the moment
            # just return nothing
            pass
        return am_info_tuple
