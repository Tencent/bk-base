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
import re
import urllib.error
import urllib.parse
import urllib.request

from lxml import etree

from dataflow.flow.tasklog.http_util.http_util import common_http_request
from dataflow.flow.tasklog.log_process_util.json_util import extract
from dataflow.flow.tasklog.log_process_util.log_content_util import parse_log_line
from dataflow.flow.tasklog.yarn_api_client.hadoop_conf import get_spark_history_endpoint
from dataflow.flow.tasklog.yarn_util import YarnUtil
from dataflow.shared.log import flow_logger as logger

from .yarn_api_client.hadoop_conf import get_jobhistory_endpoint


class YarnLogHelper(object):
    # the title when node manager fail to redirect to mr_history server
    REDIRECT_TITLE = "Failed redirect for"

    @staticmethod
    def get_app_status(app_id):
        app_status_url = YarnUtil.get_rm().get_active_endpoint() + "/ws/v1/cluster/apps/" + app_id + "/state"
        app_status_response = common_http_request(app_status_url)
        status_dict = {}
        if app_status_response:
            status_dict = eval(app_status_response.text)
        return status_dict.get("state")

    @staticmethod
    def get_all_nodes():
        get_all_nodes_url = YarnUtil.get_rm().get_active_endpoint() + "/ws/v1/cluster/nodes"
        get_all_nodes_response = common_http_request(get_all_nodes_url)
        nodes = []
        if get_all_nodes_response:
            data = json.loads(get_all_nodes_response.text)
            d = dict(data)
            extract(None, d, "nodeHTTPAddress", nodes)
        return nodes

    @staticmethod
    def get_all_container_log_links(nodes):
        container_log_linkss = []
        for x in nodes:
            if x != "":
                get_containers_url = "http://" + x + "/ws/v1/node/containers"
                get_containers_response = common_http_request(get_containers_url)
                if get_containers_response:
                    data = json.loads(get_containers_response.text)
                    d = dict(data)
                    extract(None, d, "containerLogsLink", container_log_linkss)
        return container_log_linkss

    @staticmethod
    def modify_url_qs(url, new_append_path, new_params):
        """
        修改url中的query string
        :param url: 输入url
        :param new_append_path: 新增的url path，只支持往尾部添加，如果已有相同path则不生效
        :param new_params: query string参数pair
        :return: 修改后的url
        """
        url_parse_result = urllib.parse.urlparse(url)
        origin_qs = dict(urllib.parse.parse_qsl(url_parse_result.query))
        if new_params:
            for key, val in list(new_params.items()):
                if val is not None:
                    origin_qs[key] = val
                elif key in list(origin_qs.keys()):
                    origin_qs.pop(key)
        new_qs = urllib.parse.urlencode(origin_qs)
        new_path = (
            url_parse_result.path + "/" + new_append_path
            if new_append_path and new_append_path not in url_parse_result.path
            else url_parse_result.path
        )
        new_parse_result = (
            url_parse_result.scheme,
            url_parse_result.netloc,
            new_path,
            url_parse_result.params,
            new_qs,
            url_parse_result.fragment,
        )
        new_url = urllib.parse.urlunparse(new_parse_result)
        return new_url

    @staticmethod
    def get_one_container_log(url, log_type=None, start=None, end=None, app_user_name=None):
        """
        one container log data (with specified log_type & start & end)
        plain text log data
        """

        # url like this:
        # http://hadoop-xxx:2xxxx/node/containerlogs/container_e270_1584501917228_1720_01_000001/root

        container_log_url = YarnLogHelper.modify_url_qs(url, log_type, {"start": start, "end": end})

        # container_log_url like this:
        # http://hadoop-xxx:2xxxx/node/containerlogs/container_e270_1584501917228_1720_01_000001/root
        # /stdout/?start=100&end=200

        container_log_response = common_http_request(container_log_url)
        if container_log_response:
            logger.info(
                "get container_log_url({}) status_code({})".format(
                    container_log_url, container_log_response.status_code
                )
            )

            container_log_response_content = etree.HTML(container_log_response.text)

            # get title and judge if redirect if needed
            title_xpath = "//title"
            title_content = container_log_response_content.xpath(title_xpath)
            if title_content:
                title = title_content[0].text.strip()
                if YarnLogHelper.REDIRECT_TITLE in title:
                    # redirect
                    redirect_url = YarnLogHelper.get_redirect_container_log_url(container_log_url, app_user_name)
                    container_log_list_response = common_http_request(redirect_url)
                    logger.info(
                        "get_one_container_log_length_info origin url(%s), redirect url(%s)"
                        % (container_log_url, redirect_url)
                    )
                    container_log_response_content = etree.HTML(container_log_list_response.text)

            xpath2 = '//table/tbody/tr/td[@class="content"]/pre[last()]'
            log_content = container_log_response_content.xpath(xpath2)
            if log_content and log_content[0].text:
                return log_content[0].text
        return ""

    @staticmethod
    def get_component_type(log_component_type):
        if log_component_type == "executor":
            return "spark"
        elif log_component_type == "taskmanager" or log_component_type == "jobmanager":
            return "flink"

    @staticmethod
    def get_real_log_type(component_type, log_type):
        if not log_type:
            return None
        if log_type == "stdout":
            if component_type == "taskmanager":
                log_type = "taskmanager.out"
            elif component_type == "jobmanager":
                log_type = "jobmanager.out"
            elif component_type == "executor":
                log_type = "stdout"
        elif log_type == "stderr":
            if component_type == "taskmanager":
                log_type = "taskmanager.err"
            elif component_type == "jobmanager":
                log_type = "jobmanager.err"
            elif component_type == "executor":
                log_type = "stderr"
        elif log_type == "log":
            if component_type == "taskmanager":
                log_type = "taskmanager.log"
            elif component_type == "jobmanager":
                log_type = "jobmanager.log"
        elif log_type == "gc":
            log_type = "gc.log.0.current"
        elif log_type == "prelaunch.err":
            log_type = "prelaunch.err"
        elif log_type == "prelaunch.out":
            log_type = "prelaunch.out"
        return log_type

    @staticmethod
    def get_normalized_log_type(component_type, log_name):
        if not log_name:
            return None
        log_type = None
        if component_type == "taskmanager" or component_type == "jobmanager":
            if log_name == component_type + ".out":
                log_type = "stdout"
            elif log_name == component_type + ".err":
                log_type = "stderr"
            elif log_name == component_type + ".log":
                log_type = "log"
            elif log_name == "gc.log.0.current":
                log_type = "gc"
        if component_type == "executor":
            log_type = log_name

        if log_name == "prelaunch.err":
            log_type = "prelaunch.err"
        elif log_name == "prelaunch.out":
            log_type = "prelaunch.out"
        return log_type

    @staticmethod
    def is_valid_log_name(componet_type, log_name):
        if componet_type == "taskmanager":
            if (
                log_name == "taskmanager.out"
                or log_name == "taskmanager.err"
                or log_name == "taskmanager.log"
                or log_name == "gc.log.0.current"
                or log_name == "prelaunch.out"
                or log_name == "prelaunch.err"
            ):
                return True
        elif componet_type == "jobmanager":
            if (
                log_name == "jobmanager.out"
                or log_name == "jobmanager.err"
                or log_name == "jobmanager.log"
                or log_name == "gc.log.0.current"
                or log_name == "prelaunch.out"
                or log_name == "prelaunch.err"
            ):
                return True
        elif componet_type == "executor":
            if (
                log_name == "stdout"
                or log_name == "stderr"
                or log_name == "prelaunch.out"
                or log_name == "prelaunch.err"
            ):
                return True
        return False

    @staticmethod
    def get_container_info(url, node_id=None):
        """
        parse one container's host_port & container_id & user_name info
        ("hadoop-xxx:2xxxx", "container_e270_1584501917228_1720_01_000001")
        """
        # get container id prefix
        # http://hadoop-nodemanager-87:23999/node/containerlogs/container_e270_1584508_1720_01_000001/root/?start=0
        # return: container_e270_1584501917228_1720_01
        start_index = url.find("/") + 2
        end_index = url.find(":", start_index)
        container_host = url[start_index:end_index]

        start_index2 = url.find("containerlogs/") + len("containerlogs/")
        end_index2 = url.find("/", start_index2)
        container_id = url[start_index2:end_index2]

        if node_id:
            container_host = node_id

        return (container_host, container_id)

    @staticmethod
    def get_container_host(container_host_port):
        """
        return container_host, trip container_port
        :param container_host_port:
        :return:
        """
        container_host = container_host_port
        if ":" in container_host_port:
            container_host = container_host_port[0 : container_host_port.find(":")]
        return container_host

    @staticmethod
    def get_container_log_url(container_host, nm_port, container_id, app_user_name="root"):
        """
        get container url
        "http://hadoop-xxx:2xxxx/node/containerlogs/container_e270_1584501917228_6365_01_000001/root"
        """
        if ":" in container_host:
            container_host = container_host[0 : container_host.find(":")]
        container_log_url = (
            "http://" + container_host + ":" + nm_port + "/node/containerlogs/" + container_id + "/" + app_user_name
        )
        return container_log_url

    @staticmethod
    def get_redirect_container_log_url(from_url, app_user_name):
        """
        from: "http://hadoop-nodemanager-xx:2xxx/node/containerlogs/container_e270_1584501917228_82181_01_000001/root"
        to:   "http://mr-jobhistory.service.xxx.bk:1xxxx/jobhistory/logs/hadoop-nodemanager-xxx:45454/
                  container_e26_1588553843359_1795009_01_000001/container_e26_1588553843359_1795009_01_000001/root"
        """
        # 1. extract nm host and container_id
        nm_host, container_id = YarnLogHelper.get_container_info(from_url)
        end_index = from_url.rfind(app_user_name)
        suffix = from_url[end_index + len(app_user_name) :]
        # 2. generate redirect url
        redirect_log_url = (
            "http://"
            + get_jobhistory_endpoint()
            + "/jobhistory/logs/"
            + nm_host
            + ":"
            + YarnUtil.get_nm_port()
            + "/"
            + container_id
            + "/"
            + container_id
            + "/"
            + app_user_name
            + suffix
        )
        return redirect_log_url

    @staticmethod
    def get_spark_history_log_links(app_id, log_type):
        # http://0.0.0.0:8080/api/v1/applications/application_1584501917228_2956/executors
        # log_type should be stdout/stderr!
        # get_all_executors_url = SPARK_HISTORY_SERVER_URL + '/api/v1/applications/' + app_id + "/executors"
        get_all_executors_url = (
            "http://" + get_spark_history_endpoint() + "/api/v1/applications/" + app_id + "/executors"
        )
        get_all_executors_response = common_http_request(get_all_executors_url)

        log_links = []
        if get_all_executors_response:
            try:
                data = json.loads(get_all_executors_response.text)
                for item in data:
                    if item["executorLogs"]:
                        if log_type:
                            log_links.append(item["executorLogs"][log_type])
                        else:
                            log_links.extend(list(item["executorLogs"].values()))
            except Exception:
                pass
        return log_links

    @staticmethod
    def get_one_container_log_length_info(container_log_url, component_type=None, log_type=None, app_user_name=None):
        """
        one container logs length dict
        {
            "jobmanager.err": "0",
            "jobmanager.log": "15084825",
            ...
        }
        """
        # for flink url
        # http://hadoop-xxx:2xxxx/node/containerlogs/container_e270_1584501917228_6365_01_000001/root
        # for spark url
        # http://hadoop-xxx:2xxxx/node/containerlogs/container_e270_1584501917228_6365_01_000001/root/?start=-2&end=-1
        container_log_list_response = common_http_request(container_log_url)
        ret = {}
        if container_log_list_response:
            logger.info(
                "get container_log_length_url(%s) status_code(%s)"
                % (container_log_url, container_log_list_response.status_code)
            )
            log_type = YarnLogHelper.get_real_log_type(component_type, log_type)
            container_log_response_content = etree.HTML(container_log_list_response.text)

            # get title and judge if redirect if needed
            title_xpath = "//title"
            title_content = container_log_response_content.xpath(title_xpath)
            if title_content:
                title = title_content[0].text.strip()
                if YarnLogHelper.REDIRECT_TITLE in title:
                    # redirect
                    redirect_url = YarnLogHelper.get_redirect_container_log_url(container_log_url, app_user_name)
                    container_log_list_response = common_http_request(redirect_url)
                    logger.info(
                        "get_one_container_log_length_info origin url(%s), redirect url(%s)"
                        % (container_log_url, redirect_url)
                    )
                    container_log_response_content = etree.HTML(container_log_list_response.text)

            if component_type == "executor":
                # for spark executor
                xpath2 = (
                    '//table/tbody/tr/td[@class="content"]/p[contains(text(),"Log Length") or '
                    'contains(text(),"Log Type")]'
                )
                log_list_content = container_log_response_content.xpath(xpath2)
                i = 0
                while i < len(log_list_content):
                    log_name = log_list_content[i].text.strip()
                    log_name = log_name[log_name.find(":") + 2 :]
                    ret_log_name = YarnLogHelper.get_normalized_log_type(component_type, log_name)
                    log_length = log_list_content[i + 1].text.strip()
                    log_length = log_length[log_length.find(":") + 2 :]
                    i = i + 2

                    if log_type:
                        if log_name == log_type:
                            ret[ret_log_name] = log_length
                            return ret
                        else:
                            continue
                    elif YarnLogHelper.is_valid_log_name(component_type, log_name):
                        ret[ret_log_name] = log_length
            elif component_type == "jobmanager" or component_type == "taskmanager":
                # for flink jobmanager/taskmanager
                xpath2 = '//table/tbody/tr/td[@class="content"]/p/a'
                log_list_content = container_log_response_content.xpath(xpath2)
                for one_log_item in log_list_content:
                    # gc.log.0.current : Total file length is 57427 bytes.
                    # jobmanager.err : Total file length is 0 bytes.
                    # jobmanager.log : Total file length is 15084825 bytes.
                    # jobmanager.log.1 : Total file length is 20971551 bytes.
                    # jobmanager.log.2 : Total file length is 20971758 bytes.
                    # jobmanager.log.json : Total file length is 632475 bytes.
                    # jobmanager.out : Total file length is 1783723 bytes.
                    # prelaunch.err : Total file length is 0 bytes.
                    # prelaunch.out : Total file length is 70 bytes.
                    one_log_length_info = one_log_item.text
                    pattern = re.compile(r"(\S+)\s:\D+\s(\d+)\sbytes", re.I)
                    m = pattern.match(one_log_length_info)
                    if m:
                        ret_log_name = YarnLogHelper.get_normalized_log_type(component_type, m.group(1))
                        log_name = m.group(1)
                        if log_type:
                            if log_name == log_type:
                                ret[ret_log_name] = m.group(2)
                                return ret
                            else:
                                continue
                        elif YarnLogHelper.is_valid_log_name(component_type, log_name):
                            ret[ret_log_name] = m.group(2)
        return ret

    @staticmethod
    def get_all_container_log_len_from_yarn(containers_dict, component_type, log_type, app_user_name="root"):
        ret = {}
        containers_dict = list(set(containers_dict))
        for one_container in containers_dict:
            container_host_port = one_container[0]
            container_id = one_container[1]
            one_container_log_link = YarnLogHelper.get_container_log_url(
                container_host_port,
                YarnUtil.get_nm_webapp_port(),
                container_id,
                app_user_name,
            )

            one_ret_log_length_dict = YarnLogHelper.get_one_container_log_length_info(
                one_container_log_link, component_type, log_type, app_user_name
            )
            container_str = container_host_port + "@" + container_id
            if container_str in list(ret.keys()):
                ret[container_str].update(one_ret_log_length_dict)
            else:
                ret[container_str] = one_ret_log_length_dict
        return ret

    @staticmethod
    def get_flink_job_overview(jm):
        """
        get all {"job_name": "jid"} from jm
        {
            "13035_ba67665107a54266a57d4506ab07f4d9": "df8f1dc7a7d32ce292b646708865f93e",
            "13035_a7df53204d4144cd93b6caa8684092c3": "9f9c396792b6c0c1fb0ca12434a68f32"
        }
        """
        ret = {}
        get_flink_job_overview_url = jm + "/jobs/overview"
        get_all_flink_jobs_response = common_http_request(get_flink_job_overview_url)

        if get_all_flink_jobs_response:
            try:
                all_jobs = json.loads(get_all_flink_jobs_response.text)
                for one_job in all_jobs["jobs"]:
                    if one_job["state"] in ["RUNNING", "RESTARTING", "FAILING"]:
                        ret[one_job["name"]] = one_job["jid"]
            except Exception:
                pass
        return ret

    @staticmethod
    def get_flink_all_task_managers(jm):
        """
        get all {task manager host_port: container id} dict
        {
            'hadoop-xxx:4xxxx': 'container_e270_1584501917228_6365_01_000008'
        }
        """
        ret = {}
        get_all_task_managers_url = jm + "/taskmanagers"
        get_all_task_managers_response = common_http_request(get_all_task_managers_url)

        if get_all_task_managers_response:
            try:
                all_task_managers = json.loads(get_all_task_managers_response.text)
                for one_task_manager in all_task_managers["taskmanagers"]:
                    # dataPort <-> containerId
                    akka_host = one_task_manager["path"]
                    start_index = akka_host.find("@")
                    end_index = akka_host.find(":", start_index)
                    host = akka_host[start_index + 1 : end_index]
                    ret[host + ":" + str(one_task_manager["dataPort"])] = one_task_manager["id"]
            except Exception:
                pass
        return ret

    @staticmethod
    def get_flink_job_vertices(jm, jid):
        """
        get [vertice id] list for a job(using jid)
        [
            "cbc357ccb763df2852fee8c4fc7d55f2",
            ...
        ]
        """
        ret = []
        get_flink_job_info_url = jm + "/jobs/" + jid
        get_flink_job_info = common_http_request(get_flink_job_info_url)

        if get_flink_job_info:
            try:
                job_info = json.loads(get_flink_job_info.text)
                for one_job in job_info["vertices"]:
                    ret.append(one_job["id"])
            except Exception:
                pass
        return ret

    @staticmethod
    def get_flink_job_exception(jm, jid, tm_location=None):
        ret = ""
        get_flink_job_exception_url = jm + "/jobs/" + jid + "/exceptions"
        get_flink_exceptions_info = common_http_request(get_flink_job_exception_url)

        if get_flink_exceptions_info:
            try:
                job_exceptions = json.loads(get_flink_exceptions_info.text)
                if not tm_location:
                    # return root-exception
                    ret = job_exceptions["root-exception"]
                else:
                    # return per-taskmanager exception
                    for one_tm_exception in job_exceptions["all-exceptions"]:
                        if one_tm_exception and one_tm_exception["location"] == tm_location:
                            ret = one_tm_exception["exception"]
                            break
            except Exception:
                pass
        if not ret:
            ret = ""
        return ret

    @staticmethod
    def get_flink_job_task_managers(jm, jid, vertice):
        """
        get task manger list running the job
        [
            'hadoop-xxx:5xxxx',
            'hadoop-xxx:3xxxx'
        ]
        """
        ret = []
        get_task_managers_url = jm + "/jobs/" + jid + "/vertices/" + vertice + "/taskmanagers"
        get_task_managers_info = common_http_request(get_task_managers_url)

        if get_task_managers_info:
            try:
                task_managers_info = json.loads(get_task_managers_info.text)
                for one_task_manager in task_managers_info["taskmanagers"]:
                    ret.append(one_task_manager["host"])
            except Exception:
                pass
        return ret

    @staticmethod
    def get_flink_job_containers(all_task_managers, job_task_managers):
        """
        get [(task manager host:port, container id)] list for a job
        [
            (u'hadoop-xxx:3xxxx', u'container_e270_1584501917228_6365_01_000009'),
            (u'hadoop-xxx:4xxxx', u'container_e270_1584501917228_6365_01_000007'),
            (u'hadoop-xxx:5xxxx', u'container_e270_1584501917228_6365_01_000008')
        ]
        """
        ret = []
        for one_tm in job_task_managers:
            if one_tm in list(all_task_managers.keys()):
                ret.append((one_tm, all_task_managers[one_tm]))
        return ret

    @staticmethod
    def get_log_entry_from_yarn(
        containers_dict,
        log_component_type,
        log_type,
        process_start,
        process_end,
        search_words,
        log_format,
        app_user_name,
    ):
        ret = []
        for one_container in containers_dict:
            container_host = one_container[0]
            container_id = one_container[1]
            # one_container_log_link like this:
            # http://hadoop-nodemanager-87:23999/node/containerlogs/container_e270_1584501917228_1720_01_000001/root
            one_container_log_link = YarnLogHelper.get_container_log_url(
                container_host, YarnUtil.get_nm_webapp_port(), container_id, app_user_name
            )
            log_type = YarnLogHelper.get_real_log_type(log_component_type, log_type)
            # spark or flink
            component_type = YarnLogHelper.get_component_type(log_component_type)

            one_ret_log = YarnLogHelper.get_one_container_log(
                one_container_log_link, log_type, process_start, process_end, app_user_name
            )

            one_container_info = YarnLogHelper.get_container_info(one_container_log_link)
            logger.info(
                "get_container_info({}), container_log_links({})".format(one_container_info, one_container_log_link)
            )

            processed_start_end = YarnLogHelper.get_processed_start_end(one_ret_log, process_start, process_end)
            one_log_result = YarnLogHelper.process_one_log_result(
                one_ret_log,
                container_host,
                container_id,
                log_type,
                processed_start_end[0],
                processed_start_end[1],
                search_words,
                log_format,
                component_type,
            )
            ret.append(one_log_result)

        return ret

    @staticmethod
    def get_yarn_log_length(
        app_id,
        job_name,
        component_type,
        log_type=None,
        container_host=None,
        container_id=None,
        app_user_name="root",
    ):

        # get specified container log for (container_host, container_id)
        if container_host and container_id:
            log_containers_list = [(container_host, container_id)]
            log_length_dict = YarnLogHelper.get_all_container_log_len_from_yarn(
                log_containers_list, component_type, log_type, app_user_name
            )
            return log_length_dict
        # get spark log length
        if component_type == "executor":
            # get app status
            # “NEW”, “NEW_SAVING”, “SUBMITTED”, “ACCEPTED”, “RUNNING”, “FINISHED”, “FAILED”, “KILLED”
            app_status = YarnLogHelper.get_app_status(app_id)

            log_status = ""
            if (
                app_status == "NEW"
                or app_status == "NEW_SAVING"
                or app_status == "SUBMITTED"
                or app_status == "ACCEPTED"
            ):
                logger.warning("app_id({}), status({}), has no log".format(app_id, app_status))
                return ["app_id({}), status({}), has no log".format(app_id, app_status)]
            elif app_status == "RUNNING":
                # get running log
                log_status = "RUNNING"
            else:
                # get history log
                log_status = "FINISHED"

            logger.info(
                "get_app_status, app_id({}), app_status({}), log_status({})".format(app_id, app_status, log_status)
            )
            rm = YarnUtil.get_rm()
            # the app maybe running or finished, so we try both get log links from yarn rm or spark history server
            # even the app is finished, we should try both way
            # the log should not be found in both way, only one way!
            spark_running_log_links = []
            spark_finished_log_links = []
            if log_status == "FINISHED":
                spark_finished_log_links = YarnLogHelper.get_spark_history_log_links(app_id, log_type)
                if not spark_finished_log_links:
                    spark_running_log_links = YarnUtil.get_containers_log_list(rm, app_id)
            else:
                spark_running_log_links = YarnUtil.get_containers_log_list(rm, app_id)
                if not spark_running_log_links:
                    spark_finished_log_links = YarnLogHelper.get_spark_history_log_links(app_id, log_type)
            spark_log_links = spark_running_log_links + spark_finished_log_links
            ret = {}

            logger.info(
                "spark_log_links, app_id(%s), spark_running_log_links(%s), spark_finished_log_links(%s)"
                % (
                    app_id,
                    ";".join(spark_running_log_links),
                    ";".join(spark_finished_log_links),
                )
            )

            # 获取任务日志长度
            for one_container_log_link in spark_log_links:
                if one_container_log_link:
                    one_container_info = YarnLogHelper.get_container_info(one_container_log_link)
                    logger.info(
                        "get_container_info({}), container_log_link({})".format(
                            one_container_info, one_container_log_link
                        )
                    )

                    # modify one_container_log_link
                    # add "/?start=-2&end=-1"
                    one_container_log_link = YarnLogHelper.modify_url_qs(
                        one_container_log_link, None, {"start": -2, "end": -1}
                    )
                    one_ret_log_length = YarnLogHelper.get_one_container_log_length_info(
                        one_container_log_link, component_type, log_type, app_user_name
                    )
                    container_host_port = one_container_info[0]
                    container_id = one_container_info[1]
                    container_host = YarnLogHelper.get_container_host(container_host_port)
                    container_str = container_host + "@" + container_id
                    if container_str in list(ret.keys()):
                        ret[container_str].update(one_ret_log_length)
                    else:
                        ret[container_str] = one_ret_log_length

            ret["log_status"] = log_status
            return ret
        else:
            # get flink log length
            job_log_length_dict = {}
            am_info_tuple = YarnUtil.get_am_info(YarnUtil.get_rm(), app_id)
            if not am_info_tuple:
                # can not get am info, maybe app is killed
                return job_log_length_dict
            logger.info("get_am_tracking_url({})".format(am_info_tuple))

            jm = am_info_tuple[0]
            jm_log_url = am_info_tuple[2]
            jm_node_id = am_info_tuple[4]

            if component_type == "jobmanager":
                # http://hadoop-xxx:2xxxx/node/containerlogs/container_e270_1584501917228_6365_01_000001/root
                job_containers_list = [YarnLogHelper.get_container_info(jm_log_url, jm_node_id)]
                job_log_length_dict = YarnLogHelper.get_all_container_log_len_from_yarn(
                    job_containers_list, component_type, log_type, app_user_name
                )
            elif component_type == "taskmanager":
                job_name_jid_dict = YarnLogHelper.get_flink_job_overview(jm)
                logger.info("get_flink_job_overview(%s)" % (job_name_jid_dict))
                host_container_dict = YarnLogHelper.get_flink_all_task_managers(jm)
                logger.info("get_flink_all_task_managers(%s)" % (host_container_dict))

                if job_name in list(job_name_jid_dict.keys()):
                    jid = job_name_jid_dict[job_name]
                    vertices = YarnLogHelper.get_flink_job_vertices(jm, jid)
                    logger.info("get_flink_job_vertices(%s)" % (vertices))

                    job_tm_list = []
                    for one_vertice in vertices:
                        job_tm_list.extend(YarnLogHelper.get_flink_job_task_managers(jm, jid, one_vertice))

                    logger.info("job_tm_list(%s)" % (job_tm_list))

                    # job_containers_list: [(containers_host_port, container_id)]
                    job_containers_list = YarnLogHelper.get_flink_job_containers(host_container_dict, job_tm_list)

                    logger.info("job_containers_list(%s)" % (job_containers_list))

                    # {container_host_port@container_id, {stdout: 100, stderr:200, log:300} }
                    job_log_length_dict = YarnLogHelper.get_all_container_log_len_from_yarn(
                        job_containers_list, component_type, log_type, app_user_name
                    )

            # flink task status set to 'RUNNING'
            job_log_length_dict["log_status"] = "RUNNING"
            return job_log_length_dict

    @staticmethod
    def process_one_log_result(
        one_ret_log,
        process_start,
        process_end,
        search_words=None,
        log_format="json",
        component_type="flink",
    ):
        first_new_line_pos = one_ret_log.find("\n")
        if process_start == 0:
            first_new_line_pos = 0
        last_new_line_pos = one_ret_log.rfind("\n")
        one_log_result = {"inner_log_data": []}
        if first_new_line_pos <= last_new_line_pos:
            # if first_new_line_pos != 0:
            #     first_new_line_pos = first_new_line_pos + 1
            one_ret_log = one_ret_log[first_new_line_pos:last_new_line_pos]

            if log_format == "text":
                one_log_result = {
                    "pos_info": {
                        "process_start": process_start + first_new_line_pos,
                        "process_end": process_start + last_new_line_pos,
                    },
                    "read_bytes": last_new_line_pos - first_new_line_pos,
                    "line_count": 1,
                    "inner_log_data": [one_ret_log],
                }
            elif log_format == "json":
                json_log_list = parse_log_line(
                    log_content=one_ret_log, search_words=search_words, log_platform_type=component_type
                )
                one_log_result = {
                    "pos_info": {
                        "process_start": process_start + first_new_line_pos,
                        "process_end": process_start + last_new_line_pos,
                    },
                    "read_bytes": last_new_line_pos - first_new_line_pos,
                    "line_count": len(json_log_list),
                    "inner_log_data": json_log_list,
                }
        return one_log_result

    @staticmethod
    def get_processed_start_end(one_ret_log, process_start, process_end):
        one_start = process_start
        one_end = process_end
        if one_ret_log is not None:
            if process_start < 0:
                # start<0 & end < 0
                one_start = (
                    process_start if len(one_ret_log) == process_end - process_start else process_end - len(one_ret_log)
                )
            if process_end > 0:
                # start>=0 & end > 0
                one_end = (
                    process_end if len(one_ret_log) == process_end - process_start else process_start + len(one_ret_log)
                )
        return one_start, one_end

    @staticmethod
    def get_spark_job_log(
        app_id,
        component_type,
        log_type,
        process_start=None,
        process_end=None,
        container_host=None,
        container_id=None,
        search_words=None,
        log_format="json",
        app_user_name="root",
    ):

        # 1. http://xx.x.xx.xxx:8xxx/proxy/application_1584501917228_1720

        # 2. [自动跳转，解析出3] http://xx.x.xx.xxx:8081/cluster/app/application_1584501917228_1720

        # 3. http://hadoop-xxx:2xxxxx/node/containerlogs/container_e270_1584501917228_1720_01_000001/root

        # 4. [自动跳转]http://hadoop-xxx:2xxxxx:8x/jobhistory/logs/hadoop-xxx:4xxxxx/
        #       container_e270_1584501917228_1720_01_000001/container_e270_1584501917228_1720_01_000001/root

        if container_host and container_id:
            # get specified container log
            specified_containers_list = [(container_host, container_id)]

            # [(container_host + container_id, log)]
            specified_log_list = YarnLogHelper.get_log_entry_from_yarn(
                specified_containers_list,
                component_type,
                log_type,
                process_start,
                process_end,
                search_words,
                log_format,
                app_user_name,
            )
            return specified_log_list

        # get app status
        # “NEW”, “NEW_SAVING”, “SUBMITTED”, “ACCEPTED”, “RUNNING”, “FINISHED”, “FAILED”, “KILLED”
        app_status = YarnLogHelper.get_app_status(app_id)

        log_status = ""
        if app_status == "NEW" or app_status == "NEW_SAVING" or app_status == "SUBMITTED" or app_status == "ACCEPTED":
            logger.warning("app_id({}), status({}), has no log".format(app_id, app_status))
            return ["app_id({}), status({}), has no log".format(app_id, app_status)]
        elif app_status == "RUNNING":
            # get running log
            log_status = "RUNNING"
        else:
            # get history log
            log_status = "FINISHED"

        logger.info("get_app_status, app_id({}), app_status({}), log_status({})".format(app_id, app_status, log_status))

        rm = YarnUtil.get_rm()
        # the app maybe running or finished, so we try both get log links from yarn rm or spark history server
        # even the app is finished, we should try both way
        # the log should not be found in both way, only one way!
        spark_running_log_links = []
        spark_finished_log_links = []
        if log_status == "FINISHED":
            spark_finished_log_links = YarnLogHelper.get_spark_history_log_links(app_id, log_type)
            if not spark_finished_log_links:
                spark_running_log_links = YarnUtil.get_containers_log_list(rm, app_id)
        else:
            spark_running_log_links = YarnUtil.get_containers_log_list(rm, app_id)
            if not spark_running_log_links:
                spark_finished_log_links = YarnLogHelper.get_spark_history_log_links(app_id, log_type)
        spark_log_links = spark_running_log_links + spark_finished_log_links
        ret = []

        # 获取任务运行日志
        for one_container_log_link in spark_log_links:
            if one_container_log_link:
                one_container_info = YarnLogHelper.get_container_info(one_container_log_link)
                logger.info(
                    "get_container_info({}), container_log_link({})".format(one_container_info, one_container_log_link)
                )

                one_ret_log = YarnLogHelper.get_one_container_log(
                    one_container_log_link, log_type, process_start, process_end, app_user_name
                )
                processed_start_end = YarnLogHelper.get_processed_start_end(one_ret_log, process_start, process_end)
                one_log_result = YarnLogHelper.process_one_log_result(
                    one_ret_log,
                    processed_start_end[0],
                    processed_start_end[1],
                    search_words,
                    log_format,
                    "spark",
                )
                ret.append(one_log_result)

        return ret

    @staticmethod
    def get_flink_job_log(
        app_id,
        job_name,
        log_component_type,
        log_type,
        process_start,
        process_end,
        container_host=None,
        container_id=None,
        search_words=None,
        log_format="json",
        app_user_name="dataflow",
    ):
        # get log from flink
        # "trackingUrl":"hadoop-xxx:8xxx/proxy/application_1584501917228_6365/"
        # "container_id":"container_e270_1584501917228_6365_01_000003"
        # "log_type":"log/stdout/stderr..."
        # get log:
        #   http://xx.x.xx.xxx:8xxx/proxy/:appid/taskmanagers/:containerid/log
        #   http://xx.x.xx.xxx:8xxx/proxy/:appid/taskmanagers/:containerid/stdout

        # get log from yarn
        # http://xx.x.xx.xxx:8xxx/proxy/application_158101_0111/taskmanagers/container_e255_158201_0111_01_001641/log

        if container_host and container_id and (log_type is None or log_type != "exception"):
            # get specified container log
            specified_containers_list = [(container_host, container_id)]

            # [(container_host + container_id, log)]
            specified_log_list = YarnLogHelper.get_log_entry_from_yarn(
                specified_containers_list,
                log_component_type,
                log_type,
                process_start,
                process_end,
                search_words,
                log_format,
                app_user_name,
            )
            return specified_log_list

        job_tm_log_list = []
        am_info_tuple = YarnUtil.get_am_info(YarnUtil.get_rm(), app_id)
        if not am_info_tuple:
            # can not get am info, maybe app is killed
            return job_tm_log_list
        logger.info("get_am_tracking_url({})".format(am_info_tuple))

        jm = am_info_tuple[0]
        jm_log_url = am_info_tuple[2]

        if log_component_type == "jobmanager":
            if log_type and log_type == "exception":
                job_name_jid_dict = YarnLogHelper.get_flink_job_overview(jm)
                logger.info("get_flink_job_overview(%s)" % (job_name_jid_dict))
                host_container_dict = YarnLogHelper.get_flink_all_task_managers(jm)
                logger.info("get_flink_all_task_managers(%s)" % (host_container_dict))

                if job_name in list(job_name_jid_dict.keys()):
                    jid = job_name_jid_dict[job_name]
                    job_exception_msg = YarnLogHelper.get_flink_job_exception(jm, jid)
                    logger.info("job({}), exception({})".format(job_name, job_exception_msg))
                    one_log_result = {
                        "pos_info": {"process_start": 0, "process_end": len(job_exception_msg)},
                        "inner_log_data": [
                            {"log_time": "", "log_level": "", "log_source": "", "log_content": job_exception_msg}
                        ],
                        "read_bytes": len(job_exception_msg),
                        "line_count": 1,
                    }
                    return [one_log_result]

            job_jm_log_list = []
            # http://hadoop-nodemanager-88:23999/node/containerlogs/container_e270_1584501917228_6365_01_000001/root
            job_containers_list = []
            jm_container_info = YarnLogHelper.get_container_info(jm_log_url)
            # if container_id and container_id != jm_container_info[1]:
            #     return []
            job_containers_list.append(jm_container_info)
            job_jm_log_list = YarnLogHelper.get_log_entry_from_yarn(
                job_containers_list,
                log_component_type,
                log_type,
                process_start,
                process_end,
                search_words,
                log_format,
                app_user_name,
            )
            return job_jm_log_list
        elif log_component_type == "taskmanager":
            job_name_jid_dict = YarnLogHelper.get_flink_job_overview(jm)
            logger.info("get_flink_job_overview(%s)" % (job_name_jid_dict))
            host_container_dict = YarnLogHelper.get_flink_all_task_managers(jm)
            logger.info("get_flink_all_task_managers(%s)" % (host_container_dict))

            if job_name in list(job_name_jid_dict.keys()):
                jid = job_name_jid_dict[job_name]
                vertices = YarnLogHelper.get_flink_job_vertices(jm, jid)
                logger.info("get_flink_job_vertices(%s)" % (vertices))

                job_tm_list = []
                for one_vertice in vertices:
                    job_tm_list.extend(YarnLogHelper.get_flink_job_task_managers(jm, jid, one_vertice))
                job_tm_list = list(set(job_tm_list))
                logger.info("job_tm_list(%s)" % (job_tm_list))

                # job_containers_list: (container_host_port, container_id)
                job_containers_list = YarnLogHelper.get_flink_job_containers(host_container_dict, job_tm_list)

                logger.info("job_containers_list(%s)" % (job_containers_list))

                if log_type and log_type == "exception":
                    for one_container_info in job_containers_list:
                        one_container_host_port = one_container_info[0]
                        one_container_id = one_container_info[1]
                        if container_id == one_container_id:
                            job_exception_msg = YarnLogHelper.get_flink_job_exception(jm, jid, one_container_host_port)
                            logger.info("job({}), exception({})".format(job_name, job_exception_msg))
                            one_log_result = {
                                "pos_info": {"process_start": 0, "process_end": len(job_exception_msg)},
                                "inner_log_data": [
                                    {
                                        "log_time": "",
                                        "log_level": "",
                                        "log_source": "",
                                        "log_content": job_exception_msg,
                                    }
                                ],
                                "read_bytes": len(job_exception_msg),
                                "line_count": 1,
                            }
                            return [one_log_result]
                    # if no container in list, return empty log
                    empty_exception_log_result = {
                        "pos_info": {"process_start": 0, "process_end": 0},
                        "inner_log_data": [{"log_time": "", "log_level": "", "log_source": "", "log_content": ""}],
                        "read_bytes": 0,
                        "line_count": 1,
                    }
                    return [empty_exception_log_result]

                # [(container_host + container_id, log)]
                job_tm_log_list = YarnLogHelper.get_log_entry_from_yarn(
                    job_containers_list,
                    log_component_type,
                    log_type,
                    process_start,
                    process_end,
                    search_words,
                    log_format,
                    app_user_name,
                )

            return job_tm_log_list
        else:
            return []
