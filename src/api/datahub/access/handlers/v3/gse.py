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
import os
import random
import time

from common.exceptions import ValidationError
from common.log import logger
from conf import dataapi_settings
from datahub.access.api import JobApi
from datahub.access.collectors.utils.bk_cloud_util import (
    change_plat_id_to_standard_style,
)
from datahub.access.collectors.utils.status import TASK_IP_LOG_STATUS
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.access.utils.base64_utils import b64encode
from django.utils.translation import ugettext as _

CLOUDS_BIZ = 200000
GSEAPI_ASYNC_CHECK_WAITING_TIME = 10


class GseHandler(object):

    SUCCESS_CODE = 9
    PENDING_CODE = 5
    PLUGIN_CONF_PATH = dataapi_settings.PLUGIN_CONF_PATH

    def __init__(
        self,
        task_id,
        bk_biz_id,
        conf_path,
        setup_path,
        proc_name,
        pid_path,
        host_config,
        account="root",
        interval=3,
        retry_times=20,
        bk_username="",
    ):
        self.bk_biz_id = bk_biz_id
        self.conf_path = conf_path
        self.setup_path = setup_path
        self.proc_name = proc_name
        self.pid_path = pid_path
        self.interval = interval
        self.retry_times = retry_times
        self.account = account
        self.task_id = task_id
        self.host_config = host_config
        self.bk_username = bk_username
        self.TASK_OPERATOR = bk_username

    def check_process_exists(self, hosts):
        """
        检查进程是否存在
        :return: {
                "exists": ["x.x.x.x"],
                "not_exists": ["x.x.x.x"],
                "error": [{"obj": "x.x.x.x", "errmsg": xxx}]
            }
        """

        logger.info(_(u"检查进程是否存在"))
        ret = self._do_proc_op(hosts, "check_proc_status")

        exists = []
        not_exists = []

        # 判断进程是否存在
        for exit_code in ret:
            if not exit_code.get("content"):
                not_exists.append({"ip": exit_code["ip"], "bk_cloud_id": exit_code["bk_cloud_id"]})
            else:
                content = json.loads(exit_code["content"])
                if content["process"] and content["process"][0]["instance"][0]["pid"] == -1:
                    not_exists.append({"ip": exit_code["ip"], "bk_cloud_id": exit_code["bk_cloud_id"]})
                else:
                    exists.append({"ip": exit_code["ip"], "bk_cloud_id": exit_code["bk_cloud_id"]})

        # 如果不存在全部不存在则直接为失败
        if not exists and not not_exists:
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_NOT_EXSIT_PROC,
                errors=u"检查是否存在采集器进程操作，没有结果",
                message=u"无法检查出采集器进程状态,请重试",
            )
        return {"exists": exists, "not_exists": not_exists, "exit_code": ret}

    def do_proc_manage(self, hosts, param_config):
        """
        # 托管进程
        :param hosts:
        :param op_type:
        :param cycle_time:
        :return:
        """
        if not hosts:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_HOSTS)

        if param_config["op_type"] == "register":
            op_type = 1
        elif param_config["op_type"] == "cancle":
            op_type = 2
        else:
            # 无该操作进程状态
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_NO_JOB_OPR_PROC_TYPE,
                errors=u"没有操作进程类型",
            )

        params = {
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
            "app_id": self.bk_biz_id,
            "oper_type": op_type,
            "process_infos": [
                {
                    "ip_list": [
                        {
                            "ip": host["ip"],
                            "source": change_plat_id_to_standard_style(
                                host["bk_cloud_id"], dataapi_settings.INSTALL_PACAKAGE
                            ),
                        }
                        for host in hosts
                    ],
                    "proc_name": self.proc_name,
                    "setup_path": self.setup_path,
                    "proc_username": self.account,  # param_config['bk_username'],
                    "contact": param_config["bk_username"],
                    "cpu_lmt": dataapi_settings.COLLECTOR_DEFAULT_CPU_LIMIT,
                    "start_cmd": "./{}.sh {}".format(self.proc_name, param_config["raw_data_id"]),
                    "mem_lmt": dataapi_settings.COLLECTOR_DEFAULT_MEM_LIMIT,
                }
            ],
        }

        if param_config.get("pid_path"):
            params["process_infos"][0]["pid_path"] = param_config["pid_path"]

        if op_type == 1:
            params["process_infos"][0]["type"] = 0
            params["process_infos"][0]["cycle_time"] = int(param_config["cycle_time"])
        else:
            params["process_infos"][0]["type"] = 3

        logger.info(u"Hosting configuration params: %s" % params)
        ret = JobApi.gse_process_manage(params)
        if not ret.is_success():
            logger.error(
                _(u"调用post:gse_process_manage失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id)
            )
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                errors=u"gse_process_manage调用失败,托管进程失败,原因:" + ret.message,
                message=_(u"托管进程失败:{}").format(ret.message),
            )

        seqid = ret.data["gse_task_id"]

        ret = self._get_proc_rst(seqid)

        return ret

    def _do_proc_op(self, hosts, op_type):
        """
        调用gse接口操作进程
        :param op_type: 操作类型
        :return:
        """

        if not hosts:
            return []

        params = self._op_proc_params(hosts, op_type)
        ret = JobApi.gse_proc_operate(params)
        if not ret.is_success():
            logger.error(
                _(u"调用post:gse_proc_operate失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id)
            )
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                errors=u"gse_proc_operate调用失败,检查采集器失败,原因:" + ret.message,
                message=_(u"检查采集器进程失败:{}").format(ret.message),
            )

        seqid = ret.data["gse_task_id"]

        ret = self._get_proc_rst(seqid)

        return ret

    def _op_proc_params(self, hosts, op_type):
        """构建进程操作参数

        Args:
            op_type: 操作类型

        Returns:
            操作参数（hash）
        """

        if op_type == "start":
            # 启动进程
            op_type = 0
        elif op_type == "stop":
            # 停止进程
            op_type = 1
            # 重启进程
        elif op_type == "restart":
            op_type = 7
        elif op_type == "check_proc_status":
            op_type = 2
        else:
            # 无该操作进程状态
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_NO_JOB_OPR_PROC_TYPE,
                errors=u"没有操作进程类型",
            )

        if self.account == dataapi_settings.LINUX_DEFAULT_ACCOUNT:
            cmd_shell_ext = "sh"

        else:
            # 不管内部版和企业版,只要不是root ,内部版 adminstrator 企业版 system
            cmd_shell_ext = "bat"

        # todo 暂时都是sh
        cmd_shell_ext = "sh"
        params = {
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
            "app_id": self.bk_biz_id,
            "operate_type": op_type,
            "proc_list": [
                {
                    "ip_list": [
                        {
                            "ip": host["ip"],
                            "source": change_plat_id_to_standard_style(
                                host["bk_cloud_id"], dataapi_settings.INSTALL_PACAKAGE
                            ),
                        }
                        for host in hosts
                    ],
                    "setup_path": self.setup_path,
                    "proc_name": self.proc_name,
                    "pid_path": self.pid_path,
                    "username": self.account,
                    "cmd_shell_ext": cmd_shell_ext,
                    "cpu_lmt": dataapi_settings.COLLECTOR_DEFAULT_CPU_LIMIT,
                    "mem_lmt": dataapi_settings.COLLECTOR_DEFAULT_MEM_LIMIT,
                }
            ],
        }
        return params

    def _get_proc_rst(self, seqid, interval=3, retry_times=20):
        """获取进程操作类执行的结果
        Args:
            seqid: gse命令执行id
            interval: 查询结果间隔
            retry_times: 查询结果重试次数

        Returns:
            执行结果
            {
                "result": True,
                "gse_result": {
                    "success": [{"obj": 'x.x.x.x', 'data': xxx}],
                    "pending": [{"obj": "x.x.x.x"}],
                    "error": [{"obj": "x.x.x.x", "errmsg": xxx}]
                 }
            }


        """
        params = {
            "app_id": self.bk_biz_id,
            "gse_task_id": seqid,
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }

        logger.info(u"get_proc_result_params:%s" % params)

        # 获取结果前，sleep 10s
        time.sleep(GSEAPI_ASYNC_CHECK_WAITING_TIME)
        while retry_times > 0:
            retry_times -= 1
            ret = JobApi.get_proc_result(params)
            logger.info(_(u"get_proc_result result:{}, task_id:{}").format(ret.data, self.task_id))

            if not ret.is_success():
                logger.error(
                    _(u"调用get:get_proc_result失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id)
                )
                raise CollectorError(
                    error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                    errors=u"get_proc_result调用失败,获取检查采集器任务结果失败,原因:" + ret.message,
                    message=_(u"获取进程操作类执行的结果失败:{}").format(ret.message),
                )

            is_finished, gse_result = self.judge_gse_result(ret.data)

            if is_finished:
                return gse_result

            time.sleep(interval)
        # 如果还存在pending状态的host,置为失败
        if not gse_result:
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_TIME_OUT,
                errors=u"get_proc_result调用没有返回内容,原因:" + ret.message,
                message=_(u"获取进程操作类执行的结果失败:{}").format(ret.message),
            )
        for exit_code in gse_result:
            if exit_code["status"] == TASK_IP_LOG_STATUS.PENDING_CODE:
                exit_code["status"] == TASK_IP_LOG_STATUS.FAIL_CODE
                exit_code["error_msg"] == u"操作进程超时,操作进程处于处理中状态,经过多次重试仍然无法确定"

        return gse_result

    def sync_push_file(self, hosts, content):
        """推送文件到目标机器

        Args:
            hosts: ip列表
            content: 文件内容
        Returns:
            异步接口轮训结果
        """

        logger.info(_(u"开始推送文件到目标机器"))

        params = {
            "app_id": self.bk_biz_id,
            "target_path": os.path.dirname(self.conf_path),
            "file_list": [
                {
                    "file_name": os.path.basename(self.conf_path),
                    "content": b64encode(content),
                }
            ],
            "ip_list": [
                {
                    "ip": host["ip"],
                    "source": change_plat_id_to_standard_style(host["bk_cloud_id"], dataapi_settings.INSTALL_PACAKAGE),
                }
                for host in hosts
            ],
            "account": self.account,
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }
        ret = JobApi.gse_push_file(params)

        if not ret.is_success():
            logger.error(_(u"调用post:gse_push_file失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id))
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                errors=u"gse_push_file调用失败,下发配置失败,原因:" + ret.message,
                message=_(u"下发配置失败:{}").format(ret.message),
            )

        task_id = ret.data["taskInstanceId"]
        return self._check_task_ip_log_result(task_id)

    def sync_push_file_patch(self, hosts, files):
        """推送文件到目标机器

        Args:
            hosts: ip列表
            content: 文件内容
        Returns:
            异步接口轮训结果
        """
        logger.info(_(u"开始批量推送文件到目标机器"))
        params = {
            "app_id": self.bk_biz_id,
            "target_path": os.path.dirname(self.conf_path),
            "file_list": [
                {
                    "file_name": _file["file_name"],
                    "content": b64encode(bytes(_file["content"], "utf-8")),
                }
                for _file in files
            ],
            "ip_list": [
                {
                    "ip": host["ip"],
                    "source": change_plat_id_to_standard_style(host["bk_cloud_id"], dataapi_settings.INSTALL_PACAKAGE),
                }
                for host in hosts
            ],
            "account": self.account,
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }

        ret = JobApi.gse_push_file(params)

        if not ret.is_success():
            logger.error(_(u"调用post:gse_push_file失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id))
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                errors=u"gse_push_file调用失败,下发配置失败,原因:" + ret.message,
                message=_(u"下发配置失败:{}").format(ret.message),
            )

        task_id = ret.data["taskInstanceId"]
        return self._check_task_ip_log_result(task_id)

    def fast_push_package(self, taget_hosts, package_config):
        logger.info(_(u"开始下载安装包"))
        if not self.host_config:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_HOST_CONFIG)
        src_hosts = []
        index = random.randint(1, 10) % len(self.host_config)
        # for config in self.host_config:
        src_hosts.append(
            {
                "ip": self.host_config[index]["ip"],
                "source": self.host_config[index]["source"],
            }
        )

        files = list()
        files.append(package_config["package"])
        if package_config.get("script"):
            files.append(package_config["script"])

        params = {
            "app_id": self.bk_biz_id,
            "file_target_path": package_config["target_path"],
            "ip_list": [
                {
                    "ip": host["ip"],
                    "source": change_plat_id_to_standard_style(host["bk_cloud_id"], dataapi_settings.INSTALL_PACAKAGE),
                }
                for host in taget_hosts
            ],
            "account": self.account,
            "file_source": [
                {
                    "ip_list": src_hosts,
                    "account": self.account,  # use linux server as file server
                    "files": files,
                }
            ],
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }

        ret = JobApi.fast_push_file(params)
        if not ret.is_success():
            logger.error(_(u"调用post:fast_push_file失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id))
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                errors=u"fast_push_file调用失败,下载安装包失败,原因:" + ret.message,
                message=_(u"下载安装包失败:{}").format(ret.message),
            )

        task_id = ret.data["taskInstanceId"]

        return self._check_task_ip_log_result(task_id)

    def install_package(self, taget_hosts, src_hosts):
        pass

    def exc_script(self, hosts, content):
        """
        执行脚本
        :param hosts: 主机列表
        :param content: 执行内容
        :return: 执行结果
        """
        logger.info(_(u"开始执行脚本"))
        params = {
            "app_id": self.bk_biz_id,
            "content": b64encode(content),
            "ip_list": [
                {
                    "ip": host["ip"],
                    "source": change_plat_id_to_standard_style(host["bk_cloud_id"], dataapi_settings.INSTALL_PACAKAGE),
                }
                for host in hosts
            ],
            "type": 1,
            "account": self.account,
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }
        # 执行脚本
        ret = JobApi.fast_execute_script(params)
        if not ret.is_success():
            logger.error(
                _(u"调用post:fast_execute_script失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id)
            )
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                errors=u"fast_execute_script调用失败,原因:" + ret.message,
                message=_(u"执行脚本失败:{}").format(ret.message),
            )

        task_id = ret.data["taskInstanceId"]
        # 检查执行结果 todo get_task_ip_log 老代码用的返回这个接口，我觉得这这样就行
        return self._check_task_ip_log_result(task_id)

    def _check_file_result(self, task_id):
        # 查询GSE获取文件结果
        # 115表示发出的命令管道请求还在执行中，这个error不应该判定为出错
        ERROR_CODE_NOT_DONE_YET = 115

        exit_codes = list()
        is_finished = False
        params = {
            "app_id": self.bk_biz_id,
            "gse_task_id": task_id,
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }

        # 获取结果前，sleep 10s
        time.sleep(GSEAPI_ASYNC_CHECK_WAITING_TIME)
        ret = JobApi.get_file_result(params)

        if not ret.is_success():
            logger.error(
                _(u"调用post:get_file_result失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id)
            )
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                errors=u"调用get_file_result失败: " + ret.message,
                message=_(u"获取文件结果失败:{}").format(ret.message),
            )

        if ret.data["errorCode"] == TASK_IP_LOG_STATUS.SUCCESS_CODE:
            is_finished = True
            data_list = ret.data["data"]
            for data in data_list:
                exit_code = dict()
                exit_code["ip"] = data["ip"]
                exit_code["status"] = TASK_IP_LOG_STATUS.SUCCESS_CODE
                exit_code["error_msg"] = "ok"
                exit_code["content"] = json.loads(data["content"]).get("content", "")
                exit_codes.append(exit_code)
        elif ret.data["errorCode"] == ERROR_CODE_NOT_DONE_YET:
            pass
        else:
            is_finished = True
            data_list = ret.data["data"]
            for data in data_list:
                exit_code = dict()
                exit_code["ip"] = data["ip"]
                exit_code["status"] = TASK_IP_LOG_STATUS.FAIL_CODE
                exit_code["error_msg"] = data["content"].get("errmsg")
                exit_code["content"] = data["content"]
                exit_codes.append(exit_code)

        return is_finished, exit_codes

    def sync_restart_process(self, hosts):
        """
        同步重启进程
        :param hosts: 主机列表
        :return: 返回执行结果
        """
        logger.info(_(u"开始重启进程"))
        return self._do_proc_op(hosts, "restart")

    def sync_start_process(self, hosts):
        """
        同步启动进程
        :param hosts: 主机列表
        :return: 返回执行结果
        """
        logger.info(_(u"开始启动进程"))
        return self._do_proc_op(hosts, "start")

    def sync_stop_process(self, hosts):
        """
        同步停止进程
        :param hosts: 主机列表
        :return: 返回执行结果
        """
        logger.info(_(u"开始停止进程"))
        return self._do_proc_op(hosts, "stop")

    """
       主机任务状态码， 1.Agent异常; 3.上次已成功; 5.等待执行; 7.正在执行; 9.执行成功; 11.任务失败;
       12.任务下发失败; 13.任务超时; 15.任务日志错误;
       101.脚本执行失败; 102.脚本执行超时; 103.脚本执行被终止; 104.脚本返回码非零;
       202.文件传输失败; 203.源文件不存在; 310.Agent异常; 311.用户名不存在;
       320.文件获取失败; 321.文件超出限制; 329.文件传输错误; 399.任务执行出错
    """

    def get_exit_code(self, result):
        """
        解析 get_task_ip_log 返回结果，获取退出码
        :param result: 调用 get_task_ip_log 的返回结果
        :return: (执行是否完成，exit_codes为列表)
        执行是否完成  0: 执行完成 1：正在执行
        exit_codes为列表：
        [
            {
                'plat_id':1,
                'ip':'x.x.x.x',
                'exit_code':0
            }
        ]
        """
        waiting = 0
        exit_codes = list()

        if not result.data[0]["isFinished"]:
            logger.info("wait")
            waiting = 1

        step_analyse_result_list = result.data[0]["stepAnalyseResult"]
        for step_analyse_result in step_analyse_result_list:
            if "ipLogContent" not in step_analyse_result:
                logger.error("lost ipLogContent...")
                logger.error(result)
                continue
            ip_log_content = step_analyse_result["ipLogContent"]
            for item in ip_log_content:
                exit_code = dict()
                exit_code["ip"] = item["ip"]
                exit_code["bk_cloud_id"] = int(item["source"])
                status = item["status"]
                if status == 9:  # 执行成功
                    exit_code["status"] = TASK_IP_LOG_STATUS.SUCCESS_CODE
                    exit_code["error_msg"] = "ok"
                    exit_code["content"] = item["logContent"]
                else:
                    exit_code["status"] = item["status"]
                    exit_code["content"] = item.get("logContent")
                    exit_code["error_msg"] = TASK_IP_LOG_STATUS.STATUS[item["status"]]

                exit_codes.append(exit_code)

        return waiting, exit_codes

    def _check_task_ip_log_result(self, task_id, interval=3, retry_times=80):
        """检查job任务的结果详细结果和错误原因
        Args:
            task_id: gse命令执行id
            interval: 查询结果间隔 4分钟超时
            retry_times: 查询结果重试次数

        Returns:
            执行结果
        """
        params = {
            "task_instance_id": task_id,
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }

        # 获取结果前sleep 10s
        time.sleep(GSEAPI_ASYNC_CHECK_WAITING_TIME)
        while retry_times > 0:
            retry_times -= 1
            ret = JobApi.get_task_ip_log(params)
            logger.info(_(u"get_task_ip_log  result:{} , task_id:{}").format(ret.data, self.task_id))
            if not ret.is_success():
                logger.error(
                    _(u"调用post:get_task_ip_log:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id)
                )
                raise CollectorError(
                    error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                    errors=u"get_task_ip_log,获取任务结果失败,原因:" + ret.message,
                    message=_(u"获取任务结果失败:{}").format(ret.message),
                )

            status, exit_codes = self.get_exit_code(ret)
            if status == 1:  # doing
                time.sleep(interval)
                continue
            elif status == 0:  # done
                return exit_codes

        logger.error(_(u"调用post:get_task_ip_log超时:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id))
        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_JOB_TIME_OUT,
            errors=_(u"任务超时,原因:{}").format(ret.message),
            message=_(u"任务超时"),
        )

    def _get_task_ip_log(self, task_id, interval=3, retry_times=20):
        params = {
            "task_instance_id": task_id,
            "bk_username": self.TASK_OPERATOR,
            "operator": self.TASK_OPERATOR,
        }
        ip_log = []

        # 获取结果前sleep 10s
        time.sleep(GSEAPI_ASYNC_CHECK_WAITING_TIME)
        while retry_times > 0:
            retry_times -= 1
            ret = JobApi.get_task_ip_log(params)
            if not ret.is_success():
                logger.error(
                    _(u"调用post:get_task_ip_log失败:{},请求参数:{}, task_id:{}").format(ret.message, params, self.task_id)
                )
                raise CollectorError(
                    error_code=CollerctorCode.COLLECTOR_JOB_FAIL,
                    errors=_(u"get_task_ip_log,获取任务结果日志失败"),
                    message=_(u"获取任务结果日志失败:{}").format(ret.message),
                )

            for ret_data in ret.data:
                if ret_data.get("isFinished") and ret_data.get("stepAnalyseResult"):
                    if ret_data.get("stepAnalyseResult")[0].get("ipLogContent"):
                        for _log_content in ret_data.get("stepAnalyseResult")[0].get("ipLogContent"):
                            ip_log.append(_log_content.get("logContent"))
                        return ip_log
            time.sleep(interval)

        raise CollectorError(
            error_code=CollerctorCode.COLLECTOR_JOB_TIME_OUT,
            errors=_(u"下发配置任务超时"),
            message=u"下发配置任务超时",
        )

    @classmethod
    def validate_path(cls, conf_str):
        conf_obj = json.loads(conf_str)

        if "tlogcfg" not in conf_obj:
            return
        for task in conf_obj["tlogcfg"]:
            path = task.get("file")
            data_set = task.get("dataset", "")
            if path and not path.startswith("/") and path != "[[host-specific]]":
                raise ValidationError(_(u"{}:路径 {}非绝对路径，请调整").format(data_set, path))

    @staticmethod
    def judge_gse_result(data):
        """判定gse命令执行结果

        Args:
            data: gse返回的运行结果

            "data":{
                "m_rsp":{
                    "/usr/local/gse/gseagent/plugins/ProcessMonitorBeat/ProcessMonitorBeat:1:x.x.x.x":{
                        "content":"{"ip":"x.x.x.x","process":[{"instance":[{"cmdline":"./ProcessMonitorBeat ",
                        "cpuUsage":0,"cpuUsageAve":0,"diskSize":-1,"elapsedTime":0,"freeVMem":"342732",
                        "phyMemUsage":0.12997265160083771,"pid":5868,"processName":"ProcessMonitorBeat",
                        "startTime":"Wed May 24 15:25:48 2017","stat":"S","stime":"1",
                        "threadCount":0,"usePhyMem":10412,"utime":"1"}],"procname":"ProcessMonitorBeat"}],
                        "timezone":8,"utctime":"2017-05-24 15:25:56","utctime2":"2017-05-24 07:25:56"} ",
                        "errcode":0,
                        "errmsg":"success"
                    }
                },
                "setM_errmsg":true,
                "m_errmsg":"",
                "m_errcode":0,
                "m_rspSize":1,
                "setM_errcode":true,
                "setM_rsp":true
            },

        Returns:
            执行结果， gse的反馈中0表示成功，115表示正在执行，其他表示失败
            {
                "success": [{"obj": 'x.x.x.x', 'data': xxx}],
                "pending": [{"obj": "x.x.x.x"}],
                "error": [{"obj": "x.x.x.x", "errmsg": xxx}]
            }

        """
        exit_codes = list()
        m_error_code = data["m_errcode"]
        data = data["m_rsp"]
        is_finished = True

        # 任务正在执行
        if m_error_code == 115:
            return False, exit_codes

        for obj, ret in data.iteritems():

            # path:cloudid:ip, path may contain ':' on Windows
            keys = obj.split(":")
            ip = keys[-1]
            bk_cloud_id = change_plat_id_to_standard_style(int(keys[-2]), True)

            exit_code = dict()
            exit_code["ip"] = ip

            if ret["errcode"] == 0:
                exit_code["status"] = TASK_IP_LOG_STATUS.SUCCESS_CODE
                exit_code["error_msg"] = "ok"
                # 进程结果需要特别判断
                exit_code["bk_cloud_id"] = bk_cloud_id
                exit_code["content"] = ret["content"]
            elif ret["errcode"] == 115:
                is_finished = False
                exit_code["bk_cloud_id"] = bk_cloud_id
                exit_code["status"] = TASK_IP_LOG_STATUS.PENDING_CODE
                exit_code["error_msg"] = "pending"
            else:
                exit_code["bk_cloud_id"] = bk_cloud_id
                exit_code["status"] = TASK_IP_LOG_STATUS.FAIL_CODE
                exit_code["error_msg"] = ret["errmsg"]

            exit_codes.append(exit_code)
        return is_finished, exit_codes
