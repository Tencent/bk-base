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

from common.exceptions import ApiRequestError
from django.utils.translation import ugettext as _

from dataflow import pizza_settings
from dataflow.pizza_settings import JOBNAVI_STREAM_CLUSTER_ID
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import stream_logger as logger
from dataflow.stream.exceptions.comp_execptions import JobNaviError, StreamCode, YarnResourcesLackError
from dataflow.stream.settings import API_ERR_RETRY_TIMES

YARN_SERVICE_SCHEDULE = "yarn-service"


class StreamJobNaviHelper(object):
    def __init__(self, geog_area_code, cluster_id=JOBNAVI_STREAM_CLUSTER_ID):
        self.jobnavi = JobNaviHelper(geog_area_code, cluster_id=cluster_id)

    @staticmethod
    def get_jobnavi_timeout(seconds=180):
        return seconds

    def get_event_status(self, event_id, timeout=180):
        """
        获取yarn session状态

        :param event_id:
        :param timeout:
        :return: under_deploy (没有部署成功) deployed(部署成功)
        """
        try:
            for i in range(self.get_jobnavi_timeout(timeout)):
                data = self.jobnavi.get_event(event_id)
                logger.info("[get_application_status] event id {} and result data is {}".format(event_id, data))

                if data["is_processed"]:
                    return data["process_info"]
                time.sleep(1)
            raise JobNaviError(_("获取作业提交状态失败"))
        except ApiRequestError as e0:
            logger.exception(e0)
            if e0.code and e0.code == StreamCode.YARN_RESOURCES_LACK_ERR:
                raise YarnResourcesLackError()
            raise JobNaviError(_("获取作业提交状态失败"))
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取作业提交状态失败"))

    def get_event_result(self, event_id):
        """
        获取yarn session状态

        :param event_id:
        :return: under_deploy (没有部署成功) deployed(部署成功)
        """
        try:
            for i in range(5):
                data = self.jobnavi.get_event(event_id)
                logger.info("[get_application_status] event id {} and result data is {}".format(event_id, data))

                if data["is_processed"] and data["process_success"]:
                    return data["process_info"]
                time.sleep(1)
            raise JobNaviError(_("获取调度事件结果失败"))
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取调度事件结果失败"))

    def get_result_for_kill_application(self, event_id, timeout):
        """
        获取kill application的处理结果
        {u'process_success': True, u'is_processed': True, u'process_info': None}
        process_success: 事件的处理结果
        is_processed： 事件是否处理完成
        process_info： 处理的一些相关xinxi

        :param event_id:
        :param timeout:
        :return: True or False
        """
        try:
            for i in range(timeout):
                data = self.jobnavi.get_event(event_id)
                logger.info("[get_application_status] event id {} and result data is {}".format(event_id, data))

                if data["is_processed"]:
                    return data["process_success"]
                time.sleep(1)
            raise JobNaviError(_("获取作业处理结果失败"))
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取作业处理结果失败"))

    def list_jobs_status(self, event_id, timeout=180):
        """
        根据event id获取jobs的状态

        :param event_id: event id
        :param timeout: timeout
        :return:
            {
                "jobs-running": []
            }
        """
        try:
            for i in range(self.get_jobnavi_timeout(timeout)):
                data = self.jobnavi.get_event(event_id)
                logger.info("[list_jobs_status] event id {} and result data is {}".format(event_id, data))

                if data["is_processed"]:
                    if data["process_success"]:
                        return data["process_info"]
                    else:
                        logger.exception("调度失败: %s" % data["process_info"])
                        break
                time.sleep(1)
            raise JobNaviError(_("获取作业状态失败"))
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取作业状态失败"))

    def get_run_cancel_job_result(self, event_id):
        """
        获取run任务是否成功

        cancel的时候，判断job是否存在，如果不存在，返回true，并返回信息
        {'data'：{'is_processed': True, 'process_success': True, 'info': {'code': 404, 'message': xxx}}}

        :param event_id:
        :return: true/false
        """
        try:
            for i in range(self.get_jobnavi_timeout()):
                data = self.jobnavi.get_event(event_id)
                logger.info("[get_run_cancel_job_result] event id {} and result data is {}".format(event_id, data))

                if data["is_processed"] and data["process_success"]:
                    return data["process_success"]
                elif (
                    data["is_processed"]
                    and not data["process_success"]
                    and data["process_info"]
                    and json.loads(data["process_info"])["code"]
                    and json.loads(data["process_info"])["code"] == 1571022
                ):
                    raise YarnResourcesLackError()
                time.sleep(1)
            raise JobNaviError(_("获取作业操作状态失败"))
        except YarnResourcesLackError:
            raise YarnResourcesLackError()
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取作业操作状态失败"))

    def get_cancel_job_result_for_cluster(self, event_id):
        try:
            data = self.jobnavi.get_event(event_id)
            logger.info("[get_cancel_job_result_for_cluster] event id {} and result data is {}".format(event_id, data))

            if data["is_processed"]:
                return data["process_success"]
            return False
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取作业操作状态失败"))

    def get_execute_status(self, execute_id, timeout=180):
        """
        根据execute id轮询执行状态, 超时时间
        :param event_id: event id
        :param timeout: timeout 获取 execute，若 schedule 刚创建，可能需要多等一些时间
        :return: running
        """
        try:
            for i in range(self.get_jobnavi_timeout(timeout)):
                data = self.jobnavi.get_execute_status(execute_id)
                logger.info("[get execute status]execute id {} and result data is {}".format(execute_id, data))
                # db同步数据需要时间，这里有可能是None
                if not data:
                    pass
                # skipped状态代表execute id已存在，与running保持一致
                elif data["status"] == "running" or data["status"] == "skipped":
                    return data
                elif data["status"] == "failed":
                    logger.exception("Jobnavi get execute status failed. %s" % data)
                    if (
                        data["info"]
                        and json.loads(data["info"])["code"]
                        and json.loads(data["info"])["code"] == 1571022
                    ):
                        raise YarnResourcesLackError()
                    else:
                        raise JobNaviError(_("任务提交失败，请查看日志"))
                elif data["status"] != "preparing" and data["status"] != "none":
                    raise JobNaviError("Jobnavi get execute unknown status: {}. {}".format(data["status"], data))
                time.sleep(1)
            raise JobNaviError(_("任务提交失败，请查看日志"))
        except YarnResourcesLackError:
            raise YarnResourcesLackError()
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("任务提交失败，请查看日志"))

    def get_yarn_session_execute_id(self, schedule_id):
        """
        在yarn session状态下获取execute id
        在running下没有找到execute id，则需要创建execute id
        启动yarn session不需要判断资源是否足够

        :param schedule_id: 对应的cluster name
        :return:
        """
        try:
            running_data = self.jobnavi.get_execute_result_by_status(schedule_id, "running")
            if len(running_data) >= 1 and running_data[0]["execute_info"]["id"]:
                return running_data[0]["execute_info"]["id"]
            # failed_data = self.jobnavi.get_schedule_execute_result_by_status(schedule_id, 'failed')
            # if len(failed_data) >= 1 and failed_data[0]['info'] and json.loads(failed_data[0]['info'])['code'] \
            #         and json.loads(failed_data[0]['info'])['code'] == 1571022:
            #     raise YarnResourcesLackError()
            # if len(running_data) == 0 and len(failed_data) == 0:
            #     return None
            return None
        except Exception:
            raise JobNaviError(_("获取作业调度信息异常"))

    def get_execute_id(self, schedule_id, timeout=180):
        """
        根据schedule id获取execute的信息
        @param schedule_id: 对应的cluster name
        @param timeout: 获取 execute，若 schedule 刚创建，可能需要多等一些时间
        @return: execute info
        """
        status = "running"
        for i in range(self.get_jobnavi_timeout(timeout)):
            data = None
            # sometimes the jobnavi's gunicorn may appear 502 response, just retry after sleep
            try:
                data = self.jobnavi.get_execute_result_by_status(schedule_id, status)
                logger.info("[get execute id] schedule id {} and result is {}".format(schedule_id, data))
            except ApiRequestError:
                logger.error("[get execute id] schedule id %s call ApiRequestError exception" % schedule_id)

            if data and len(data) >= 1 and data[0]["execute_info"]["id"]:
                logger.info("The cluster name {} result is {}".format(schedule_id, str(data)))
                return data[0]["execute_info"]["id"]
            time.sleep(1)
        logger.error("Jobnavi get execute info timeout, schedule id is %s" % schedule_id)
        raise JobNaviError(_("获取作业调度信息异常"))

    def get_one_yarn_cluster_all_execute_id(self, schedule_id):
        """

        :param schedule_id:
        :return:
        """
        status = "running"
        try:
            data = self.jobnavi.get_execute_result_by_status(schedule_id, status)
            logger.info(
                "[jobnavi_helper#get_one_yarn_cluster_all_execute_id] schedule id %s and result is %s"
                % (schedule_id, data)
            )

            execute_ids = []
            for tmp in data:
                execute_ids.append(tmp["execute_info"]["id"])
            return execute_ids
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取作业调度信息异常"))

    def kill_execute_id(self, execute_id):
        """
        kill execute id

        :param execute_id:
        :return:
        """
        args = {
            "event_name": "kill",
            "execute_id": execute_id,
            "change_status": "killed",
        }
        try:
            data = self.jobnavi.send_event(args)
            logger.info("[jobnavi_helper#kill_execute_id] the result is %s" % data)
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("清除作业调度信息异常"))

    def request_jobs_status(self, execute_id):
        """
        提交获取jobs状态的任务

        :return: event id
        """
        args = {"event_name": "sync_job", "execute_id": execute_id}
        try:
            data = self.jobnavi.send_event(args)
            logger.info("[request_jobs_status] execute id {} and data is {}".format(execute_id, data))
            return data
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("请求同步作业状态异常"))

    def request_jobs_resources(self, execute_id, extra_args=None):
        """
        提交获取jobs状态的任务

        :return: event id
        """
        args = {"event_name": "get_resources", "execute_id": execute_id}
        if extra_args:
            args.update(extra_args)
        try:
            data = self.jobnavi.send_event(args)
            logger.info("[request_jobs_resources] execute id {} and data is {}".format(execute_id, data))
            return data
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("请求资源信息异常"))

    def run_flink_job(self, execute_id, job_name, resources_unit, jar_name, job_config):
        """
        run flink job

        :param execute_id:
        :param job_name:
        :param resources_unit:
        :param jar_name:
        :param job_config:
        :return:
        """

        event_info = {
            "job_name": job_name,
            "parallelism": resources_unit,
            "jar_file_name": jar_name,
            "argument": job_config,
        }

        if pizza_settings.RUN_VERSION == pizza_settings.RELEASE_ENV:
            event_info["license"] = str(
                json.dumps(
                    {
                        "license_server_url": pizza_settings.LICENSE_SERVER_URL,
                        "license_server_file_path": pizza_settings.LICENSE_SERVER_FILE_PATH,
                    }
                )
            )

        args = {
            "event_name": "run_job",
            "execute_id": execute_id,
            "event_info": str(json.dumps(event_info)),
        }
        try:
            data = self.jobnavi.send_event(args)
            logger.info("[run_flink_job] execute id {} and result {}".format(execute_id, data))
            return data
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("提交作业失败"))

    def cancel_flink_job(self, execute_id, job_name, use_savepoint=True, event_name="cancel_job"):
        """
        停止任务
        @param execute_id:  对应的yarn-session
        @param job_name:
        @param use_savepoint:
        @param event_name: cancel_job/stop_job
        @return:
        """
        event_info = {"job_name": job_name, "use_savepoint": use_savepoint}
        args = {
            "event_name": event_name,
            "execute_id": execute_id,
            "event_info": str(json.dumps(event_info)),
        }
        try:
            data = self.jobnavi.send_event(args)
            logger.info("[cancel_flink_job] execute id {} and result {}".format(execute_id, data))
            return data
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("停止作业失败"))

    def submit_query_application_task(self, execute_id):
        """
        轮询获取application运行状态

        :param execute_id: 对应的yarn session
        :return: under_deploy (没有部署成功) deployed(部署成功)
        """
        args = {"event_name": "application_status", "execute_id": execute_id}
        try:
            data = self.jobnavi.send_event(args)
            if not data:
                raise JobNaviError("Jobnavi submit job failed.")
            logger.info("[submit_application] execute id {} and result {}".format(execute_id, data))
            return data
        except ApiRequestError as e0:
            logger.exception(e0)
            if e0.code and e0.code == 1571022:
                raise YarnResourcesLackError()
            raise JobNaviError(_("获取任务提交信息异常"))
        except Exception as e:
            logger.exception(e)
            raise JobNaviError(_("获取任务提交信息异常"))

    def add_yarn_schedule(self, args):
        result = self.jobnavi.create_schedule_info(args, timeout_auto_retry=True)
        logger.info("[add_yarn_schedule] args is {} and result is {}".format(args, result))
        return result

    def execute_yarn_schedule(self, schedule_id):
        args = {"schedule_id": schedule_id}
        data = self.jobnavi.execute_schedule(args)
        logger.info("[execute_yarn_schedule] args is {} and result is {}".format(str(args), data))
        if not data:
            logger.error("Jobnavi execute schedule did not return execute id.")
            raise JobNaviError(_("任务调度异常"))
        return data

    def execute_schedule_meta_data(self, cluster_set, args):
        data = self.jobnavi.get_schedule_info(cluster_set)
        logger.info("[execute_schedule_meta_data get_schedule] args is {} and result is {}".format(cluster_set, data))
        # 如果schedule id存在，则update，不存在 则add
        if data:
            # update
            self.jobnavi.update_schedule_info(args)
        else:
            # add
            self.jobnavi.create_schedule_info(args)

    def del_schedule(self, schedule_id):
        self.jobnavi.delete_schedule(schedule_id, True)

    def get_schedule(self, schedule_id):
        data = self.jobnavi.get_schedule_info(schedule_id)
        logger.info("[get_schedule] result is %s" % data)
        if data:
            return True
        else:
            return False

    def get_yarn_application_status(self, execute_id):
        for i in range(self.get_jobnavi_timeout()):
            event_id = self.submit_query_application_task(execute_id)
            status = self.get_event_status(event_id)
            if status and status == "deployed":
                return True
            else:
                time.sleep(2)
        raise JobNaviError(_("获取任务提交状态超时"))

    def get_cluster_app_deployed_status(self, execute_id, timeout=180):
        event_id = self.submit_query_application_task(execute_id)
        status = self.get_event_status(event_id, timeout)
        if status and status == "deployed":
            return True
        else:
            return False

    def send_event_for_kill_yarn_app(self, execute_id):
        """
        kill 参数： {"event_name": "kill_application", "execute_id": "xxxx"}

        :param execute_id:
        :return:
        """
        args = {"event_name": "kill_application", "execute_id": execute_id}

        data = self.jobnavi.send_event(args)
        logger.info("[kill_yarn_application] the result is {} and execute id is {}".format(data, execute_id))
        return data

    def kill_yarn_application(self, execute_id, timeout):
        """
        kill 参数： {"event_name": "kill_application", "execute_id": "xxxx"}

        1.send event
        2.get event result

        :param execute_id:
        :param timeout:
        :return:
        """
        args = {"event_name": "kill_application", "execute_id": execute_id}

        event_id = self.jobnavi.send_event(args)
        result = self.get_result_for_kill_application(event_id, timeout)
        logger.info("[kill_yarn_application] the result is {} and execute id is {}".format(result, execute_id))
        return result

    def get_job_exceptions(self, execute_id, job_name):
        """
        获取任务的异常信息

        发事件参数为
        {
            'event_name': 'job_exception',
            'execute_id': execute_id,
            'event_info': {
                'job_name': job_name
                }
        }

        :param execute_id:  execute id
        :return: exception messages
        """
        event_info = str(json.dumps({"job_name": job_name}))
        args = {
            "event_name": "job_exception",
            "execute_id": execute_id,
            "event_info": event_info,
        }

        event_id = self.jobnavi.send_event(args)
        try:
            result = self.get_event_result(event_id)
        except Exception:
            return None
        logger.info(
            "[get_job_exceptions] the job name is %s, the execute id is %s, and the result is %s"
            % (job_name, execute_id, result)
        )
        return json.loads(result)

    def send_event(self, execute_id, event_name, event_info=None, timeout_auto_retry=True):
        """
        提交获取jobs状态的任务
        :return: event id
        """
        args = {"event_name": event_name, "execute_id": execute_id}
        if event_info:
            args["event_info"] = event_info
        data = None
        for i in range(API_ERR_RETRY_TIMES):
            try:
                data = self.jobnavi.send_event(args, timeout_auto_retry)
                break
            except ApiRequestError as e1:
                logger.warning("[send event][{}]请求异常, detail:{}".format(execute_id, e1.message))
                time.sleep(1)
        logger.info("[request_jobs_status] execute id {} and data is {}".format(execute_id, data))
        return data

    def valid_yarn_service_schedule(self):
        """
        检查yarn service schedule是否存在，不存在就创建
        """
        schedule_info = self.jobnavi.get_schedule_info(YARN_SERVICE_SCHEDULE)
        if not schedule_info:
            args = {
                "schedule_id": YARN_SERVICE_SCHEDULE,
                "type_id": YARN_SERVICE_SCHEDULE,
            }
            self.jobnavi.create_schedule_info(args)

    def get_yarn_service_execute(self):
        """
        获取yarn service的exec id
        :return: exec id
        """
        data = self.jobnavi.get_execute_result_by_status(YARN_SERVICE_SCHEDULE, "running")
        if data:
            exec_id = data[0]["execute_info"]["id"]
        else:
            args = {
                "schedule_id": YARN_SERVICE_SCHEDULE,
            }
            exec_id = self.jobnavi.execute_schedule(args)
        self.wait_for_deployed(exec_id)
        return exec_id

    def wait_for_deployed(self, execute_id):
        """
        根据execute id轮询执行状态, 等待部署完成

        :param execute_id: execute id
        """
        try:
            for i in range(10):
                data = self.jobnavi.get_execute_status(execute_id)
                if not data:
                    time.sleep(1)
                elif data["status"] == "running":
                    return data
                elif data["status"] != "preparing" and data["status"] != "none":
                    raise JobNaviError("Jobnavi get execute status failed. %s" % data)
            raise JobNaviError("获取调度(%s)任务状态失败" % YARN_SERVICE_SCHEDULE)
        except Exception as e:
            logger.exception(e)
            raise JobNaviError("获取调度(%s)任务状态失败" % YARN_SERVICE_SCHEDULE)

    def send_yarn_state_event(self, exec_id):
        """
        发送yarn_state事件
        :param exec_id: exec id
        :return: result data
        """
        args = {"event_name": "yarn_state", "execute_id": exec_id}
        try:
            event_id = self.jobnavi.send_event(args)
            return self.get_result(event_id)
        except Exception as e:
            logger.exception(e)
            raise JobNaviError("获取最新数据失败，请联系管理员")

    def send_yarn_apps_event(self, exec_id):
        """
        发送yarn_apps事件
        :param exec_id: exec id
        :return: result data
        """
        args = {"event_name": "yarn_apps", "execute_id": exec_id}
        try:
            event_id = self.jobnavi.send_event(args)
            return self.get_result(event_id)
        except Exception as e:
            logger.exception(e)
            raise JobNaviError("获取最新数据失败，请联系管理员")

    def get_result(self, event_id):
        """
        获取事件结果
        :param event_id: event id
        :return: result data
        """
        try:
            for i in range(10):
                data = self.jobnavi.get_event(event_id)

                if data["is_processed"]:
                    if data["process_success"]:
                        return data["process_info"]
                    else:
                        raise JobNaviError("获取最新数据失败")
                time.sleep(1)
            raise JobNaviError("获取最新数据失败")
        except Exception as e:
            logger.exception(e)
            raise JobNaviError("获取最新数据失败")
