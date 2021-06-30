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

from .base import BaseYarnAPI, get_logger
from .hadoop_conf import get_webproxy_endpoint

log = get_logger(__name__)


class ApplicationMaster(BaseYarnAPI):
    """
    The MapReduce Application Master REST API's allow the user to get status
    on the running MapReduce application master. Currently this is the
    equivalent to a running MapReduce job. The information includes the jobs
    the app master is running and all the job particulars like tasks,
    counters, configuration, attempts, etc.

    If `address` argument is `None` client will try to extract `address` and
    `port` from Hadoop configuration files.

    :param str service_endpoint: ApplicationMaster HTTP(S) address
    :param int timeout: API connection timeout in seconds
    :param AuthBase auth: Auth to use for requests
    :param boolean verify: Either a boolean, in which case it controls whether
        we verify the server's TLS certificate, or a string, in which case it must
        be a path to a CA bundle to use. Defaults to ``True``
    """

    def __init__(self, service_endpoint=None, timeout=30, auth=None, verify=True):
        if not service_endpoint:
            service_endpoint = get_webproxy_endpoint(timeout, auth, verify)

        super(ApplicationMaster, self).__init__(service_endpoint, timeout, auth, verify)

    def application_information(self, application_id):
        """
        The MapReduce application master information resource provides overall
        information about that mapreduce application master.
        This includes application id, time it was started, user, name, etc.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/info".format(appid=application_id)

        return self.request(path)

    def jobs(self, application_id):
        """
        The jobs resource provides a list of the jobs running on this
        application master.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs".format(appid=application_id)

        return self.request(path)

    def job(self, application_id, job_id):
        """
        A job resource contains information about a particular job that was
        started by this application master. Certain fields are only accessible
        if user has permissions - depends on acl settings.

        :param str application_id: The application id
        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}".format(appid=application_id, jobid=job_id)

        return self.request(path)

    def job_attempts(self, application_id, job_id):
        """
        With the job attempts API, you can obtain a collection of resources
        that represent the job attempts.

        :param str application_id: The application id
        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """

        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/jobattempts".format(appid=application_id, jobid=job_id)

        return self.request(path)

    def job_counters(self, application_id, job_id):
        """
        With the job counters API, you can object a collection of resources
        that represent all the counters for that job.

        :param str application_id: The application id
        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/counters".format(appid=application_id, jobid=job_id)

        return self.request(path)

    def job_conf(self, application_id, job_id):
        """
        A job configuration resource contains information about the job
        configuration for this job.

        :param str application_id: The application id
        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/conf".format(appid=application_id, jobid=job_id)

        return self.request(path)

    def job_tasks(self, application_id, job_id):
        """
        With the tasks API, you can obtain a collection of resources that
        represent all the tasks for a job.

        :param str application_id: The application id
        :param str job_id: The job id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks".format(appid=application_id, jobid=job_id)

        return self.request(path)

    def job_task(self, application_id, job_id, task_id):
        """
        A Task resource contains information about a particular
        task within a job.

        :param str application_id: The application id
        :param str job_id: The job id
        :param str task_id: The task id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}".format(
            appid=application_id, jobid=job_id, taskid=task_id
        )

        return self.request(path)

    def task_counters(self, application_id, job_id, task_id):
        """
        With the task counters API, you can object a collection of resources
        that represent all the counters for that task.

        :param str application_id: The application id
        :param str job_id: The job id
        :param str task_id: The task id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/counters".format(
            appid=application_id, jobid=job_id, taskid=task_id
        )

        return self.request(path)

    def task_attempts(self, application_id, job_id, task_id):
        """
        With the task attempts API, you can obtain a collection of resources
        that represent a task attempt within a job.

        :param str application_id: The application id
        :param str job_id: The job id
        :param str task_id: The task id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts".format(
            appid=application_id, jobid=job_id, taskid=task_id
        )

        return self.request(path)

    def task_attempt(self, application_id, job_id, task_id, attempt_id):
        """
        A Task Attempt resource contains information about a particular task
        attempt within a job.

        :param str application_id: The application id
        :param str job_id: The job id
        :param str task_id: The task id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}".format(
            appid=application_id, jobid=job_id, taskid=task_id, attemptid=attempt_id
        )

        return self.request(path)

    def task_attempt_state(self, application_id, job_id, task_id, attempt_id):
        """
        With the task attempt state API, you can query the state of a submitted
        task attempt.

        :param str application_id: The application id
        :param str job_id: The job id
        :param str task_id: The task id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/state".format(
            appid=application_id, jobid=job_id, taskid=task_id, attemptid=attempt_id
        )

        return self.request(path)

    def task_attempt_state_kill(self, application_id, job_id, task_id, attempt_id):
        """
        Kill specific attempt using task attempt state API.

        :param str application_id: The application id
        :param str job_id: The job id
        :param str task_id: The task id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        data = {"state": "KILLED"}

        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/state".format(
            appid=application_id, jobid=job_id, taskid=task_id, attemptid=attempt_id
        )

        return self.request(path, "PUT", json=data)

    def task_attempt_counters(self, application_id, job_id, task_id, attempt_id):
        """
        With the task attempt counters API, you can object a collection
        of resources that represent al the counters for that task attempt.

        :param str application_id: The application id
        :param str job_id: The job id
        :param str task_id: The task id
        :param str attempt_id: The attempt id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/proxy/{appid}/ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters".format(
            appid=application_id, jobid=job_id, taskid=task_id, attemptid=attempt_id
        )

        return self.request(path)
