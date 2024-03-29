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
from .constants import ApplicationState
from .errors import IllegalArgumentError
from .hadoop_conf import get_nodemanager_webapp_endpoint

log = get_logger(__name__)

LEGAL_APPLICATION_STATES = {s for s, _ in ApplicationState}


def validate_application_state(state, required=False):
    if state:
        if state not in LEGAL_APPLICATION_STATES:
            msg = "Application State {} is illegal".format(state)
            raise IllegalArgumentError(msg)
    else:
        if required:
            msg = "state argument is required to be provided"
            raise IllegalArgumentError(msg)


class NodeManager(BaseYarnAPI):
    """
    The NodeManager REST API's allow the user to get status on the node and
    information about applications and containers running on that node.

    If `service_endpoint` argument is `None` client will try to extract it from
    Hadoop configuration files.

    :param str service_endpoint: NodeManager HTTP(S) address
    :param int timeout: API connection timeout in seconds
    :param AuthBase auth: Auth to use for requests
    :param boolean verify: Either a boolean, in which case it controls whether
        we verify the server's TLS certificate, or a string, in which case it must
        be a path to a CA bundle to use. Defaults to ``True``
    """

    def __init__(self, service_endpoint=None, timeout=30, auth=None, verify=True):
        if not service_endpoint:
            service_endpoint = get_nodemanager_webapp_endpoint()

        super(NodeManager, self).__init__(service_endpoint, timeout, auth, verify)

    def node_information(self):
        """
        The node information resource provides overall information about that
        particular node.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/ws/v1/node/info"
        return self.request(path)

    def node_applications(self, state=None, user=None):
        """
        With the Applications API, you can obtain a collection of resources,
        each of which represents an application.

        :param str state: application state
        :param str user: user name
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        :raises yarn_api_client.errors.IllegalArgumentError: if `state`
            incorrect
        """
        path = "/ws/v1/node/apps"

        validate_application_state(state)

        loc_args = (("state", state), ("user", user))

        params = self.construct_parameters(loc_args)

        return self.request(path, params=params)

    def node_application(self, application_id):
        """
        An application resource contains information about a particular
        application that was run or is running on this NodeManager.

        :param str application_id: The application id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/ws/v1/node/apps/{appid}".format(appid=application_id)

        return self.request(path)

    def node_containers(self):
        """
        With the containers API, you can obtain a collection of resources,
        each of which represents a container.

        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/ws/v1/node/containers"

        return self.request(path)

    def node_container(self, container_id):
        """
        A container resource contains information about a particular container
        that is running on this NodeManager.

        :param str container_id: The container id
        :returns: API response object with JSON data
        :rtype: :py:class:`yarn_api_client.base.Response`
        """
        path = "/ws/v1/node/containers/{containerid}".format(containerid=container_id)

        return self.request(path)
