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

import logging
from datetime import datetime

import requests

from .errors import APIError, ConfigurationError

try:
    from urllib.parse import urlparse, urlunparse
except ImportError:
    from urllib.parse import urlparse, urlunparse


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    return logger


log = get_logger(__name__)


class Response(object):
    """
    Basic container for response dictionary

    :param requests.Response response: Response for call via requests lib
    """

    def __init__(self, response):
        #: Dictionary with response data.  Handle cases where content is empty
        # to prevent JSON decode issues
        if response.content:
            self.data = response.json()
        else:
            self.data = {}


class Uri(object):
    def __init__(self, service_endpoint):
        if not (service_endpoint.startswith("http://") or service_endpoint.startswith("https://")):
            service_endpoint = "http://" + service_endpoint

        service_uri = urlparse(service_endpoint)
        self.scheme = service_uri.scheme or "http"
        self.hostname = service_uri.hostname or service_uri.path
        self.port = service_uri.port
        self.is_https = service_uri.scheme == "https" or False

    def to_url(self, api_path=None):
        path = api_path or ""
        if self.port:
            result_url = urlunparse(
                (
                    self.scheme,
                    self.hostname + ":" + str(self.port),
                    path,
                    None,
                    None,
                    None,
                )
            )
        else:
            result_url = urlunparse((self.scheme, self.hostname, path, None, None, None))

        return result_url


class BaseYarnAPI(object):
    response_class = Response

    def __init__(self, service_endpoint=None, timeout=None, auth=None, verify=True):
        self.timeout = timeout

        if service_endpoint:
            self.service_uri = Uri(service_endpoint)
        else:
            self.service_uri = None

        self.session = requests.Session()
        self.session.auth = auth
        self.session.verify = verify

    def _validate_configuration(self):
        if not self.service_uri:
            raise ConfigurationError("API endpoint is not set")

    def request(self, api_path, method="GET", **kwargs):
        self._validate_configuration()
        api_endpoint = self.service_uri.to_url(api_path)

        if method == "GET":
            headers = {}
        else:
            headers = {"Content-Type": "application/json"}

        if "headers" in kwargs and kwargs["headers"]:
            headers.update(kwargs["headers"])

        begin = datetime.now()
        response = self.session.request(
            method=method, url=api_endpoint, headers=headers, timeout=self.timeout, **kwargs
        )
        end = datetime.now()
        log.debug(
            "'{method}' request against endpoint '{endpoint}' took {duration} ms".format(
                method=method,
                endpoint=api_endpoint,
                duration=round((end - begin).total_seconds() * 1000, 3),
            )
        )

        if response.status_code in (200, 202):
            return self.response_class(response)
        else:
            msg = "Response finished with status: {status}. Details: {msg}".format(
                status=response.status_code, msg=response.text
            )
            raise APIError(msg)

    def construct_parameters(self, arguments):
        params = {key: value for key, value in arguments if value is not None}
        return params
