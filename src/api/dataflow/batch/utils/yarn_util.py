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

import time

import common.http as http
import requests
from django.utils.translation import ugettext as _ugt

from dataflow.batch.config.yarn_config import ConfigYarn
from dataflow.batch.exceptions.comp_execptions import InnerYarnServerException
from dataflow.shared.log import batch_logger


class YARN(object):
    def __init__(self, cluster_group):
        self.cluster_group = cluster_group
        self.config = ConfigYarn(self.cluster_group)
        self.__get_active_rm_url()

    def __access_rm_rest_api(self, path, method="GET", data=None, retry_times=1):
        try:
            access_url = self.active_url + path
            if method == "GET":
                data = data or ""
                _, result = http.get(access_url, data)
                return result
            elif method == "POST":
                data = data or {}
                _, result = http.post(access_url, data)
                return result
            else:
                raise InnerYarnServerException(message=_ugt("不支持的method - %s") % method)
        except requests.exceptions.ConnectionError as error:
            batch_logger.error(error)
            time.sleep(retry_times)
            try:
                self.__get_active_rm_url()
            except Exception:
                pass
            return self.__access_rm_rest_api(path, method, data, retry_times + 1)
        except Exception as e:
            batch_logger.exception(e)
            raise InnerYarnServerException(message=_ugt("connect to yarn failed."))

    def __get_active_rm_url(self):
        for url in self.config.grt_rm_web_urls():
            access_url = "%s/ws/v1/cluster/metrics" % url
            try:
                _, api_rtn = http.get(access_url)
                batch_logger.info("xxx", api_rtn)
                if isinstance(api_rtn, dict):
                    self.active_url = url
                    return
            except Exception as e:
                batch_logger.exception(e)
        raise InnerYarnServerException(message=_ugt("No Active ResourceManager"))

    def get_yarn_metrics(self):
        return self.__access_rm_rest_api("/ws/v1/cluster/metrics")

    def get_queue_resource(self, name):
        queue_string = "/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,"
        if "." in name:
            queue_string = queue_string + "q0=root,"
            queue_list = name.split(".")
            count = 1
            for queue_hierarchy in queue_list:
                queue_string = queue_string + "q" + str(count) + "=" + queue_hierarchy + ","
                count = count + 1
            queue_string = queue_string.strip(",")
        else:
            queue_string = queue_string + "q0=" + name
        return self.__access_rm_rest_api(queue_string)
