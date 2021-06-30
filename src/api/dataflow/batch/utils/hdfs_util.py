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

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import urllib.response

from dataflow.batch.config.hdfs_config import ConfigHDFS
from dataflow.batch.exceptions.base_exception import BatchException
from dataflow.batch.settings import hdfs_max_retry_times as max_retry_times
from dataflow.pizza_settings import FILE_UPLOAD_MAX_MEMORY_SIZE
from dataflow.shared.log import batch_logger


class CustomRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        pass

    def http_error_302(self, req, fp, code, msg, headers):
        info_url = urllib.response.addinfourl(fp, headers, req.get_full_url())
        info_url.status = code
        info_url.code = code
        info_url.headers = headers
        info_url.msg = msg
        return info_url

    http_error_301 = http_error_303 = http_error_307 = http_error_302


class HDFS(object):
    def __init__(self, cluster_group):
        self.cluster_group = cluster_group
        self.config_hdfs = ConfigHDFS(self.cluster_group)
        self.active_idx = self.__get_active_nn()

        if self.active_idx == -1:
            raise Exception("HDFS Cluster 不可用")

        self.standby_idx = self.active_idx ^ 1

    def __get_active_nn(self):
        for idx, url in enumerate(self.config_hdfs.get_jmx_namenode_status_urls()):
            try:
                batch_logger.info("%s->%d->%s" % ("__get_active_nn", idx, url))
                bean = json.loads(urllib.request.urlopen(url).read())["beans"][0]
                batch_logger.info(bean)
                if bean["State"] == "active":
                    return idx
            except Exception as e:
                batch_logger.exception(e)
                pass
        return -1

    @staticmethod
    def __urllib2(url, method, data=None):
        opener = urllib.request.build_opener()
        request = urllib.request.Request(url, data)
        request.get_method = lambda: method
        return opener.open(request)

    @staticmethod
    def __urllib2_ignore_3x(url, method, data=None):
        opener = urllib.request.build_opener(CustomRedirectHandler)
        request = urllib.request.Request(url.encode("utf-8"), data)
        request.get_method = lambda: method
        try:
            rtn = opener.open(request)
            return rtn
        except Exception as e:
            raise Exception("{} - {}".format(e, url))

    def __access_hdfs_rest_api(self, file_path, op, method="GET", is_ignore_3x=False, data=None):
        return self.__access_hdfs_rest_api_retry(file_path, op, 1, method=method, is_ignore_3x=is_ignore_3x, data=data)

    def __access_hdfs_rest_api_retry(self, file_path, op, retry_time, method="GET", is_ignore_3x=False, data=None):
        if retry_time > max_retry_times:
            batch_logger.info("重试操作hdfs达%s次失败" % max_retry_times)
            return None
        url = "{}{}?op={}".format(
            self.config_hdfs.get_webhdfs_urls()[self.active_idx],
            file_path,
            op,
        )
        batch_logger.info(url)
        try:
            return (
                self.__urllib2(url, method, data=data)
                if not is_ignore_3x
                else self.__urllib2_ignore_3x(url, method, data=data)
            )
        except Exception as e:
            batch_logger.error(e)
            time.sleep(0.1)
            self.active_idx = self.__get_active_nn()
            return self.__access_hdfs_rest_api_retry(
                file_path,
                op,
                retry_time + 1,
                method=method,
                data=data,
                is_ignore_3x=is_ignore_3x,
            )

    def get_live_datanode(self):
        bean = json.loads(
            urllib.request.urlopen(self.config_hdfs.get_jmx_namenode_info_urls()[self.active_idx]).read()
        )["beans"][0]
        return list(json.loads(bean["LiveNodes"]).keys())

    @staticmethod
    def check_datanode():
        rtn = {}
        for cluster_group in ConfigHDFS.get_all_cluster():
            hdfs = HDFS(cluster_group)
            rtn[cluster_group] = hdfs.get_live_datanode()
        return rtn

    def get_nn_status(self):
        rtn = {}
        # active
        url = self.config_hdfs.get_jmx_namenode_status_urls()[self.active_idx]
        bean = json.loads(urllib.request.urlopen(url).read())["beans"][0]
        rtn[self.config_hdfs.get_namenode_addrs()[self.active_idx]] = bean["State"]
        # standby
        url = self.config_hdfs.get_jmx_namenode_status_urls()[self.standby_idx]
        rtn[self.config_hdfs.get_namenode_addrs()[self.standby_idx]] = "dead"
        try:
            bean = json.loads(urllib.request.urlopen(url).read())["beans"][0]
            rtn[self.config_hdfs.get_namenode_addrs()[self.standby_idx]] = bean["State"]
        except Exception as e:
            batch_logger.error(e)

        return rtn

    @staticmethod
    def check_nn_status():
        rtn = {}
        for cluster_group in ConfigHDFS.get_all_cluster():
            hdfs = HDFS(cluster_group)
            rtn[cluster_group] = hdfs.get_nn_status()
        return rtn

    def get_active_nn_addr(self):
        return self.config_hdfs.get_namenode_addrs()[self.active_idx]

    # def get_active_nn_host(self):
    #     return self.config_hdfs.get_namenode_hosts()[self.active_idx]

    def create_and_write(self, file_path, content, is_overwrite=False, replication=2):
        op = "CREATE&overwrite={}&replication={}".format(
            str(is_overwrite).lower(),
            replication,
        )
        dn_loc = self.__access_hdfs_rest_api(file_path, op, method="PUT", is_ignore_3x=True, data=content).headers[
            "Location"
        ]
        return self.__urllib2_ignore_3x(dn_loc, "PUT", data=content).read()

    def create_and_write_large_file(self, file_path, file, is_overwrite=False, replication=2):
        # jetty 默认有 200000 字节的限制，分块大小不要超过该大小
        md5 = hashlib.md5()
        for chunk in file.chunks(chunk_size=FILE_UPLOAD_MAX_MEMORY_SIZE):
            md5.update(chunk)
            if not is_overwrite:
                op = "APPEND&replication=%s" % replication
                method = "POST"
            else:
                op = "CREATE&overwrite={}&replication={}".format(
                    str(is_overwrite).lower(),
                    replication,
                )
                method = "PUT"
            dn_loc = self.__access_hdfs_rest_api(file_path, op, method=method, is_ignore_3x=True, data=chunk).headers[
                "Location"
            ]
            self.__urllib2_ignore_3x(dn_loc, method, data=chunk).read()
            is_overwrite = False
        file.close()
        return md5.hexdigest()

    def open_and_read(self, file_path):
        op = "OPEN"
        res = self.__access_hdfs_rest_api(file_path, op)
        return res.read() if res else None

    def delete(self, file_path, is_recursive=False):
        op = "DELETE&recursive=%s" % str(is_recursive).lower()
        res = self.__access_hdfs_rest_api(file_path, op, method="DELETE")
        return json.loads(res.read()) if res else None

    def get_file_status(self, file_path):
        res = self.__access_hdfs_rest_api(file_path, "GETFILESTATUS")
        return json.loads(res.read()) if res else None

    def get_content_summary(self, file_path):
        """
        {
            "ContentSmmary": {
                "directoryCont": 280,
                "fileCont": 19591,
                "length": 563329295,
                "quota": -1,
                "spaceConsumed": 1126672954,
                "spaceQuota": -1
            }
        }
        """
        res = self.__access_hdfs_rest_api(file_path, "GETCONTENTSUMMARY")
        return json.loads(res.read()) if res else None

    def list_status(self, file_path):
        res = self.__access_hdfs_rest_api(file_path, "LISTSTATUS")
        return json.loads(res.read()) if res else None

    def list_xattrs(self, file_path):
        res = self.__access_hdfs_rest_api(file_path, "LISTXATTRS")
        return json.loads(res.read()) if res else None

    def mkdirs(self, file_path):
        res = self.__access_hdfs_rest_api(file_path, "MKDIRS", method="PUT")
        return json.loads(res.read()) if res else None

    def rename(self, file_path, new_file_path):
        move_dir = os.path.dirname(new_file_path)
        self.mkdirs(move_dir)
        op = "RENAME&destination=%s" % new_file_path
        res = self.__access_hdfs_rest_api(file_path, op, method="PUT")
        move_result = json.loads(res.read()) if res else None
        if not move_result or not move_result["boolean"]:
            # 移动文件前要确保 to_path 所在目录存在
            raise BatchException("文件移动失败({}->{})".format(file_path, new_file_path))
        return move_result

    def is_file_exists(self, file_path):
        return True if self.get_file_status(file_path) else False
