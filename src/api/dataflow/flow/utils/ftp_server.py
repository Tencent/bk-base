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

from dataflow.flow.models import FlowFileUpload
from dataflow.pizza_settings import HDFS_UPLOADED_FILE_DIR, HDFS_UPLOADED_SDK_FILE_DIR
from dataflow.shared.batch.batch_helper import BatchHelper
from dataflow.shared.log import flow_logger as logger


class FtpServer(object):
    ftp_type = "hdfs"

    UPLOADED_FILE_DIR_MAP = {
        "tdw": HDFS_UPLOADED_FILE_DIR,
        "sdk": HDFS_UPLOADED_SDK_FILE_DIR,
    }

    def __init__(self, cluster_group):
        self.cluster_group = cluster_group

    def clean_expired_file(self, directory, expires=2):
        """
        清理指定目录的过期文件
        @param directory:
        @param expires:
        @return:
        """
        logger.info("开始清理过期 %s 文件" % self.ftp_type)
        res_data = BatchHelper.hdfs_list_status(self.cluster_group, directory)
        current_time = int(round(time.time() * 1000))
        # 临时文件过期时间为两天
        expire_time = current_time - expires * 24 * 3600 * 1000
        uploaded_file_to_clean = []
        if res_data:
            for file_status in res_data["FileStatuses"]["FileStatus"]:
                access_time = file_status["accessTime"]
                if access_time < expire_time and file_status["pathSuffix"]:
                    file_path = "{}{}".format(directory, file_status["pathSuffix"])
                    logger.info(
                        "临时文件过期(%s)，即将删除：%s"
                        % (
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(access_time / 1000)),
                            file_path,
                        )
                    )
                    uploaded_file_to_clean.append(file_path)
        clean_res_data = {}
        if uploaded_file_to_clean:
            clean_res_data = BatchHelper.hdfs_clean(self.cluster_group, uploaded_file_to_clean)
        return clean_res_data

    def clean_unused_uploaded_file(self, expires=2):
        """
        已创建节点重新更新上传文件，清理无效文件
        @param expires:
        @return:
        """
        logger.info("开始清理无效 %s 文件" % self.ftp_type)
        uploaded_file_to_clean = []
        # 设置过期时间 buffer
        expire_time = int(round(time.time() * 1000)) - expires * 24 * 3600 * 1000
        for file_type, file_dir in list(FtpServer.UPLOADED_FILE_DIR_MAP.items()):
            res_data = BatchHelper.hdfs_list_status(self.cluster_group, file_dir)
            if res_data:
                for file_status in res_data["FileStatuses"]["FileStatus"]:
                    access_time = file_status["accessTime"]
                    file_path = "{}{}".format(file_dir, file_status["pathSuffix"])
                    # 文件所在绝对路径不在文件上传表 PATH 中, 并且 accessTime 小于当前时间两天
                    if not FlowFileUpload.objects.filter(path=file_path) and access_time < expire_time:
                        logger.info(
                            "%s文件未被使用(%s)，即将删除：%s"
                            % (
                                file_type,
                                time.strftime(
                                    "%Y-%m-%d %H:%M:%S",
                                    time.localtime(access_time / 1000),
                                ),
                                file_path,
                            )
                        )
                        uploaded_file_to_clean.append(file_path)
        clean_res_data = {}
        if uploaded_file_to_clean:
            clean_res_data = BatchHelper.hdfs_clean(self.cluster_group, uploaded_file_to_clean)
        return clean_res_data
