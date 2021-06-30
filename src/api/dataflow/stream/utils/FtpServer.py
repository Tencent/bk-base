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

from conf.dataapi_settings import MULTI_GEOG_AREA

from dataflow.shared.log import udf_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.stream.exceptions.comp_execptions import FtpServerInfoError
from dataflow.stream.settings import DEFAULT_GEOG_AREA_CODE


class FtpServer(object):

    ftp_type = "hdfs"

    def __init__(self, cluster_group):
        self.cluster_group = cluster_group

    @classmethod
    def list_cluster_groups(cls):
        if MULTI_GEOG_AREA:
            geog_area_codes = TagHelper.list_tag_geog()
        else:
            geog_area_codes = [DEFAULT_GEOG_AREA_CODE]
        cluster_groups = []
        for geog_area_code in geog_area_codes:
            cluster_group = FtpServer.get_ftp_server_cluster_group(geog_area_code)
            if cluster_group in cluster_groups:
                logger.exception("不同地域不可使用相同文件服务器: %s" % cluster_group)
                raise FtpServerInfoError()
            cluster_groups.append(cluster_group)
        return cluster_groups

    @classmethod
    def get_ftp_server_cluster_group(cls, geog_area_code):
        return StorekitHelper.get_default_storage(FtpServer.ftp_type, geog_area_code)["cluster_group"]
