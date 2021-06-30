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

from django.utils.translation import ugettext as _

from dataflow.batch.utils.hdfs_util import HDFS
from dataflow.shared.log import batch_logger


def check_hdfs():
    """
    检查hdfs可用性
        - namenode
        - datanode
    """
    try:
        rtn = {}
        status = True

        # 先检查NN
        all_nn_status = HDFS.check_nn_status()
        all_dn_list = HDFS.check_datanode()

        for cluster in list(all_nn_status.keys()):
            nn_status = all_nn_status[cluster]
            dn_list = all_dn_list[cluster]
            rtn.update(
                {
                    cluster: {
                        "namenode": {"message": "", "status": True},
                        "datanode": {"message": "", "status": True},
                    }
                }
            )
            if "active" not in list(nn_status.values()):
                rtn[cluster]["namenode"] = {
                    "message": _("NameNode状态异常： %s") % json.dumps(nn_status),
                    "status": False,
                }
                status = False
            if len(dn_list) <= 1:
                rtn[cluster]["datanode"] = {
                    "message": _("存活DataNode小于2个。存活dn列表： %s") % json.dumps(dn_list),
                    "status": False,
                }
                status = False
        return status, rtn
    except Exception as e:
        batch_logger.exception(e)
        return False, _("访问WebHDFS REST API异常: {}".format(e))
