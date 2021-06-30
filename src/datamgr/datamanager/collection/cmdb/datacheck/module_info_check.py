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
"""
对主机信息进行批量对账检查
"""
import logging

from collection.cmdb.datacheck.base import CMDBBaseCheckerMixin
from collection.cmdb.module_info import CMDBModuleInfoCollector
from collection.conf.constants import BKDATA_BIZ_ID, CMDB_MODULE_TABLE_NAME

logger = logging.getLogger(__name__)


class CMDBModuleInfoChecker(CMDBModuleInfoCollector, CMDBBaseCheckerMixin):
    key_name = "bk_module_id"

    def __init__(self, config=None, init_producer=False):
        super(CMDBModuleInfoChecker, self).__init__(config, init_producer=init_producer)
        if "rt_id" in config:
            self.rt_id = config["rt_id"]
        else:
            self.rt_id = f"{config['bk_biz_id']}_{config['raw_data_name']}"


def check_cmdb_module_info(params=None):
    if not params:
        params = {"bk_biz_id": BKDATA_BIZ_ID, "raw_data_name": CMDB_MODULE_TABLE_NAME}
    c = CMDBModuleInfoChecker(config=params)
    c.check_all_biz()


def check_cmdb_module_info_by_one(bk_biz_id):
    params = {"bk_biz_id": BKDATA_BIZ_ID, "raw_data_name": CMDB_MODULE_TABLE_NAME}
    c = CMDBModuleInfoChecker(config=params)
    c.check_biz(bk_biz_id)


if __name__ == "__main__":
    check_cmdb_module_info()
