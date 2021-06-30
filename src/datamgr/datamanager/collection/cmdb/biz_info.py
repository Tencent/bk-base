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

from api import cmdb_api
from collection.cmdb.base import CMDBBaseCollector
from collection.conf import constants

logger = logging.getLogger(__name__)


class CMDBBizInfoCollector(CMDBBaseCollector):
    """
    cmdb 业务信息采集
    """

    object_type = "biz"
    fields = constants.CMDB_BIZ_FIELDS
    key_name = constants.CMDB_BIZ_PK_NAME

    def __init__(self, config: dict, init_producer=True):
        super(CMDBBizInfoCollector, self).__init__(config, init_producer=init_producer)

    def batch_report(self):
        """
        全量获取上报
        """
        logger.info("Start to load bizs.")
        success_count = 0
        failure_count = 0
        try:

            logger.info("[BIZ] Start to load bizs.")
            bk_biz_info = cmdb_api.search_business({}, raise_exception=True)
            bizs_list = bk_biz_info.data["info"]
            logger.info(f"[BIZ] Succeed to load {len(bizs_list)} bizs")

            for biz_index, h in enumerate(bizs_list):
                biz_info = self.filter_columns(h)
                try:
                    self.produce_message(message=biz_info)
                except Exception as err:
                    logger.exception(
                        f"[BIZ] Fail to write message({biz_info}) to kafka, {err}"
                    )
                    failure_count += 1
                else:
                    success_count += 1

                if biz_index % 500 == 0:
                    logger.info(f"[BIZ] Now process {biz_index + 1}th biz.")
        except Exception as err:
            logger.exception(f"[BIZ] Fail to batch report biz, {err}")

        logger.info(
            f"[BIZ] Result: success_count={success_count}, failure_count={failure_count}"
        )


def batch_collect_cmdb_biz_info(params=None):
    if not params:
        params = {
            "bk_biz_id": constants.BKDATA_BIZ_ID,
            "raw_data_name": constants.CMDB_BIZ_TABLE_NAME,
        }
        CMDBBizInfoCollector(params).batch_report()


if __name__ == "__main__":
    batch_collect_cmdb_biz_info()
