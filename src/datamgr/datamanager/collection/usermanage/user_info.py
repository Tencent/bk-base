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
用户信息采集上报
"""
import json
import logging

from api import usermanage_api
from collection.common.collect import BKDRawDataCollector
from collection.conf import constants

logger = logging.getLogger(__name__)


class UserInfoCollector(BKDRawDataCollector):
    """
    全量用户信息采集
    """

    object_type = "user"

    def __init__(self, config: dict, init_producer=True):
        super(UserInfoCollector, self).__init__(config, init_producer=init_producer)

    def object_info_filter(self, user_info: dict):
        """
        上报前处理用户消息
        """
        # 脱敏(暂全量上报，在维度模型中筛选)
        # 提取主部门信息
        if user_info["departments"]:
            user_info["main_department"] = user_info["departments"][0]["full_name"]
            user_info["main_department_id"] = user_info["departments"][0]["id"]
        for k, v in user_info.items():
            if isinstance(v, (list, dict)):
                user_info[k] = json.dumps(v)
        return user_info

    def report(self):
        self.batch_report()

    def batch_report(self):
        """
        全量获取上报
        """
        all_user_list_info_generator = usermanage_api.generate_all_user_list({})
        logger.info("Start batch report users info.")
        success_count = 0
        failed_count = 0
        for user_list_page in all_user_list_info_generator:
            for user in user_list_page:
                try:
                    self.produce_message(self.object_info_filter(user))
                    success_count += 1
                except Exception as e:
                    logger.error(f"Failed to report user {user} for error {e}")
                    failed_count += 1
        logger.info(
            f"Finish user info batch report. Success: {success_count}. Failed: {failed_count}."
        )


def batch_collect_user_info(params=None):
    if not params:
        params = {
            "bk_biz_id": constants.BKDATA_BIZ_ID,
            "raw_data_name": constants.USER_INFO_TABLE_NAME,
        }
    UserInfoCollector(params).batch_report()


if __name__ == "__main__":
    batch_collect_user_info()
