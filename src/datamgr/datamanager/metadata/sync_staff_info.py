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
from __future__ import absolute_import, print_function, unicode_literals

import logging
import re

import attr
from metadata_client.exc import MetaDataError

from api import tof_api
from api.access_token import TOF_ACCESS_TOKEN
from common.db import connections
from common.meta import metadata_client
from metadata.local_metadata import Staff

logger = logging.getLogger(__name__)


class UserManager(object):
    QQ_REGULAR_FORMAT = r"^\d+$"
    TOF_ACCESS_TOKEN = TOF_ACCESS_TOKEN

    def __init__(self):
        pass

    def get_auth_user_ids(self):
        with connections["basic"].session() as session:
            # # 当前权限体系存有用户信息
            content = """
                SELECT DISTINCT user_id FROM auth_user_role where auth_status='normal';
            """
            result = session.execute(content)
            auth_user_ids = [item[0] for item in result]

        return filter(
            lambda _id: not re.match(self.QQ_REGULAR_FORMAT, _id), auth_user_ids
        )

    def get_existed_user_ids(self):
        content = """
            {
                target(func: has(Staff.typed)) {
                    Staff.login_name
                }
            }
        """
        result = metadata_client.query(content)
        return [d["Staff.login_name"] for d in result["data"]["target"]]

    def refresh_existed_user_tof_info(self):
        """
        刷新系统现存的用户信息
        """
        existed_user_ids = self.get_existed_user_ids()
        self.refresh_users_tof_info(existed_user_ids)

    def sync_new_user_tof_info(self):
        """
        同步增量用户的 TOF 信息
        """
        auth_user_ids = self.get_auth_user_ids()
        existed_user_ids = self.get_existed_user_ids()

        user_ids = list(
            filter(lambda _user_id: _user_id not in existed_user_ids, auth_user_ids)
        )
        self.refresh_users_tof_info(user_ids)

    def refresh_users_tof_info(self, user_ids):
        """
        同步用户 TOF 信息
        """
        success_num = 0
        failed_num = 0
        failed_users = []
        logger.info("Now prepare to sync, lenth={}".format(len(user_ids)))
        for _user_id in user_ids:
            try:
                params = {"access_token": self.TOF_ACCESS_TOKEN, "login_name": _user_id}
                response = tof_api.get_staff_info(params, raise_exception=True)
                data = self.transform(response.data)
                fields = attr.fields_dict(Staff)

                with metadata_client.session as session:
                    kwargs = {k: v for k, v in data.items() if k in fields}
                    session.create(Staff(**kwargs))
                    session.commit()

                success_num += 1
                logger.info("Succeed to refresh tof info, user_id={}".format(_user_id))

            except MetaDataError:
                failed_num += 1
                failed_users.append(_user_id)
                logger.exception(
                    "Fail to sync infomation to metadata, user_id={}".format(_user_id)
                )
            except Exception as e:
                failed_num += 1
                failed_users.append(_user_id)
                logger.exception(
                    "Fail to refresh user tof info, user_id={}, error={}".format(
                        _user_id, e
                    )
                )

        logger.info(
            "Sync result, success_num={}, failed_num={}".format(success_num, failed_num)
        )
        if len(failed_users) > 0:
            logger.info("Sync result, failed users={}".format(failed_users))

    @staticmethod
    def transform(item):
        """
        需要将 tof 的字段转换为 metadata 的标准字段，比如
            QQ -> qq
            ID -> id
            OfficialName -> officail_name
            ...
        """
        return {
            "id": item["ID"],
            "login_name": item["LoginName"],
            "wei_xin_user_name": item["WeiXinUserName"],
            "work_qq_number": item["WorkQQNumber"],
            "status_name": item["StatusName"],
            "status_id": item["StatusId"],
            "type_id": item["TypeId"],
            "post_name": item["PostName"],
            "work_dept_id": item["WorkDeptId"],
            "official_id": item["OfficialId"],
            "type_name": item["TypeName"],
            "work_dept_name": item["WorkDeptName"],
            "gender": item["Gender"],
            "ex_properties": item["ExProperties"],
            "official_name": item["OfficialName"],
            "rtx": item["RTX"],
            "department_name": item["DepartmentName"],
            "enabled": item["Enabled"],
            "group_name": item["GroupName"],
            "department_id": item["DepartmentId"],
            "english_name": item["EnglishName"],
            "qq": item["QQ"],
            "chinese_name": item["ChineseName"],
            "birthday": item["Birthday"],
            "full_name": item["FullName"],
            "post_id": item["PostId"],
            "mobile_phone_number": item["MobilePhoneNumber"],
            "group_id": item["GroupId"],
            "branch_phone_number": item["BranchPhoneNumber"],
        }


def sync_new_staff_info():
    """
    同步新增员工信息
    """
    UserManager().sync_new_user_tof_info()


def refresh_existed_staff_info():
    """
    刷新系统现存员工信息
    """
    UserManager().refresh_existed_user_tof_info()
