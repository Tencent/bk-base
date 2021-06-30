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
import re

import attr

from auth.api.meta import MetaApi


class STAFF_STATUS:
    # StatusId
    # 员工状态ID，可选值如下：
    # 1: 在职
    # 2: 离职
    # 3: 试用
    # 8: 待入职
    # 9: 未入职
    # TypeId
    # 员工类型ID，可选值如下：
    #
    # 1: 试用
    # 2: 正式
    # 3: 外聘
    # 4: 实习
    # 5: 合作伙伴
    # 6: 顾问
    # 7: 毕业生
    # 8: 短期劳务工
    # 9: 外包
    ON_DUTY = "1"
    DIMISSION = "2"


@attr.s
class AuthStaffInfo:
    staff_name = attr.ib(type=str, converter=str)
    status_id = attr.ib(type=str, converter=str)
    sync_time = attr.ib(type=str, converter=str, default="")


class StaffManager:

    QQ_REGULAR_FORMAT = r"^\d+$"
    CACHE_TIME = 3600

    def __init__(self):
        self.map_staff_objects = {}
        self.load_staff_objects()

    def is_dimission(self, staff_name):
        """
        判断员工是否在职
        """
        _staff = self.get_staff_info(staff_name)
        return _staff.status_id == STAFF_STATUS.DIMISSION

    def get_staff_info(self, staff_name):
        """
        获取员工信息
        """
        # 如果是 QQ 号，直接构造在职员工信息
        if self.is_qq(staff_name):
            return AuthStaffInfo(staff_name=staff_name, status_id=STAFF_STATUS.DIMISSION)

        if staff_name in self.map_staff_objects:
            return self.map_staff_objects[staff_name]

        return AuthStaffInfo(staff_name=staff_name, status_id=STAFF_STATUS.ON_DUTY)

    def load_staff_objects(self):
        """
        加载目前 DB 存有的员工信息
        """
        statement = """{
            target(func: has(Staff.login_name)) {
                staff_name: Staff.login_name
                status_id: Staff.status_id
                sync_time: Staff.synced_at
            }
        }"""
        content = MetaApi.entity_complex_search(
            {"statement": statement, "backend_type": "dgraph"}, raise_exception=True
        ).data["data"]["target"]

        for c in content:
            self.map_staff_objects[c["staff_name"]] = AuthStaffInfo(
                staff_name=c["staff_name"],
                status_id=c.get("status_id", STAFF_STATUS.ON_DUTY),
                sync_time=c.get("sync_time", ""),
            )

    def is_qq(self, staff_name):
        """
        判断是否为 QQ 号
        """
        return re.match(self.QQ_REGULAR_FORMAT, staff_name)

    @property
    def staff_memebers(self):
        return list(self.map_staff_objects.keys())
