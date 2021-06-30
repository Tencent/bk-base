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

from api import tof_api
from api.access_token import TOF_ACCESS_TOKEN
from auth.core.permission import RoleController
from common.db import connections
from common.meta import metadata_client

logger = logging.getLogger(__name__)


def clean_members(arr):
    """
    去重，移除空字符
    """
    return [a.strip() for a in list(set(arr)) if a.strip()]


def load_v1_apps():
    statement = """
        {
          target(func: has(BizV1.typed)) {
            bk_biz_id: BizV1.ApplicationID
            bk_biz_name: BizV1.ApplicationName
            managerv1: BizV1.Maintainers
            managerv2: BizV1.OperationPlanning
            developer: BizV1.AppDevMan
            tester: BizV1.PmpTestTM
            productor: BizV1.PmpProductMan
          }
        }
    """
    content = metadata_client.query(statement)
    data = content["data"]["target"]
    for d in data:
        d["manager"] = clean_members(
            d["managerv1"].split(";") + d["managerv2"].split(";")
        )
        d["developer"] = clean_members(d["developer"].split(";"))
        d["tester"] = clean_members(d["tester"].split(";"))
        d["productor"] = clean_members(d["productor"].split(";"))

    return data


def load_v3_apps():
    statement = """
        {
          target(func: has(BizV3.typed)) {
            bk_biz_id: BizV3.bk_biz_id
            bk_biz_name: BizV3.bk_biz_name
            managerv1: BizV3.bk_oper_plan
            managerv2: BizV3.bk_biz_maintainer
            developer: BizV3.bk_biz_developer
            tester: BizV3.bk_biz_tester
            productor: BizV3.bk_biz_productor
          }
        }
    """
    content = metadata_client.query(statement)
    data = content["data"]["target"]
    for d in data:
        d["manager"] = clean_members(
            d.get("managerv1", "").split(",") + d["managerv2"].split(",")
        )
        d["developer"] = clean_members(d["developer"].split(","))
        d["tester"] = clean_members(d["tester"].split(","))
        d["productor"] = clean_members(d["productor"].split(","))

    return data


def sync_members():
    """
    同步CC业务信息
    """
    app_list = load_v1_apps() + load_v3_apps()
    mapping_apps = dict()
    for app in app_list:
        if app["bk_biz_id"]:
            mapping_apps[app["bk_biz_id"]] = app

    logger.info(
        "[MEMBERS SYNC] Start sync members, business count={}".format(len(mapping_apps))
    )

    err_tmp = "[MEMBERS SYNC ERROR] app_id={}, error={}"
    success_num = 0
    failure_num = 0

    role_controller = RoleController()

    # 拉取现存所有业务角色成员列表，需要注意 get_role_members_with_all_scope 得到的组装结构 scope_id 为字符串
    with connections["basic"].session() as session:
        mapping_role_member = {
            "manager": role_controller.get_role_members_with_all_scope(
                session, "biz.manager"
            ),
            "developer": role_controller.get_role_members_with_all_scope(
                session, "biz.developer"
            ),
            "tester": role_controller.get_role_members_with_all_scope(
                session, "biz.tester"
            ),
            "productor": role_controller.get_role_members_with_all_scope(
                session, "biz.productor"
            ),
        }

    for _app in mapping_apps.values():
        bk_biz_id = _app.get("bk_biz_id", "")

        if (success_num + failure_num) % 100 == 0:
            logger.info("[MEMBERS SYNC] Process {}".format(success_num + failure_num))

        try:
            bk_biz_id = _app["bk_biz_id"]

            with connections["basic"].session() as session:
                for role_name in ["manager", "developer", "tester", "productor"]:
                    new_members = sorted(_app[role_name])
                    old_members = sorted(
                        mapping_role_member[role_name].get(str(bk_biz_id), [])
                    )

                    # 仅有改变时，才做变更
                    if new_members != old_members:
                        role_id = "biz.{}".format(role_name)
                        role_controller.update_role_members(
                            session, role_id, bk_biz_id, new_members
                        )
                        session.commit()

            success_num += 1
        except Exception as e:
            failure_num += 1
            logger.exception(err_tmp.format(bk_biz_id, e))

    conclusion = "[MEMBERS SYNC SUCCEED] failure_num={}, success_num={}".format(
        failure_num, success_num
    )
    logger.info(conclusion)


def sync_leader():
    role_controller = RoleController()

    err_tmp = "[LEADER SYNC ERROR] app_id={}, error={}"
    success_num = 0
    failure_num = 0

    with connections["basic"].session() as session:
        role_members = role_controller.get_role_members_with_all_scope(
            session, "biz.manager"
        )
        logger.info(
            "[LEADER SYNC] Start sync leader, business count={}".format(
                len(role_members)
            )
        )

        for bk_biz_id, users in role_members.items():

            if (success_num + failure_num) % 100 == 0:
                logger.info(
                    "[LEADER SYNC] Process {}".format(success_num + failure_num)
                )

            try:
                # 选择第一个运维作为基准，向上寻找直属业务总监，找不着则遍历下一个运维
                leader = None
                for user in users:
                    leader = get_team_leader(user)
                    if leader is not None:
                        break

                if leader is None:
                    raise Exception("NoLeader for {}".format(users))

                role_controller.update_role_members(
                    session, "biz.leader", bk_biz_id, [leader]
                )
                session.commit()
                success_num += 1
            except Exception as err:
                failure_num += 1
                logger.exception(err_tmp.format(bk_biz_id, err))

    conclusion = "[LEADER SYNC SUCCEED] failure_num={}, success_num={}".format(
        failure_num, success_num
    )
    logger.info(conclusion)


def get_team_leader(user_id):
    """
    业务总监
    """
    # 员工信息中的 OfficialId 表示领导级别
    # 7、8      普通员工
    # 6         TeamLeader（副组长、组长、总监、副总监）
    # 5、4、3    助理总经理/总经理
    # 2         主管副总（副总裁）
    # 1         总裁

    auth_info = {"access_token": TOF_ACCESS_TOKEN, "login_name": user_id}
    last_user_id = user_id
    staff_info = tof_api.get_staff_info(auth_info).data
    for i in range(4):
        if staff_info is None or "OfficialId" not in staff_info:
            return None
        elif int(staff_info["OfficialId"]) > 6:
            pass
        elif int(staff_info["OfficialId"]) < 6:
            # 如果拉取的员工级别较高时，默认取上一级员工
            return last_user_id
        elif "总监" in staff_info["PostName"]:
            return staff_info["LoginName"]

        last_user_id = staff_info["LoginName"]
        auth_info["login_name"] = last_user_id
        staff_info = tof_api.get_staff_direct_leader(auth_info).data

    return None
