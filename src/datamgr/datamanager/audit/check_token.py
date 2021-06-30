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
import datetime
import logging
import time

from api.modules.cmsi import CmsiApi
from audit.templates import render
from auth import models
from common.db import connections
from conf.settings import APP_DATAWEB_URL, RUN_MODE

logger = logging.getLogger(__name__)

EXPIRED_DAYS = 14


def check_data_token():
    """
    执行data_token 检查的主要逻辑，获取列表->筛选列表->发送邮件
    只检查状态为expired 以及 enabled 的授权码
    """
    with connections["basic"].session() as session:
        data_token_list = get_data_token_list(
            session,
            [
                models.AuthDataTokenStatus.ENABLED.value,
                models.AuthDataTokenStatus.EXPIRED.value,
            ],
        )
        send_email_list = get_send_email_list(data_token_list)
        send_email(session, send_email_list)


def get_data_token_list(session, status):
    """
    获取所有符合条件的token列表,ALL代表获取全部
    """
    if status == "ALL":
        data_token_list = (
            session.query(models.AuthDataToken)
            .filter(
                models.AuthDataToken.status.in_(
                    [
                        models.AuthDataTokenStatus.ENABLED.value,
                        models.AuthDataTokenStatus.EXPIRED.value,
                        models.AuthDataTokenStatus.DISABLED.value,
                    ]
                )
            )
            .all()
        )
        return data_token_list
    data_token_list = (
        session.query(models.AuthDataToken)
        .filter(models.AuthDataToken.status.in_(status))
        .all()
    )
    return data_token_list


def get_send_email_list(data_token_list):
    """
    :param session: 数据库连接的session 对象
    :param data_token_list: 所有未过期的并且正在使用的token列表
    :return: 返回符合邮件发送的data_token列表
    """
    cur_point = datetime.datetime.now()

    send_email_list = []
    start_point = cur_point - datetime.timedelta(days=EXPIRED_DAYS)
    end_point = cur_point + datetime.timedelta(days=EXPIRED_DAYS)
    for data_token in data_token_list:
        if data_token.expired_at is None:
            continue
        if start_point <= data_token.expired_at <= end_point:
            send_email_list.append(data_token)
    return send_email_list


def get_permission_info(session, data_token_id):
    """
    获取到当前授权码下所有权限信息
    :return {
        '数据订阅': ['591_login_60s_indicator_1_4232_StormSqlAggregate_2']
    }
    """
    auth_data_token_permission_list = (
        session.query(models.AuthDataTokenPermission)
        .filter(
            models.AuthDataTokenPermission.data_token_id == data_token_id,
            models.AuthDataTokenPermission.status
            == models.AuthDataTokenPermissionStatus.ACTIVE.value,
        )
        .all()
    )

    permission_info = {}

    for auth_data_token_permission in auth_data_token_permission_list:
        action_name = get_action_name(session, auth_data_token_permission.action_id)
        if action_name in permission_info.keys():
            permission_info[action_name].append(auth_data_token_permission.scope_id)
        else:
            permission_info[action_name] = [auth_data_token_permission.scope_id]
    return permission_info


def get_action_name(session, action_id):
    """
    获取当前授权码对应的权限操作名
    """
    auth_action_config = (
        session.query(models.AuthActionConfig)
        .filter(models.AuthActionConfig.action_id == action_id)
        .first()
    )
    return auth_action_config.action_name


def get_user_role_list(session, data_token_id):
    """
    获取当前授权码对应的授权码管理员列表
    """
    user_role_list = (
        session.query(models.AuthUserRole)
        .filter(
            models.AuthUserRole.scope_id == str(data_token_id),
            models.AuthUserRole.role_id == "data_token.manager",
        )
        .all()
    )
    return user_role_list


def send_email(session, send_email_list):
    """
    执行具体的邮件发送逻辑
    """

    if RUN_MODE == "DEVELOP":
        title = "【IEG数据平台测试】请及时续期您的授权码"
    else:
        title = "【IEG数据平台】请及时续期您的授权码"

    send_count = 0
    user_role_dict = {}
    send_successful_list = []
    send_error_list = []
    cur_point = datetime.datetime.now()

    for data_token in send_email_list:
        user_role_list = get_user_role_list(session, data_token.id)
        permission_info = get_permission_info(session, data_token.id)

        for user_role in user_role_list:

            mail_object = {"data_token": data_token, "permission_info": permission_info}
            if user_role.user_id in user_role_dict.keys():
                user_role_dict[user_role.user_id].append(
                    _build_mail_objects(mail_object)
                )
            else:
                user_role_dict[user_role.user_id] = [_build_mail_objects(mail_object)]

    for user_id_key in user_role_dict.keys():
        try:
            content = render(
                "mail_one_token_member.jinja",
                {
                    "title": title,
                    "send_date": cur_point.strftime("%Y-%m-%d %H:%M:%S"),
                    "app_dataweb_url": APP_DATAWEB_URL,
                    "objects": user_role_dict[user_id_key],
                },
            )

            # 1分钟发送5封邮件，确认半小时内，不超过发送 200 封邮件
            if send_count > 0 and send_count % 5 == 0:
                logger.info(
                    "Waiting 1 minutes as mail send_count is {}".format(send_count)
                )
                time.sleep(60)

            CmsiApi.send_mail(
                {"receivers": user_id_key, "title": title, "content": content}
            )

            send_count += 1
            send_successful_list.append(user_id_key)
        except Exception as err:
            send_error_list.append(user_id_key)
            logger.error(
                "Audit check token send a email error,receiver={}, err={}".format(
                    user_id_key, err
                )
            )
        logger.info(
            "Audit check token is send email successfully, receiver={}".format(
                user_id_key
            )
        )

    logger.info(
        "The mails has been sent. successful_count={}, error_count={}, send_successful_list={}, "
        "send_error_list={}".format(
            len(send_successful_list),
            len(send_error_list),
            send_successful_list,
            send_error_list,
        )
    )


def update_data_token_status(session, data_token_id, status):
    """
    如果授权码超过14天没有续期，则修改状态为expired
    """
    data_token = (
        session.query(models.AuthDataToken)
        .filter(models.AuthDataToken.id == str(data_token_id))
        .first()
    )
    data_token.status = status
    session.commit()


def _build_mail_objects(mail_object):
    """
    构建一个mail_objects 用于邮件发送
    """
    return {
        "id": mail_object["data_token"].id,
        "data_token": mail_object["data_token"].data_token[0:3]
        + "*" * 5
        + mail_object["data_token"].data_token[-3:],
        "app_code": mail_object["data_token"].data_token_bk_app_code,
        "expired_id": mail_object["data_token"].expired_at.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "description": mail_object["data_token"].description,
        "permission_info": mail_object["permission_info"],
    }


def update_token_status_batch():
    """
    批量更新激活码状态
    """
    update_ids = []

    with connections["basic"].session() as session:

        data_token_list = get_data_token_list(session, "ALL")
        cur_point = datetime.datetime.now()

        for data_token in data_token_list:
            if data_token.expired_at is None:
                continue
            if cur_point < data_token.expired_at:
                if (
                    data_token.status != models.AuthDataTokenStatus.DISABLED.value
                    and data_token.status != models.AuthDataTokenStatus.ENABLED.value
                ):
                    update_data_token_status(
                        session, data_token.id, models.AuthDataTokenStatus.ENABLED.value
                    )
                    update_ids.append(data_token.id)
            if (
                data_token.expired_at
                < cur_point
                <= data_token.expired_at + datetime.timedelta(days=EXPIRED_DAYS)
            ):
                if data_token.status != models.AuthDataTokenStatus.EXPIRED.value:
                    update_data_token_status(
                        session, data_token.id, models.AuthDataTokenStatus.EXPIRED.value
                    )
                    update_ids.append(data_token.id)
            if (
                data_token.expired_at + datetime.timedelta(days=EXPIRED_DAYS)
                < cur_point
            ):
                update_data_token_status(
                    session, data_token.id, models.AuthDataTokenStatus.REMOVED.value
                )
                update_ids.append(data_token.id)

    logger.info(
        "Batch status update completed. update_count={}, update_successful_list={}".format(
            len(update_ids), update_ids
        )
    )
