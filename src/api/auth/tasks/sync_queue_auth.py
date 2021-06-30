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
import random
import time

from auth.api import DatabusApi, MetaApi
from auth.models import AuthDataTokenPermission, DataTokenQueueUser
from celery.schedules import crontab
from celery.task import periodic_task, task
from common.log import logger


@task(queue="auth")
def add_queue_auth(data_token_result_table_list):
    """
    授予队列权限，由于授权的耗时较长，将调整为异步任务模式

    @paramExample data_token_result_table_list
        [
            (1, '1_xx'),
            (1, '2_xx'),
            (1, '3_xx')
        ]
    """
    # 临时解决方案，如果该任务在新增授权码，自动过单立即添加权限时，由于异步执行，异步进程中，新增授权码还未提交，需要等待一下
    time.sleep(10)
    for data_token_id, result_table_id in data_token_result_table_list:
        try:
            queue_user = DataTokenQueueUser.get_or_init_by_id(data_token_id)
            queue_user.register_result_tables([result_table_id])
            logger.info(
                ("[ADD_QUEUE_AUTH] Succeed to register rt for datatoken, data_token={}, " "result_table_id={}").format(
                    data_token_id, result_table_id
                )
            )
        except Exception as err:
            logger.exception(
                "[ADD_QUEUE_AUTH] Fail to register rt for datatoken, data_token={}, result_table_id={}, "
                "error={}".format(data_token_id, result_table_id, err)
            )


@periodic_task(run_every=crontab(minute="*/10"), queue="auth")
def sync_queue_by_project(gap=True):
    """
    按照项目维度授权的队列服务
    @todo：后续期望不支持以项目为维度进行授权，更不希望后续还有以业务为维度进行授权，这样会导致授权范围不稳定，出现权限漏洞
    """
    if gap:
        # gap_seconds 用于在任务被调度时，隔个随机秒数再被执行，避免多机部署 celery beat 在同一时间点执行
        gap_seconds = random.randint(0, 60)
    else:
        gap_seconds = 1

    logger.info(f"[SYNC_QUEUE_AUTH] Start to sync, gap seconds={gap_seconds}")
    time.sleep(gap_seconds)

    token_user_rts = extract_token_queue_auth()
    kafka_user_rts = extract_kafka_queue_auth()

    for user, rts in list(token_user_rts.items()):
        if user not in kafka_user_rts:
            logger.exception(f"[SYNC_QUEUE_AUTH] Token user not in kakfa, queue_user={user}")
            continue

        _need_add_rts = [rt for rt in rts if rt not in kafka_user_rts[user]]

        if len(_need_add_rts) == 0:
            logger.info(f"[SYNC_QUEUE_AUTH] No rts need to add queue auth, user={user}")
            continue

        times = 3

        while times:
            response = DatabusApi.add_queue_auth({"user": user, "result_table_ids": _need_add_rts})

            if response.is_success() and response.data:
                logger.info(
                    "[SYNC_QUEUE_AUTH] Succeed to add queue auth, "
                    "user={}, reuslt_table_ids={}".format(user, _need_add_rts)
                )
                break

            times -= 1

        if times == 0:
            logger.exception(
                "[SYNC_QUEUE_AUTH] Retry to add queue auth 3 times and failed, "
                "user={}, result_table_ids={}".format(user, _need_add_rts)
            )

    logger.info("[SYNC_QUEUE_AUTH] Execution ended")


def extract_token_queue_auth():
    """
    从 Token 配置库中提取按照项目授权的队列权限关系
    @returnExample:
        {
            'gem&&xxxdkffkda': ['1_rt1', '1_rt2']
        }
    """
    perms = AuthDataTokenPermission.objects.filter(
        action_id="result_table.query_queue", scope_id_key="project_id", status="active"
    )
    user_rts = {}

    for _perm in perms:
        _token = _perm.data_token.data_token
        _project_id = _perm.scope_id
        try:
            user = DataTokenQueueUser.objects.get(data_token=_token).queue_user
            rts = []

            api_params = {
                "project_id": _project_id,
                "need_storage_detail": 0,
                "related_filter": json.dumps(
                    {"type": "storages", "attr_name": "common_cluster.cluster_type", "attr_value": "queue"}
                ),
            }

            rts.extend(MetaApi.list_result_table(api_params).data)

            api_params = {
                "project_id": _project_id,
                "need_storage_detail": 0,
                "related_filter": json.dumps(
                    {"type": "storages", "attr_name": "common_cluster.cluster_type", "attr_value": "queue_pulsar"}
                ),
            }

            rts.extend(MetaApi.list_result_table(api_params).data)

            # 清理出仅有队列存储的RT，二次确认，确保仅授权有 queue 存储的 RT
            queue_result_table_ids = list(
                {
                    rt["result_table_id"]
                    for rt in rts
                    if "queue" in list(rt["storages"].keys()) or "queue_pulsar" in list(rt["storages"].keys())
                }
            )

            if user not in user_rts:
                user_rts[user] = []

            user_rts[user].extend(queue_result_table_ids)

        except DataTokenQueueUser.DoesNotExist:
            logger.exception(f"[SYNC_QUEUE_AUTH] No queue user record for data_token={_token}")
        except Exception as e:
            logger.exception(
                "[SYNC_QUEUE_AUTH] Failed to Extract token rts, " "data_token={}, error={}".format(_token, e)
            )

    return user_rts


def extract_kafka_queue_auth():
    """
    从 Kafka 集群上提取已注册的队列权限关系
    @returnExample:
        {
            'gem&&xxxdkffkda': ['1_rt1', '1_rt2']
        }
    """
    content = DatabusApi.query_queue_auth().data

    user_rts = {}
    for cluster, topics_wrap in list(content.items()):

        topics = topics_wrap["topic"]

        for topic_name, users in list(topics.items()):

            result_table_id = topic_name[12:]
            user_ids = list({user.split(" ")[0][5:] for user in users})

            for user_id in user_ids:
                if user_id not in user_rts:
                    user_rts[user_id] = []

                user_rts[user_id].append(result_table_id)

    return user_rts
