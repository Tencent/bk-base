# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
from datetime import datetime, timedelta

from celery.schedules import crontab
from celery.task import periodic_task

from apps.api.models import DataAPIRecord


@periodic_task(run_every=crontab(minute="0", hour="0"))
def delete_api_log():
    """
    每天清理dataapi日志
    """
    # 清理一周前的日志
    how_many_days = 7
    DataAPIRecord.objects.filter(request_datetime__lte=datetime.now() - timedelta(days=how_many_days)).delete()
