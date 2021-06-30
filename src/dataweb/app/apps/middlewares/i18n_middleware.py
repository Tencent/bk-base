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
import json

import pytz
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from django.utils.deprecation import MiddlewareMixin

from apps.api import BKLoginApi


class I18NMiddleware(MiddlewareMixin):
    """
    国际化中间件，从BK_LOGIN获取个人配置的时区
    """

    def process_view(self, request, view, args, kwargs):
        login_exempt = getattr(view, "login_exempt", False)
        if login_exempt:
            return None

        user = request.user.username
        user_info = cache.get("{user}_user_info".format(user=user))

        if user_info is None:
            try:
                user_info = BKLoginApi.get_user({"username": user})
                cache.set("{user}_user_info".format(user=user), json.dumps(user_info), 30)
            except Exception:
                user_info = {}
        else:
            user_info = json.loads(user_info)

        tzname = user_info.get("time_zone", settings.TIME_ZONE)
        timezone.activate(pytz.timezone(tzname))
        request.session["bluking_timezone"] = tzname
