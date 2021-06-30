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
import time

from api import token_api
from api.models import AccessTokenConfig
from common.db import connections

logger = logging.getLogger(__name__)


def get_lasted_access_token():
    """
    返回数据库最新的access_token,如果access_token过期，则刷新
    """
    with connections["basic"].session() as session:
        access_token_config = session.query(AccessTokenConfig).get(1)
        # 如果当前时间超过access_token的过期时间
        if time.time() > access_token_config.expired_at:
            params = {
                "app_code": access_token_config.app_code,
                "refresh_token": access_token_config.refresh_token,
                "env_name": access_token_config.env_name,
                "grant_type": "refresh_token",
            }
            token_info = token_api.refresh_token(params, raise_exception=True)
            # 更新access_token 信息
            access_token_config.access_token = token_info.data["access_token"]
            # 更新过期时间
            access_token_config.expired_at = int(
                time.time() + token_info.data["expires_in"]
            )
            session.commit()
        return access_token_config.access_token


TOF_ACCESS_TOKEN = get_lasted_access_token()
