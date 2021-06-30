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
import httplib2

from common.log import logger

from datamanage.exceptions import DatamanageErrorCode

try:
    from django.conf import settings

    COMPONENT_CONFIG = settings.COMPONENT_CONFIG
    SVR_CONFIG = settings.SVR_CONFIG
except Exception as e:
    logger.warning(f'{str(e)}')
    COMPONENT_CONFIG = {}
    SVR_CONFIG = {'MAPLE_QUERY_URL': ''}


def maple_query(sql, retry=3, timeout=60):
    app_code = COMPONENT_CONFIG.get('APP_CODE', '')
    app_secret = COMPONENT_CONFIG.get('SECRET_KEY', '')
    url = SVR_CONFIG.get('MAPLE_QUERY_URL', '')
    args = {'app_code': app_code, 'app_secret': app_secret, 'sql': sql}

    try:
        resp, content = httplib2.Http(timeout=timeout).request(
            url, 'POST', json.dumps(args), headers={'content-type': 'application/json; charset=utf-8'}
        )
    except Exception as e:
        logger.error('http query %s failed, reason %s' % (url, e), DatamanageErrorCode.HTTP_POST_ERR)
        return False
    if resp.status == 200:
        try:
            data = json.loads(content)
            return data
        except Exception as e:
            logger.warning(f'{str(e)}')
            return content
    else:
        logger.error('http query %s failed, status code %s' % (url, resp.status), DatamanageErrorCode.HTTP_STATUS_ERR)
        if retry > 0:
            return maple_query(sql, retry - 1)
        return False
