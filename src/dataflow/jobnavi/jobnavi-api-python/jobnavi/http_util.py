# coding=utf-8
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
import urllib

import httplib2

from jobnavi.jobnavi_logging import get_logger

logger = get_logger()


def http_get(url, retry=0, timeout=10):
    try:
        resp, content = httplib2.Http(timeout=timeout).request(url, "GET")
    except Exception as e:
        logger.exception(e.message)
        return False
    if resp.status == 200:
        try:
            data = json.loads(content)
            return data
        except Exception as e:
            logger.exception(e.message)
            return content
    else:
        logger.error("request " + url + " error, return status is " + str(resp.status))
        if retry > 0:
            return http_get(url, retry - 1)
        return False


def http_post(url, post=None, retry=0, timeout=30, headers=None):
    if headers is None:
        headers = {}
    try:
        if type(post) in [list, dict]:
            post = urllib.urlencode(post)
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        else:
            headers["Content-Type"] = "application/json"
        resp, content = httplib2.Http(timeout).request(url, method="POST", body=post, headers=headers)
    except Exception as e:
        logger.info("url->%s,error->%s,post->%s" % (url, e.message, post))
        return False
    if resp.status in [200, 202]:
        try:
            data = json.loads(content)
            return data
        except Exception as e:
            logger.exception(e.message)
            return content
    else:
        logger.info("url->%s,status->%s,content->%s" % (url, resp.status, content))
        if retry > 0:
            return http_post(url, post=post, retry=retry - 1)
        return False
