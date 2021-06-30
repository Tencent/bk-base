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

from common.log import logger
from datahub.common.const import EXPIRES, LIST_EXPIRE, NAME
from datahub.storekit.settings import EXPIRES_ZH_DAY_UNIT, EXPIRES_ZH_FOREVER
from django.utils.translation import ugettext as _


def translate_expires(cluster):
    """
    :param cluster: 集群信息
    :return: 翻译
    """
    expires = cluster[EXPIRES]
    trans_expires = dict()
    try:
        trans_expires = json.loads(expires)
    except Exception:
        logger.warning(f"load expires error, {expires}")

    if trans_expires:
        # 获取默认单位
        unit = trans_expires.get("unit", EXPIRES_ZH_DAY_UNIT)
        sing_expire = f"1{unit}"
        # 替换需要翻译的单位，天，月
        for expire in trans_expires[LIST_EXPIRE]:
            expire[NAME] = expire[NAME].replace(" ", "")
            expire[NAME] = expire[NAME].replace(sing_expire, _(sing_expire))
            expire[NAME] = expire[NAME].replace(unit, _(unit))
            expire[NAME] = expire[NAME].replace(EXPIRES_ZH_FOREVER, _(EXPIRES_ZH_FOREVER))

    return trans_expires
