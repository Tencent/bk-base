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

from common.business import Business
from common.log import logger
from datahub.access.utils import cache


class AccessBusiness(Business):
    def __init__(self, bk_biz_id):
        super(AccessBusiness, self).__init__(bk_biz_id)

    @classmethod
    def transfom_biz_name(cls, data):
        if not cache.BIZ_INFO["biz"]:
            logger.info(u"mine_list: no biz name cache , need query")
            _name_dict = cls.get_name_dict()
            if _name_dict:
                for k, v in _name_dict.items():
                    cache.BIZ_INFO[k] = v if v else u"未知"
            cache.BIZ_INFO["biz"] = "*"

        logger.info(u"mine_list: start replace bk_biz_name")
        for _d in data:
            _temp_data = _d
            biz_id = str(_temp_data.get("bk_biz_id"))
            _temp_data["bk_biz_name"] = cache.BIZ_INFO[biz_id] if cache.BIZ_INFO[biz_id] else u""

        logger.info(u"mine_list: end replace bk_biz_name")

        return data
