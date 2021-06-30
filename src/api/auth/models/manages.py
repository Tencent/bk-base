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
from common.local import get_request_username
from django.db import models


class BaseManager(models.Manager):
    """
    主要处理内部操作时，自动补全必要字段，比如 created_by updated_by,
    外键查询时，关联查询，减少DB查询次数
    """

    def create(self, *args, **kwargs):
        if "created_by" not in kwargs:
            kwargs["created_by"] = get_request_username()
        if "updated_by" not in kwargs:
            kwargs["updated_by"] = get_request_username()
        return super().create(*args, **kwargs)

    def bulk_create(self, *args, **kwargs):
        for _obj in args[0]:
            if not _obj.created_by:
                _obj.created_by = get_request_username()
            if not _obj.updated_by:
                _obj.updated_by = get_request_username()

        return super().bulk_create(*args, **kwargs)
