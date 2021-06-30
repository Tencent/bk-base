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
from datahub.access.raw_data.rawdata import (
    create_access_raw_data,
    retrieve_access_raw_data,
    update_access_raw_data,
)


class RawDataHandler(object):
    """
    原始数据处理器
    """

    def __init__(self, raw_data_id=None, show_display=1):
        self.raw_data_id = raw_data_id
        self.show_display = show_display
        self._data = None

    @classmethod
    def create(cls, param):
        raw_data_id = create_access_raw_data(param)
        return cls(raw_data_id)

    @classmethod
    def update(cls, raw_data_id, param):
        param["raw_data_id"] = raw_data_id
        update_access_raw_data(raw_data_id, param)
        return cls(raw_data_id)

    def retrieve(self):
        return retrieve_access_raw_data(self.raw_data_id, self.show_display)

    @property
    def data(self):
        if self._data is None:
            self._data = self.retrieve()

        return self._data
