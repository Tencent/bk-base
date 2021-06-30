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
from common.api import MetaApi
from common.exceptions import BaseAPIError
from common.log import logger


class APIModel:
    def __init__(self, *args, **kwargs):
        self._data = None

    def _get_data(self):
        """
        获取基本数据方法，用于给子类重载
        """
        return None

    @property
    def data(self):
        if self._data is None:
            self._data = self._get_data()

        return self._data


class Business(APIModel):
    """
    单个业务标准的信息内容
    {
        "bk_biz_id": 111,
        "bk_biz_name": 'DNF'
    }
    """

    def __init__(self, bk_biz_id):
        super(Business, self).__init__()
        self.bk_biz_id = bk_biz_id

    @classmethod
    def list(cls, raise_exception=False, *args, **kwargs):
        app_list = []
        try:
            app_list = MetaApi.bizs.list().data
            if not app_list:
                app_list = []
        except BaseAPIError as e:
            logger.error("[CC ERROR] get_app_list fail, error={}".format(e))
            if raise_exception:
                raise e

        return app_list

    @classmethod
    def get_name_dict(cls):
        biz_list = cls.list(standard=True)
        return {str(_biz["bk_biz_id"]): _biz["bk_biz_name"] for _biz in biz_list}

    @classmethod
    def wrap_biz_name(cls, data, val_key="bk_biz_id", display_key="bk_biz_name", key_path=None):
        """
        数据列表，统一加上业务名称
        @param {Int} val_key 业务 ID 的 KEY 值
        @param {String} display_key 业务名称的 KEY 值
        @param {List} key_path val_key在data中所在的字典路径
        @paramExample 参数样例
            {
                "data": [
                    {
                        "target_details": {
                            "xxx": {
                                "bk_biz_id": 3
                            }
                        }
                    }
                ],
                "val_key": "bk_biz_id",
                "display_key": "bk_biz_name",
                "key_path": ["target_details", "xxx"]
            }
        @successExample 成功返回
            [
                {
                    "target_details": {
                        "xxx": {
                            "bk_biz_id": 3
                            "bk_biz_name": u"蓝鲸",
                        }
                    }
                }
            ]
        """
        if not key_path:
            key_path = []
        _name_dict = cls.get_name_dict()

        for _d in data:
            _temp_data = _d
            for _key_path in key_path:
                _temp_data = _temp_data[_key_path]
            biz_id = str(_temp_data.get(val_key))
            _temp_data[display_key] = _name_dict.get(biz_id, "")
        return data

    @property
    def bk_biz_name(self):
        if self.data:
            return self.data.get("bk_biz_name")

        return "Unkown Business"

    def _get_data(self):
        try:
            return MetaApi.bizs.retrieve({"bk_biz_id": self.bk_biz_id}).data
        except BaseAPIError as e:
            logger.warning("[META ERROR] retrieve biz fail, error={}".format(e))

        return None
