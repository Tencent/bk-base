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

from common.exceptions import ApiRequestError

from dataflow.shared.api.modules.meta import MetaApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util

DATA_TRANSFERRING_NOT_FOUND_ERR = "1521091"


class DataTransferringHelper(object):
    @staticmethod
    def get_data_transferring(transferring_id, not_found_raise_exception=True):
        res = MetaApi.data_transferrings.retrieve({"transferring_id": transferring_id})
        try:
            res_util.check_response(res)
        except ApiRequestError as e:
            if not_found_raise_exception or e.code != DATA_TRANSFERRING_NOT_FOUND_ERR:
                raise e
            return None
        return res.data

    @staticmethod
    def delete_data_transferring(transferring_id):
        res = MetaApi.data_transferrings.delete({"transferring_id": transferring_id})
        try:
            res_util.check_response(res)
        except ApiRequestError as e:
            if e.code != DATA_TRANSFERRING_NOT_FOUND_ERR:
                raise e
            return False
        return True
