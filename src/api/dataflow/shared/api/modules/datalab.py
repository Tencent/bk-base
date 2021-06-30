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

from common.api.base import DataDRFAPISet, DRFActionAPI
from common.api.modules.utils import add_app_info_before_request
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import BASE_DATALAB_URL


class _DatalabAPI(object):
    MODULE = "datalab"

    def __init__(self):
        self.query_set = DataDRFAPISet(
            url=BASE_DATALAB_URL + "notebooks/{notebook_id}/cells/{cell_id}/result_tables/",
            primary_key="result_table_id",
            url_keys=["notebook_id", "cell_id"],
            module=self.MODULE,
            description=_("模型相关接口"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={"truncate": DRFActionAPI(method="delete", detail=False, url_path="{result_table_id}")},
        )

        self.query_set_truncate = DataDRFAPISet(
            url=BASE_DATALAB_URL + "notebooks/{notebook_id}/cells/{cell_id}/result_tables/{result_table_id}/",
            primary_key="result_table_id",
            url_keys=["notebook_id", "cell_id", "result_table_id"],
            module=self.MODULE,
            description=_("模型相关接口"),
            before_request=add_app_info_before_request,
            after_request=None,
            custom_config={"truncate": DRFActionAPI(method="delete", detail=False)},
        )

        self.query_notebook_model = DataDRFAPISet(
            url=BASE_DATALAB_URL + "notebooks/{notebook_id}/outputs/",
            primary_key="notebook_id",
            url_keys=["notebook_id"],
            module=self.MODULE,
            description=_("获取笔记模型接口"),
            before_request=add_app_info_before_request,
            after_request=None,
        )

        self.query_notebook_model_detail = DataDRFAPISet(
            url=BASE_DATALAB_URL + "notebooks/{notebook_id}/outputs/",
            primary_key="output_name",
            url_keys=["notebook_id", "output_name"],
            module=self.MODULE,
            description=_("获取笔记模型详情"),
            before_request=add_app_info_before_request,
            after_request=None,
        )


DataLabAPI = _DatalabAPI()
