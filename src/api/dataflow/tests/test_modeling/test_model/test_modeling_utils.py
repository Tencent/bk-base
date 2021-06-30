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

import pytest
from rest_framework.test import APITestCase

from dataflow.modeling.utils.modeling_utils import ModelingUtils


class TestModelingUtilsProcessing(APITestCase):
    @pytest.mark.usefixtures("mock_get_proc_batch_info_by_proc_id")
    def test_get_need_clear_label(self):
        delete_model, delete_table, delete_dp, delete_processing = ModelingUtils.get_need_clear_label("1")
        assert not delete_model and delete_table and delete_dp and delete_processing
        delete_model, delete_table, delete_dp, delete_processing = ModelingUtils.get_need_clear_label("2")
        assert delete_model and not delete_table and delete_dp and delete_processing
        delete_model, delete_table, delete_dp, delete_processing = ModelingUtils.get_need_clear_label("3")
        assert not delete_model and not delete_table and not delete_dp and not delete_processing
        delete_model, delete_table, delete_dp, delete_processing = ModelingUtils.get_need_clear_label("4")
        assert not delete_model and delete_table and delete_dp and delete_processing
