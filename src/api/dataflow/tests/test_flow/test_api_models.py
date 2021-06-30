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

import mock
from common.exceptions import ApiResultError
from rest_framework.test import APITestCase

from dataflow.flow.api_models import ResultTable
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class TestResultTable(APITestCase):
    def test_get_storage_id(self):
        cluster_type = "hdfs"
        storage_type = "channel"
        rtTable = ResultTable("591_zc_test")
        rtTable.get_storage_msg = mock.Mock(return_value={"storage_channel": {"channel_cluster_config_id": 1}})
        rtTable.get_storage_id(cluster_type, storage_type)
        storage_type = "storage"
        rtTable.get_storage_msg = mock.Mock(return_value={"storage_cluster": {"storage_cluster_config_id": 1}})
        rtTable.get_storage_id(cluster_type, storage_type)

    def test_cluster_name(self):
        cluster_type = "hdfs"
        rtTable = ResultTable("591_zc_test")
        StorekitHelper.get_physical_table = mock.Mock(return_value=[])
        with self.assertRaises(ApiResultError):
            rtTable.cluster_name(cluster_type)
