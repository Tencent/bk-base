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

import mock as mock
from rest_framework.test import APITestCase

from dataflow.shared.meta.cluster_group_config.cluster_group_config_helper import ClusterGroupConfigHelper
from dataflow.shared.resourcecenter.resourcecenter_helper import ResourceCenterHelper
from dataflow.stream.api import stream_jobnavi_helper
from dataflow.stream.cluster_config.cluster_config_driver import apply_flink_yarn_session


class TestClusterConfigDriver(APITestCase):
    def setUp(self):
        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper("default", "cluster_id_xx")
        jobnavi_stream_helper.execute_schedule_meta_data = mock.Mock(return_value=None)
        jobnavi_stream_helper.get_yarn_session_execute_id = mock.Mock(return_value=None)
        jobnavi_stream_helper.get_yarn_application_status = mock.Mock(return_value=None)
        jobnavi_stream_helper.execute_yarn_schedule = mock.Mock(return_value=123)
        jobnavi_stream_helper.get_execute_status = mock.Mock(return_value={"status": "running"})
        stream_jobnavi_helper.StreamJobNaviHelper = mock.Mock(return_value=jobnavi_stream_helper)

    def test_apply_flink_yarn_session(self):
        ClusterGroupConfigHelper.is_default_cluster_group = mock.Mock(return_value=True)
        ResourceCenterHelper.create_or_update_cluster = mock.Mock(return_value=True)
        ResourceCenterHelper.get_cluster_info = mock.Mock(
            return_value=[{"cluster_name": "default", "cluster_id": "cluster_id_xx"}]
        )
        assert (
            apply_flink_yarn_session(
                params={
                    "extra_info": {},
                    "version": "1.7.2",
                    "cluster_group": "default",
                    "cluster_name": "standard_xxx",
                }
            )
            is None
        )
