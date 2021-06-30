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
from bkbase.dataflow.batch.gateway import py_gateway
from bkbase.dataflow.batch.utils.batch_utils import (
    date_to_timestamp,
    generate_hdfs_path,
    time_round_to_hour,
    timestamp_to_date,
)


class TestBatchUtils(object):
    def test_batch_utils(self):
        py_gateway.set_log_level("INFO")
        time_format = timestamp_to_date(1584615926927, "/%Y/%m/%d/%H")
        assert time_format == "/2020/03/19/19"
        timestamp = date_to_timestamp(time_format, "/%Y/%m/%d/%H")
        assert timestamp == 1584615600000

        paths = generate_hdfs_path("/test", 1585253710310, 1585279710310)
        assert paths[0] == "/test/2020/03/27/04"
        assert paths[1] == "/test/2020/03/27/05"
        assert paths[2] == "/test/2020/03/27/06"
        assert paths[3] == "/test/2020/03/27/07"
        assert paths[4] == "/test/2020/03/27/08"
        assert paths[5] == "/test/2020/03/27/09"
        assert paths[6] == "/test/2020/03/27/10"

        hour_time = time_round_to_hour(1586262326907)
        assert hour_time == 1586260800000
        assert int(timestamp_to_date(hour_time, "%H")) == 20
