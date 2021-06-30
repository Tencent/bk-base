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

from .base import BaseTest


class TestDatabus(BaseTest):
    def cleans(self):
        data = {"status": "started"}
        return self.success_response(data)

    def tasks(self):
        data = {"component": "kafka", "module": "databus"}
        return self.success_response(data)

    def result_tables(self):
        return self.common_success()

    def get_result_table_data_tail(self):
        data = [
            {
                "product_id": "1106545419",
                "dtEventTimeStamp": 1540213421000,
                "istgpa": "true",
                "dtEventTime": "2018-10-22 21:03:41",
                "result": "0",
                "datestr": "20181022",
                "model": "CPH1823",
                "localTime": "2018-10-22 21:03:42",
                "version_name": "1.1.7_172",
                "manufacturer": "OPPO",
            }
        ]
        return self.success_response(data)

    def channels_inner_to_use(self):
        data = {
            "priority": 5,
            "description": "",
            "cluster_backup_ips": "",
            "cluster_domain": "127.0.0.1",
            "cluster_type": "kafka",
            "created_at": "2018-10-23T21:39:25",
            "cluster_port": 9092,
            "updated_at": "2018-10-23T21:39:25",
            "created_by": "",
            "cluster_role": "inner",
            "cluster_name": "testinner",
            "zk_domain": "127.0.0.1",
            "zk_port": 2181,
            "attribute": "bkdata",
            "active": True,
            "id": 10,
            "zk_root_path": "/kafka-test-3",
            "updated_by": "",
        }
        return self.success_response(data)

    def datanodes(self):
        data = {
            "heads": ["2_f1_batch_01"],
            "tails": ["2_f1_batch_01"],
            "result_table_ids": ["2_f1_batch_01"],
        }
        return self.success_response(data)
