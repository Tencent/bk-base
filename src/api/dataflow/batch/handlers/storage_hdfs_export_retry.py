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

from dataflow.batch.models.bkdata_flow import StorageHdfsExportRetry as Table


class StorageHdfsExportRetryHander(object):
    @staticmethod
    def save_retry_info(result_table_id, schedule_time, data_dir, batch_storage_type):
        Table.objects.create(
            **{
                "result_table_id": result_table_id,
                "schedule_time": schedule_time,
                "data_dir": data_dir,
                "batch_storage_type": batch_storage_type,
            }
        )

    @staticmethod
    def list_by_schedule_time_gt(schedule_time):
        return Table.objects.filter(schedule_time__gt=schedule_time)

    @staticmethod
    def update_retry_info(result_table_id, schedule_time, data_dir, batch_storage_type, retry_times):
        Table.objects.filter(
            result_table_id=result_table_id,
            schedule_time=schedule_time,
            data_dir=data_dir,
            batch_storage_type=batch_storage_type,
        ).update(retry_times=retry_times)

    @staticmethod
    def delete_retry_info(result_table_id, schedule_time, data_dir, batch_storage_type):
        Table.objects.filter(
            result_table_id=result_table_id,
            schedule_time=schedule_time,
            data_dir=data_dir,
            batch_storage_type=batch_storage_type,
        ).delete()

    @staticmethod
    def is_retry_record_exists(result_table_id, schedule_time, data_dir, batch_storage_type):
        return Table.objects.filter(
            result_table_id=result_table_id,
            schedule_time=schedule_time,
            data_dir=data_dir,
            batch_storage_type=batch_storage_type,
        ).exists()
