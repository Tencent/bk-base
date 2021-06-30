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

import json

from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo


class ProcessingBatchInfoHandler(object):
    @staticmethod
    def get_proc_batch_info_by_batch_id(job_id):
        return ProcessingBatchInfo.objects.get(batch_id=job_id)

    @staticmethod
    def get_proc_batch_info_lst_by_batch_id(batch_id):
        return ProcessingBatchInfo.objects.filter(batch_id=batch_id)

    @staticmethod
    def get_proc_batch_info_by_proc_id(processing_id):
        return ProcessingBatchInfo.objects.get(processing_id=processing_id)

    @staticmethod
    def is_proc_batch_info_exist_by_proc_id(processing_id):
        return ProcessingBatchInfo.objects.filter(processing_id=processing_id).exists()

    @staticmethod
    def save_proc_batch_info_v2(**kwargs):
        ProcessingBatchInfo.objects.create(**kwargs)

    @staticmethod
    def update_proc_batch_info_v2(processing_id, **kwargs):
        ProcessingBatchInfo.objects.filter(processing_id=processing_id).update(**kwargs)

    @staticmethod
    def save_proc_batch_info(args):
        ProcessingBatchInfo.objects.create(
            processing_id=args["processing_id"],
            batch_id=args["processing_id"],
            processor_type="sql",
            processor_logic=args["sql"],
            schedule_period=args["dict"]["schedule_period"],
            count_freq=args["dict"]["count_freq"],
            delay=args["dict"]["delay"],
            submit_args=json.dumps(args["dict"]),
            component_type="spark",
            created_by=args["bk_username"],
            updated_by=args["bk_username"],
            description=args["dict"]["description"],
        )
        return args["processing_id"]

    @staticmethod
    def update_proc_batch_info(args):
        prop = args["dict"]
        ProcessingBatchInfo.objects.filter(processing_id=args["processing_id"]).update(
            processing_id=args["processing_id"],
            batch_id=args["processing_id"],
            processor_type="sql",
            processor_logic=args["sql"],
            schedule_period=prop["schedule_period"],
            count_freq=prop["count_freq"],
            delay=prop["delay"],
            submit_args=json.dumps(prop),
            component_type="spark",
            created_by=args["bk_username"],
            updated_by=args["bk_username"],
            description=args["dict"]["description"],
        )
        return args["processing_id"]

    @staticmethod
    def update_proc_batch_info_submit_args(processing_id, submit_args):
        ProcessingBatchInfo.objects.filter(processing_id=processing_id).update(submit_args=json.dumps(submit_args))
        return processing_id

    @staticmethod
    def delete_proc_batch_info(processing_id):
        ProcessingBatchInfo.objects.filter(processing_id=processing_id).delete()
        return processing_id

    @staticmethod
    def get_proc_batch_info_by_prefix(processing_id_prefix):
        # todo 安全起见，暂时不按前缀查询，对目前使用场景下是没有影响的，因为目前不会产生Job的拆分
        # return
        # ProcessingBatchInfo.objects.filter(processing_id__startswith=processing_id_prefix).order_by('processing_id')
        return ProcessingBatchInfo.objects.filter(processing_id=processing_id_prefix).order_by("processing_id")

    @staticmethod
    def update_proc_batch_info_batch_id(processing_id, batch_id):
        ProcessingBatchInfo.objects.filter(processing_id=processing_id).update(batch_id=batch_id)

    @staticmethod
    def update_proc_batch_info_logic(processing_id, processor_logic):
        ProcessingBatchInfo.objects.filter(processing_id=processing_id).update(
            processor_logic=json.dumps(processor_logic)
        )
