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

from dataflow.batch.exceptions.comp_execptions import BatchInfoNotFoundException, JobInfoNotFoundException
from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.shared.log import batch_logger


def update_batch_deploy_config(processing_id, property_key, property_value):
    value_need_change = {property_key: property_value}

    if ProcessingJobInfoHandler.is_proc_job_info(processing_id):
        job_info = ProcessingJobInfoHandler.get_proc_job_info(processing_id)
        deploy_config = {}
        if job_info.deploy_config is not None:
            try:
                deploy_config = json.loads(job_info.deploy_config)
            except ValueError as e:
                batch_logger.exception(e)
        __update_json(deploy_config, value_need_change)
        ProcessingJobInfoHandler.update_proc_job_info_deploy_config(processing_id, deploy_config)
    else:
        raise JobInfoNotFoundException("Can't find {} in job info db".format(processing_id))

    if ProcessingBatchJobHandler.is_proc_batch_job_exist(processing_id):
        batch_job = ProcessingBatchJobHandler.get_proc_batch_job(processing_id)
        deploy_config = {}
        if batch_job.deploy_config is not None:
            try:
                deploy_config = json.loads(batch_job.deploy_config)
            except ValueError as e:
                batch_logger.exception(e)
        __update_json(deploy_config, value_need_change)
        ProcessingBatchJobHandler.update_proc_batch_job_deploy_config(processing_id, deploy_config)


def update_batch_advance_config(processing_id, value_need_change):
    if ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
        batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
        batch_info_submit_args = json.loads(batch_info.submit_args)
    else:
        raise BatchInfoNotFoundException("Can't find data for {} in batch info DB".format(processing_id))

    __update_advanced_submit_args(batch_info_submit_args, value_need_change)

    ProcessingBatchInfoHandler.update_proc_batch_info_submit_args(processing_id, batch_info_submit_args)

    if ProcessingJobInfoHandler.is_proc_job_info(processing_id):
        job_info = ProcessingJobInfoHandler.get_proc_job_info(processing_id)
        job_config = json.loads(job_info.job_config)
        job_info_submit_args = json.loads(job_config["submit_args"])

        __update_advanced_submit_args(job_info_submit_args, value_need_change)

        job_config["submit_args"] = json.dumps(job_info_submit_args)
        ProcessingJobInfoHandler.update_proc_job_info_job_config(processing_id, job_config)

    if ProcessingBatchJobHandler.is_proc_batch_job_exist(processing_id):
        batch_job = ProcessingBatchJobHandler.get_proc_batch_job(processing_id)
        batch_job_submit_args = json.loads(batch_job.submit_args)

        __update_advanced_submit_args(batch_job_submit_args, value_need_change)

        ProcessingBatchJobHandler.update_proc_batch_job_submit_args(processing_id, batch_job_submit_args)


def __update_advanced_submit_args(submit_args, value_need_change):
    if "advanced" not in submit_args:
        submit_args["advanced"] = {}
    __update_json(submit_args["advanced"], value_need_change)


def __update_json(origin_json, value_need_change):
    """
    将老json中的参数更新成新参数。更新规则为：
    如果新参数key的value为None，删除原有参数的key
    如果新参数key的value不为None且不为dict类型，则将key对应的value更新为新的值
    如果新参数key的value为dict，且原有json不含有该key，则将key对应的value更新为新的dict
    如果新参数key的value为dict，且原有key对应的value不为dict，则将key对应的value更新为新的dict
    如果新参数key的value为dict，且原有key对应的value也是dict，则将key对应的value重复上述操作
    :param origin_json:
    :param value_need_change:
    :return:
    """
    for key in value_need_change:
        if value_need_change[key] is None and key in origin_json:
            del origin_json[key]
        elif value_need_change[key] is not None and not isinstance(value_need_change[key], dict):
            origin_json[key] = value_need_change[key]
        elif isinstance(value_need_change[key], dict) and key not in origin_json:
            origin_json[key] = value_need_change[key]
        elif isinstance(value_need_change[key], dict) and key in origin_json and not isinstance(origin_json[key], dict):
            origin_json[key] = value_need_change[key]
        elif isinstance(value_need_change[key], dict) and key in origin_json and isinstance(origin_json[key], dict):
            __update_json(origin_json[key], value_need_change[key])
