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

from django.core.exceptions import ObjectDoesNotExist

import dataflow.batch.settings as settings
from dataflow.batch.models.bkdata_flow import ProcessingJobInfo
from dataflow.shared.log import batch_logger


class ProcessingJobInfoHandler(object):
    @staticmethod
    def create_proc_job_info(args, processing_type="batch", implement_type="sql"):
        def _choose_value(key):
            return args.get(key, getattr(settings, "%s_DEFAULT" % key.upper()))

        job_id = args["job_id"]
        code_version = _choose_value("code_version")
        if code_version is None:
            code_version = getattr(settings, "CODE_VERSION_DEFAULT")

        ProcessingJobInfo.objects.create(
            job_id=job_id,
            processing_type=processing_type,
            code_version=code_version,
            component_type=_choose_value("component_type"),
            job_config=args["job_config"],
            jobserver_config=json.dumps(args["jobserver_config"]),
            cluster_group=_choose_value("cluster_group"),
            cluster_name=_choose_value("cluster_name"),
            deploy_mode=_choose_value("deploy_mode"),
            deploy_config=_choose_value("deploy_config"),
            created_by=args["bk_username"],
            updated_by=args["bk_username"],
            implement_type=implement_type,
        )
        return job_id

    @staticmethod
    def update_proc_job_info(args, processing_type="batch"):
        def _choose_value(key):
            return args.get(key, getattr(settings, "%s_DEFAULT" % key.upper()))

        job_id = args["job_id"]
        code_version = _choose_value("code_version")
        if code_version is None:
            code_version = getattr(settings, "CODE_VERSION_DEFAULT")

        if ProcessingJobInfoHandler.is_proc_job_info(job_id):
            # 为了支持刷新spark配置项和node_label重启flow继续有效，更新JobInfo时合并更新deploy_config
            old_job_info = ProcessingJobInfoHandler.get_proc_job_info(job_id)
            deploy_config = {}
            try:
                if old_job_info.deploy_config is not None:
                    deploy_config = json.loads(old_job_info.deploy_config)
            except ValueError as e:
                batch_logger.info("Can't parse old deploy config for {}".format(job_id))
                batch_logger.exception(e)

            new_deploy_config = {}
            try:
                new_deploy_config = json.loads(_choose_value("deploy_mode"))
            except ValueError as e:
                batch_logger.info("Can't parse new deploy config for {}".format(job_id))
                batch_logger.exception(e)

            for key in new_deploy_config:
                deploy_config[key] = new_deploy_config[key]

            ProcessingJobInfo.objects.filter(job_id=job_id).update(
                processing_type=processing_type,
                code_version=code_version,
                component_type=_choose_value("component_type"),
                job_config=args["job_config"],
                jobserver_config=json.dumps(args["jobserver_config"]),
                cluster_group=_choose_value("cluster_group"),
                cluster_name=_choose_value("cluster_name"),
                deploy_mode=_choose_value("deploy_mode"),
                deploy_config=json.dumps(deploy_config),  # 原为_choose_value('deploy_config')
                updated_by=args["bk_username"],
            )

    @staticmethod
    def create_processing_job_info_v2(**kwargs):
        ProcessingJobInfo.objects.create(**kwargs)

    @staticmethod
    def update_processing_job_info_v2(processing_id, **kwargs):
        ProcessingJobInfo.objects.filter(job_id=processing_id).update(**kwargs)

    @staticmethod
    def update_proc_job_info_job_config(job_id, job_config):
        ProcessingJobInfo.objects.filter(job_id=job_id).update(job_config=json.dumps(job_config))
        return job_id

    @staticmethod
    def update_proc_job_info_deploy_config(job_id, deploy_config):
        ProcessingJobInfo.objects.filter(job_id=job_id).update(deploy_config=json.dumps(deploy_config))
        return job_id

    @staticmethod
    def get_proc_job_info(job_id):
        try:
            return ProcessingJobInfo.objects.get(job_id=job_id)
        except ObjectDoesNotExist as e:
            batch_logger.exception(e)
            return None

    @staticmethod
    def is_proc_job_info(job_id):
        obj = ProcessingJobInfo.objects.filter(job_id=job_id)
        return obj.exists()

    @staticmethod
    def delete_proc_job_info(job_id):
        if ProcessingJobInfoHandler.is_proc_job_info(job_id):
            ProcessingJobInfo.objects.filter(job_id=job_id).delete()

    @staticmethod
    def get_proc_job_info_by_prefix(job_id_prefix):
        # todo 安全起见，暂时不按前缀查询，对目前使用场景下是没有影响的，因为目前不会产生Job的拆分
        # return ProcessingJobInfo.objects.filter(job_id__startswith=job_id_prefix).order_by('-job_id')
        return ProcessingJobInfo.objects.filter(job_id=job_id_prefix).order_by("-job_id")
