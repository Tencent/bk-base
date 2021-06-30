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
import os
from functools import wraps

from common.local import get_request_username
from django.forms.models import model_to_dict
from django.utils.decorators import available_attrs
from django.utils.translation import ugettext as _

from dataflow.component.utils.hdfs_util import ConfigHDFS
from dataflow.pizza_settings import COMPUTE_TAG
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.shared.stream.utils.resource_util import get_resource_group_id
from dataflow.stream.exceptions.comp_execptions import JobLockError, SubmitJobGeogAreaTagError
from dataflow.stream.handlers import processing_job_info, processing_stream_info, processing_stream_job
from dataflow.stream.job.job_driver import (
    get_redis_checkpoint_info,
    reset_checkpoint_on_migrate_kafka,
    reset_redis_checkpoint,
)
from dataflow.stream.settings import STREAM_JOB_CONFIG_DIR, ImplementType


def lock_stream_job(func):
    """
    为实时动态表加锁，防止外围调用多次提交
    """

    @wraps(func, assigned=available_attrs(func))
    def _wrap(self, *args, **kwargs):
        if self.is_debug:
            return func(self, *args, **kwargs)
        # 手动recover job的时不更新db任务状态，防止恢复失败而遗漏该任务
        if kwargs and "is_recover" in kwargs and kwargs["is_recover"]:
            return func(self, *args, **kwargs)
        try:
            job_status = "locked"
            if processing_stream_job.get(stream_id=self.real_job_id).status == job_status:
                # 当前正在提交实时任务
                raise JobLockError(_("当前正在提交实时任务，不可重复提交"))
            processing_stream_job.update(self.real_job_id, status="locked", updated_by=get_request_username())
            return func(self, *args, **kwargs)
        except Exception as e:
            job_status = "preparing"
            logger.exception(e)
            raise e
        finally:
            job_status = "submitted" if job_status == "locked" else job_status
            processing_stream_job.update(self.real_job_id, status=job_status, updated_by=get_request_username())

    return _wrap


class Job(object):
    def __init__(self, job_id, is_debug=False):
        self.job_id = job_id
        self.is_debug = is_debug
        self._job_info = None
        self._jobserver_config = None
        self._geog_area_code_of_cluster_group = None
        self._adaptor = None
        self._stream_conf = None

    @property
    def real_job_id(self):
        if self.is_debug:
            return self.job_id.split("__")[1]
        else:
            return self.job_id

    def lock(self):
        if processing_job_info.get(self.real_job_id).locked == 1:
            raise JobLockError()
        processing_job_info.update(self.real_job_id, locked=1)

    def get_job_info(self, related):
        """
        获取 porcessing_job_info 和 processing_stream_job 的信息

        :return:
        """
        # 返回stream集群可修改的运行配置
        if related and "streamconf" in related:
            return self.retrieve_stream_conf()
        # 返回全部配置
        stream_job = processing_stream_job.get(self.job_id, raise_exception=False)
        result = {
            "job_info": model_to_dict(self.job_info),
            "stream_job": model_to_dict(stream_job) if stream_job else {},
        }
        if related and "processings" in related:
            result["processings"] = []
            job_config = json.loads(self.job_info.job_config)
            if "processings" in job_config and job_config["processings"]:
                result["processings"] = list(
                    processing_stream_info.filter(processing_id__in=job_config["processings"]).values()
                )
        return result

    def get_code_version(self):
        """
        获取存储于 DB 的版本
        @return:
        """
        code_version = self.job_info.code_version
        return code_version

    def register(self, jar_name, geog_area_code, is_fill_snapshot=False):
        """
        更新作业动态表，返回待提交作业信息
        @param jar_name:
        @param geog_area_code:
        @param is_fill_snapshot: True register的目的是对当前运行中的任务获取并填充提交信息，无副作用，不更新db
        @return:
        """
        raise NotImplementedError

    @lock_stream_job
    def submit(self, conf, is_recover=False):
        # 正常手动提交任务，需要保存启动配置快照; 手动recover任务，不需要保存启动配置快照
        if not is_recover:
            snapshot_config = {
                "submit_config": conf,
                "uc_config": self.generate_job_config(),
            }
            processing_stream_job.update(self.job_id, snapshot_config=json.dumps(snapshot_config))
        # 非 debug 任务 且 非recover任务时候 提交前需要进行检查(如指定从头消费时候，重置redis checkpoint)
        if not self.is_debug and not is_recover:
            self.check_job_before_submit(conf)
        # 任务启动配置存下来
        config_path = os.path.join(
            STREAM_JOB_CONFIG_DIR,
            "submit-{}-{}.json".format(self.component_type, self.job_id),
        )
        with open(config_path, "w") as job_file:
            job_file.write(json.dumps(conf))
        return self.adaptor.submit(conf, is_recover)

    def sync_status(self, operate_info):
        return self.adaptor.sync_status(operate_info)

    def unlock(self, entire):
        processing_job_info.update(self.real_job_id, locked=0)
        if entire:
            processing_stream_job.update(self.real_job_id, status="preparing")

    def cancel(self):
        return self.adaptor.cancel()

    def force_kill(self, timeout=180):
        return self.adaptor.force_kill(timeout)

    def check_job_before_submit(self, conf):
        """
        对于非 debug 任务，提交前的检查，目前如重置 checkpoint, savepoint等
        @return:
        """
        if self.component_type in ["storm", "flink"] and self.implement_type not in [ImplementType.CODE.value]:
            if self.offset == -1:
                reset_redis_checkpoint(
                    self.job_id,
                    self.component_type,
                    self.heads,
                    self.tails,
                    self.geog_area_code,
                )
        self.adaptor.check_job_before_submit(conf)

    @property
    def component_type(self):
        return self.job_info.component_type

    @property
    def component_type_without_version(self):
        return self.job_info.component_type.split("-")[0]

    @property
    def implement_type(self):
        return self.job_info.implement_type

    @property
    def programming_language(self):
        return self.job_info.programming_language

    def update(self, args):
        args["job_id"] = self.job_id
        args["updated_by"] = get_request_username()
        args["job_config"]["concurrency"] = self.job_config.get("concurrency")
        args["jobserver_config"] = json.dumps(args["jobserver_config"])
        args["deploy_mode"] = self.deploy_mode
        # 参数中包含deploy_config取参数中的，否则从配置表中取
        if "deploy_config" in args and args["deploy_config"]:
            args["deploy_config"] = json.dumps(args["deploy_config"])
        else:
            args["deploy_config"] = json.dumps(self.deploy_config)
        # TODO: 之后 flow 可能支持官网传 code_version
        args["code_version"] = self.job_info.code_version
        # flow传递过来的cluster_group通过查询资源系统立即转为确切的resource_group_id并存储供后续流程使用
        # 扩partition重启操作时不传集群组则使用上次已经转换过的资源组
        jobserver_config = json.loads(args["jobserver_config"])
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_group = args["cluster_group"]
        if cluster_group:
            resource_group_id = get_resource_group_id(
                geog_area_code, cluster_group, self.component_type_without_version
            )
        else:
            resource_group_id = self.cluster_group
        # 获取implement type
        if len(args["processings"]) == 0:
            raise Exception("The processings parameter cannot be empty.")

        args["job_config"]["processings"] = args["processings"]

        processing_job_info.update(
            args["job_id"],
            code_version=args["code_version"],
            cluster_group=resource_group_id,
            job_config=json.dumps(args["job_config"]),
            deploy_mode=args["deploy_mode"],
            deploy_config=args["deploy_config"],
            jobserver_config=args["jobserver_config"],
            updated_by=args["updated_by"],
        )
        # update processing_stream_info
        for processing in args["processings"]:
            processing_stream_info.update(processing, stream_id=args["job_id"])
        self._job_info = None

    def update_job_conf(self, params):
        """
        更新jobconfig，对应ProcessingJobInfo.job_config中的信息
        :param params: <key,value>形式的参数，value为None，会自动转化为null字符串
        :return:
        """
        job_info_item = self.job_info
        job_config_item = self.job_config
        for key, value in list(params.items()):
            job_config_item[key] = value
        # from dataflow.shared.log import flow_logger as logger
        # logger.info("job_id(%s), job_config(%s)" % (job_info_item.job_id, json.dumps(job_config_item)))
        processing_job_info.update(job_info_item.job_id, job_config=json.dumps(job_config_item))

    def _update_deploy_conf(self, deploy_mode, deploy_config):
        """
        更新deploy_conf，对应ProcessingJobInfo.deploy_config中的信息,也包括deploy_mode
        """
        processing_job_info.update(self.real_job_id, deploy_mode=deploy_mode, deploy_config=deploy_config)

    @property
    def job_info(self):
        if not self._job_info:
            self._job_info = processing_job_info.get(self.real_job_id)
        return self._job_info

    @property
    def geog_area_code(self):
        geog_area_code = self.jobserver_config.get("geog_area_code")
        if not geog_area_code:
            raise SubmitJobGeogAreaTagError()
        return geog_area_code

    @property
    def cluster_id(self):
        return self.jobserver_config.get("cluster_id")

    @property
    def jobserver_config(self):
        return json.loads(self.job_info.jobserver_config)

    @property
    def geog_area_code_of_cluster_group(self):
        """
        集群组对应的地区
        @return:
        """
        if not self._geog_area_code_of_cluster_group:
            self._geog_area_code_of_cluster_group = TagHelper.get_geog_area_code_by_cluster_group(self.cluster_group)
        return self._geog_area_code_of_cluster_group

    @property
    def job_config(self):
        return json.loads(self.job_info.job_config)

    @property
    def deploy_mode(self):
        return self.job_info.deploy_mode

    @property
    def cluster_name(self):
        return self.job_info.cluster_name

    @property
    def cluster_group(self):
        return self.job_info.cluster_group

    @property
    def deploy_config(self):
        deploy_config = self.job_info.deploy_config
        if not deploy_config:
            return {}
        return json.loads(deploy_config)

    @property
    def heads(self):
        return self.job_config.get("heads").split(",")

    @property
    def tails(self):
        return self.job_config.get("tails").split(",")

    @property
    def concurrency(self):
        return self.deploy_config.get("resource", {}).get("concurrency")

    @property
    def slots(self):
        return self.deploy_config.get("resource", {}).get("slots")

    @property
    def async_io_concurrency(self):
        # ignite静态关联AsyncIO并发
        return self.deploy_config.get("resource", {}).get("async_io_concurrency")

    @property
    def chain(self):
        # disable关闭chain优化（默认开启）
        return self.deploy_config.get("resource", {}).get("chain")

    @property
    def offset(self):
        return self.job_config.get("offset")

    @property
    def adaptor(self, operate_info):
        """
        绑定于 job 的适配器，用于提交到实际计算引擎及生成动态作业信息
        @return:
        """
        raise NotImplementedError

    def ftp_server(self, tag=COMPUTE_TAG):
        hdfs_cluster_group = StorekitHelper.get_default_storage("hdfs", self.geog_area_code, tag=tag)["cluster_group"]
        return ConfigHDFS(hdfs_cluster_group)

    def generate_job_config(self):
        """
        获取 UC 提交 yarn 任务参数
        @return: {json}
        """
        raise NotImplementedError

    def get_checkpoint(self, related="flink", extra=""):
        result = {}
        stream_job = processing_stream_job.get(self.job_id)
        checkpoint = get_redis_checkpoint_info(
            job_id=self.job_id,
            component_type=related,
            heads=stream_job.heads,
            tails=stream_job.tails,
            extra=extra,
            geog_area_code="inland",
        )
        result["checkpoint"] = checkpoint
        result["type"] = related
        return result

    def reset_checkpoint_on_migrate_kafka(self, head_rt):
        reset_checkpoint_on_migrate_kafka(self.job_id, head_rt)

    def transform_checkpoint(self):
        raise NotImplementedError

    def transform_config(self):
        raise NotImplementedError

    def revert_storm(self):
        processing_job_info.update(job_id=self.job_id, component_type="storm", code_version=None)
        processing_stream_job.delete(stream_id=self.job_id)
        processings = processing_stream_info.filter(stream_id=self.job_id)
        for processing in processings:
            processing_stream_info.update(processing_id=processing.processing_id, component_type="storm")
        for processing in processings:
            vnode_query_params = processing.processing_id + r"_FlinkSql"
            vnode_processings = processing_stream_info.filter(processing_id__startswith=vnode_query_params)
            for vprocessing in vnode_processings:
                processing_stream_info.delete(processing_id=vprocessing.processing_id)

    def update_stream_conf(self, params):
        """
        更新stream相关运行配置信息
        :return:
        """
        raise NotImplementedError

    def retrieve_stream_conf(self):
        """
        获取stream相关运行配置信息
        :return:
        """
        raise NotImplementedError
