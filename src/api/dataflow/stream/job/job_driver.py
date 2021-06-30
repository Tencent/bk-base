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
import uuid

from common.local import get_request_username
from conf.dataapi_settings import SYSTEM_ADMINISTRATORS
from django.db import transaction
from django.db.models import Max
from django.utils.translation import ugettext as _

from dataflow.shared.api.modules.meta import MetaApi
from dataflow.shared.handlers import processing_version_config
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.send_message import send_message
from dataflow.stream.exceptions.comp_execptions import (
    ComponentNotSupportError,
    JobLockError,
    JobNotExistsError,
    ProcessingParameterNotExistsError,
    UpstreamError,
)
from dataflow.stream.handlers import db_helper, processing_job_info, processing_stream_info, processing_stream_job
from dataflow.stream.result_table.result_table_driver import load_chain
from dataflow.stream.result_table.result_table_for_flink import load_flink_chain
from dataflow.stream.settings import (
    FLINK_CHECKPOINT_PREFIX,
    RELEASE_ENV,
    RELEASE_ENV_TGDP,
    RUN_VERSION,
    STREAM_WORK_DIR,
    DeployMode,
)
from dataflow.stream.utils.checkpoint_manager import CheckpointManager


@transaction.atomic()
def create_job(args):
    job_id = "{}_{}".format(args["project_id"], str(uuid.uuid4()).replace("-", ""))
    args["job_id"] = job_id
    args["created_by"] = get_request_username()
    args["processing_type"] = "stream"
    args["job_config"] = json.dumps(args["job_config"])
    args["jobserver_config"] = json.dumps(args["jobserver_config"])

    # 获取implement type
    if len(args["processings"]) == 0:
        raise ProcessingParameterNotExistsError()
    # TODO: 暂时用 processor_type 替代 implement_type
    implement_type = processing_stream_info.get(args["processings"][0]).processor_type
    if implement_type != "code":
        implement_type = "sql"
        programming_language = "java"
    else:
        programming_language = json.loads(processing_stream_info.get(args["processings"][0]).processor_logic)[
            "programming_language"
        ]

    if implement_type == "code" and args["component_type"] not in ["flink"]:
        deploy_mode = DeployMode.YARN_CLUSTER.value
    elif RUN_VERSION == RELEASE_ENV or RUN_VERSION == RELEASE_ENV_TGDP:
        deploy_mode = DeployMode.YARN_CLUSTER.value
    else:
        deploy_mode = None

    # save processing_stream_job
    processing_job_info.save(
        job_id=args["job_id"],
        processing_type=args["processing_type"],
        code_version=args["code_version"],
        component_type=args["component_type"],
        cluster_group=args["cluster_group"],
        job_config=args["job_config"],
        deploy_mode=deploy_mode,
        deploy_config=args["deploy_config"],
        jobserver_config=args["jobserver_config"],
        implement_type=implement_type,
        programming_language=programming_language,
        created_by=args["created_by"],
    )
    # save processing_stream_info
    for processing in args["processings"]:
        processing_stream_info.update(processing, stream_id=job_id)
    return job_id


@transaction.atomic()
def update_job(args, job_id):
    args["job_id"] = job_id
    args["updated_by"] = get_request_username()
    # get job conf
    concurrency, deploy_mode, deploy_config, code_version = get_job_conf(job_id)
    args["job_config"]["concurrency"] = concurrency
    args["job_config"] = json.dumps(args["job_config"])
    args["jobserver_config"] = json.dumps(args["jobserver_config"])
    args["deploy_mode"] = deploy_mode
    args["deploy_config"] = deploy_config
    args["code_version"] = code_version

    # 获取implement type
    if len(args["processings"]) == 0:
        raise ProcessingParameterNotExistsError()

    processing_job_info.update(
        args["job_id"],
        code_version=args["code_version"],
        cluster_group=args["cluster_group"],
        job_config=args["job_config"],
        deploy_mode=args["deploy_mode"],
        deploy_config=args["deploy_config"],
        jobserver_config=args["jobserver_config"],
        updated_by=args["updated_by"],
    )
    # update processing_stream_info
    for processing in args["processings"]:
        processing_stream_info.update(processing, stream_id=args["job_id"])
    return job_id


def lock_job(job_id):
    if processing_job_info.get(job_id).locked == 1:
        raise JobLockError()
    processing_job_info.update(job_id, locked=1)


def unlock_job(job_id):
    processing_job_info.update(job_id, locked=0)


def get_version(code_version, component_type):
    """
    获取任务的代码版本

    :param code_version:  可带版本号，也可以只是版本
    :param componet_type:  flink
    :return:
    """
    rows = list(processing_version_config.where(component_type=component_type).values())
    for row in rows:
        tag = row["branch"] + "-" + row["version"]
        if tag == code_version:
            return code_version + ".jar"
    version = processing_version_config.where(component_type=component_type, branch=code_version).aggregate(
        Max("version")
    )["version__max"]
    return code_version + "-" + version + ".jar"


def register_job(args, job_id):
    """
    注册作业信息

    :param params:
    :return:
    """
    jar_name = args.get("jar_name")
    geog_area_code = args.get("geog_area_code")
    job_info = processing_job_info.get(job_id)

    if "code" == job_info.implement_type:
        from dataflow.stream.job.code_job import CodeJob

        return CodeJob(job_id).register(jar_name, geog_area_code)
    if "flink" == job_info.component_type:
        from dataflow.stream.job.flink_job import FlinkJob

        return FlinkJob(job_id).register(jar_name, geog_area_code)
        # return register_flink_job(jar_name, job_info, job_id, geog_area_code)
    elif "storm" == job_info.component_type:
        from dataflow.stream.extend.job.job_for_storm import register_storm_job

        return register_storm_job(jar_name, job_info, job_id, geog_area_code)
    else:
        raise ComponentNotSupportError("the component %s is not supported." % job_info.component_type)


def sync_job_status(args, job_id):
    is_debug = args.get("is_debug", False)
    if is_debug:
        job_info = processing_job_info.get(job_id.split("__")[1])
    else:
        job_info = processing_job_info.get(job_id)

    if job_info.implement_type == "code":
        from dataflow.stream.job.code_job import CodeJob

        return CodeJob(job_id, is_debug).sync_status(json.loads(args.get("operate_info", "{}")))
    if "flink" == job_info.component_type:
        from dataflow.stream.job.flink_job import FlinkJob

        return FlinkJob(job_id, is_debug).sync_status(json.loads(args.get("operate_info", "{}")))
        # jobserver_config = json.loads(job_info.jobserver_config)
        # geog_area_code = jobserver_config['geog_area_code']
        # cluster_id = jobserver_config['cluster_id']
        # return sync_cluster_and_session_job_status(args, geog_area_code, cluster_id, job_id)
    elif "storm" == job_info.component_type:
        from dataflow.stream.extend.job.job_for_storm import sync_storm_job_status

        return sync_storm_job_status(job_id)
    else:
        raise ComponentNotSupportError("the component %s is not supported." % job_info.component_type)


def submit_job(args, job_id):
    is_debug = args.get("is_debug", False)
    if is_debug:
        job_info = processing_job_info.get(job_id.split("__")[1])
    else:
        job_info = processing_job_info.get(job_id)

    if job_info.implement_type == "code":
        from dataflow.stream.job.code_job import CodeJob

        return CodeJob(job_id, is_debug).submit(args["conf"])
    elif "flink" == job_info.component_type:
        from dataflow.stream.job.flink_job import FlinkJob

        return FlinkJob(job_id, is_debug).submit(args["conf"])
        # jobserver_config = json.loads(job_info.jobserver_config)
        # geog_area_code = jobserver_config['geog_area_code']
        # cluster_id = jobserver_config['cluster_id']
        # return submit_flink_job(args, geog_area_code, cluster_id, job_id)
    elif "storm" == job_info.component_type:
        from dataflow.stream.extend.job.job_for_storm import submit_storm_job

        return submit_storm_job(args, job_id)
    else:
        raise ComponentNotSupportError(
            "the component %s and the implement %s is not supported."
            % (job_info.component_type, job_info.implement_type)
        )


def load_stream_config():
    """
    key-value形式
    @return:
    """
    stream_config_file_path = os.path.join(STREAM_WORK_DIR, "conf", "stream.config")
    stream_config = {}
    try:
        with open(stream_config_file_path) as f:
            for line in f:
                key, value = line.partition("=")[::2]
                stream_config[key.strip()] = value.strip()
    except Exception as e:
        logger.exception("Error while reading file: {}, detail: {}".format(stream_config_file_path, e))
        raise e
    return stream_config


def _get_unique_source_rt(result_tables_chain, heads, result_table_id):
    """
    根据 chain 信息获取 result_table_id 唯一的数据源 RT
    @param result_tables_chain:
    @param heads:
    @param result_table_id:
    @return:
    """
    origin_result_table_id = result_table_id
    # 环校验
    result_table_ids = []
    while result_table_id not in heads:
        result_table = result_tables_chain.get(result_table_id, {})
        parents = result_table.get("parents", [])
        if len(parents) != 1:
            if len(parents) == 0:
                raise UpstreamError(_("结果表 %s 不存在上游结果表" % result_table_id))
            content = "结果表 %s 存在多个上游结果表" % result_table_id
            logger.exception(content)
            send_message(SYSTEM_ADMINISTRATORS, "《实时结果表信息错误》", content, raise_exception=False)
            # 仍然允许启动
            return parents[0]
        if result_table_id in result_table_ids:
            raise UpstreamError(_("当前实时链路存在结果表(%s)的环路，请修改后重试") % result_table_id)
        result_table_ids.append(result_table_id)
        result_table_id = parents[0]
    logger.info("结果表 {} 的源 RT 为 {}".format(origin_result_table_id, result_table_id))
    return result_table_id


def reset_redis_checkpoint(job_id, component_type, heads, tails, geog_area_code):
    """
    重置 checkpoint
    @param job_id:
    @param component_type:
    @param heads:
    @return:
    """
    logger.info("start to reset redis checkpoint if necessary.")
    processings = processing_stream_info.filter(stream_id=job_id)
    # 删除 redis 的 checkpoint 信息
    if component_type == "storm":
        # redis配置项
        # dataflow.stream.redis.host=xxx
        # dataflow.stream.redis.port=xxx
        # dataflow.stream.redis.passwd=xxx
        stream_config = load_stream_config()
        redis_host = stream_config["dataflow.stream.redis.host"]
        redis_port = stream_config["dataflow.stream.redis.port"]
        redis_passwd = stream_config["dataflow.stream.redis.passwd"]
        redis_client = db_helper.RedisConnection(redis_host, redis_port, redis_passwd)
        checkpoint_names = [_p.processing_id for _p in processings]
    else:
        checkpoint_names = []
        result_tables_chain = {}
        if len(heads) > 1 and [processing for processing in processings if processing.checkpoint_type == "offset"]:
            # 若多源链路存在 offset 类型节点，需要获取所在链路的源 RT
            heads_str, tails_str = ",".join(heads), ",".join(tails)
            result_tables_chain = load_flink_chain(heads_str, tails_str)
        for processing in processings:
            if processing.checkpoint_type == "offset":
                # uc中是通过数据源RT找子节点，为各个子节点指定其RT作为topic的组成部分
                # 由于数据源超过两个的情况必然为有窗的情形，其下游节点就算为无窗，checkpoint_type 也会被置为 timestamp
                # 因此 checkpoint_type 为 offset 时必然为无窗的情况，且所在链路的数据源不会超过1个（可能有多个链路）
                if len(heads) <= 1:
                    topic = "table_%s" % heads[0]
                else:
                    # 若有多个链路，获取实际的源
                    source_rt = _get_unique_source_rt(result_tables_chain, heads, processing.processing_id)
                    topic = "table_%s" % source_rt
                checkpoint_names.append(FLINK_CHECKPOINT_PREFIX + topic + "|" + processing.processing_id)
            elif processing.checkpoint_type == "timestamp":
                checkpoint_names.append(FLINK_CHECKPOINT_PREFIX + processing.processing_id)
        redis_client = _get_redis_client(geog_area_code)
    if len(checkpoint_names) > 0:
        logger.info("start to delete {} checkpoint of ({})".format(component_type, ",".join(checkpoint_names)))
        redis_client.delete(*checkpoint_names)
    else:
        logger.exception("Error! checkpoint_names is empty for job(%s)" % job_id)


def get_redis_checkpoint_info(job_id, component_type, heads, tails, extra, geog_area_code="inland"):
    """
    获取该job在redis中的所有checkpoint记录的信息
    @param job_id:
    @param component_type:
    @param heads:
    @return:
    """
    processings = processing_stream_info.filter(stream_id=job_id)
    redis_client = _get_redis_client(geog_area_code)
    result = {}
    if component_type == "storm":
        for processing in processings:
            processing_id = processing.processing_id
            result[processing_id] = {}
            result[processing_id]["checkpoint_type"] = processing.checkpoint_type
            result[processing_id]["checkpoint_value"] = redis_client.hgetall(processing_id)
    elif component_type == "flink":
        for processing in processings:
            if processing.checkpoint_type == "offset":
                checkpoint_name = _build_flink_offset_checkpoint_key(
                    heads=heads, tails=tails, processing_id=processing.processing_id
                )
                result[checkpoint_name] = {}
                result[checkpoint_name]["checkpoint_type"] = "offset"
                result[checkpoint_name]["checkpoint_value"] = redis_client.hgetall(checkpoint_name)
            elif processing.checkpoint_type == "timestamp":
                checkpoint_name = FLINK_CHECKPOINT_PREFIX + processing.processing_id
                result[checkpoint_name] = {}
                result[checkpoint_name]["checkpoint_type"] = "timestamp"
                result[checkpoint_name]["checkpoint_value"] = redis_client.hgetall(checkpoint_name)
    elif component_type == "redis":
        result[extra] = redis_client.hgetall(extra)
    return result


def transform_checkpoint_scheme(job_id, heads, tails, geog_area_code="inland"):
    """
    将该job在redis中的所有checkpoint记录的信息从storm格式转换为flink格式
    type   		  key           							 field       value
    storm-offset  resultTableId 							 taskIndex   checkpointvalue
    storm-timest  resultTableId 							 taskIndex   checkpointvalue
    flink-offset  "bkdata|flink|" + topic + "|" + nodeId     partition   checkpointvalue
    flink-timest  "bkdata|flink|" + nodeId  				 "timestamp" checkpointvalue

    @param job_id:
    @param component_type:
    @param heads:
    @return:
    """
    processings = processing_stream_info.filter(stream_id=job_id)
    redis_client = _get_redis_client(geog_area_code)
    for processing in processings:
        checkpoint_type = processing.checkpoint_type
        chains = load_chain(heads, tails)
        # 只有节点存在storages时候才有必要转换checkpoint
        if (
            processing.processing_id in chains
            and "storages" in chains[processing.processing_id]
            and chains[processing.processing_id]["storages"]
        ):
            checkpoint_value = redis_client.hgetall(processing.processing_id)
            # 没有存储checkpoint的时候转换异常
            if not checkpoint_value:
                raise Exception(processing.processing_id + r":redis not store any checkpoint data.")
        else:
            continue
        if checkpoint_type == "offset":
            checkpoint_name = _build_flink_offset_checkpoint_key(
                heads=heads, tails=tails, processing_id=processing.processing_id
            )
            redis_client.hmset(checkpoint_name, checkpoint_value)
        elif processing.checkpoint_type == "timestamp":
            checkpoint_name = FLINK_CHECKPOINT_PREFIX + processing.processing_id
            for key, value in list(checkpoint_value.items()):
                if value:
                    redis_client.hset(checkpoint_name, "timestamp", value)


# 构造出flink checkpoint为offset时候的redis的key
def _build_flink_offset_checkpoint_key(heads, tails, processing_id):
    head_ids = heads.split(",")
    topic = ""
    if len(head_ids) > 1:
        # flow存在两个实时数据源的情况
        topic = _get_topic_by_recursive_chains(chains=load_chain(heads, tails), result_table_id=processing_id)
    else:
        # 单数据源时heads即为最上游节点
        topic = _get_topic_by_recursive_chains(chains={heads: {}}, result_table_id=heads)
    checkpoint_name = FLINK_CHECKPOINT_PREFIX + topic + "|" + processing_id
    return checkpoint_name


# 递归遍历出节点的最上游的节点的topic
def _get_topic_by_recursive_chains(chains, result_table_id):
    # 非最上游节点
    if "parents" in chains[result_table_id] and chains[result_table_id]["parents"]:
        return _get_topic_by_recursive_chains(chains, chains[result_table_id]["parents"][0])
    # 是storm的最上游节点，注册的meta存在input_source
    elif "input_source" in chains[result_table_id]:
        return chains[result_table_id]["input_source"]
    # 是flink的最上游节点
    # 已经转化为flink后flink的meta不保存input_source,通过id来访问MetaApi.result_tables获取真实topic
    else:
        storages = MetaApi.result_tables.storages({"result_table_id": result_table_id}).data
        if not storages or not storages["kafka"] or not storages["kafka"]["physical_table_name"]:
            raise Exception(result_table_id + r":meta api not exist kafka physical_table_name")
        topic = storages["kafka"]["physical_table_name"]
        return topic


def delete_flink_checkpoint_redis_info(processing_id, checkpoint_type, heads, tails, geog_area_code):
    """
    根据processing_id删除redis中存储的flink的checkpoint信息
    @param processing_id:
    @param checkpoint_type:
    @param heads:
    @return:
    """
    redis_client = _get_redis_client(geog_area_code)
    if checkpoint_type == "offset":
        checkpoint_name = _build_flink_offset_checkpoint_key(heads=heads, tails=tails, processing_id=processing_id)
    elif checkpoint_type == "timestamp":
        checkpoint_name = FLINK_CHECKPOINT_PREFIX + processing_id
    redis_client.delete(checkpoint_name)
    logger.info("delete flink checkpoint redis info of proccessing (%s)" % processing_id)


# 获取redis client
def _get_redis_client(geog_area_code):
    checkpoint_manager = CheckpointManager(geog_area_code)
    # sentinel 模式
    if checkpoint_manager.enable_sentinel:
        # 部分版本可能有 sentinel 部署/切换的需求
        redis_client = db_helper.RedisSentinelConnection(
            checkpoint_manager.host_sentinel,
            checkpoint_manager.port_sentinel,
            checkpoint_manager.name_sentinel,
            checkpoint_manager.password,
        )
    else:
        redis_client = db_helper.RedisConnection(
            checkpoint_manager.host,
            checkpoint_manager.port,
            checkpoint_manager.password,
        )
    return redis_client


def cancel_job(args, job_id):
    is_debug = args.get("is_debug", False)
    if is_debug:
        job_info = processing_job_info.get(job_id.split("__")[1])
    else:
        job_info = processing_job_info.get(job_id)

    if job_info.implement_type == "code":
        from dataflow.stream.job.code_job import CodeJob

        return CodeJob(job_id, is_debug).cancel()
    if "flink" == job_info.component_type:
        from dataflow.stream.job.flink_job import FlinkJob

        return FlinkJob(job_id, is_debug).cancel()
        # jobserver_config = json.loads(job_info.jobserver_config)
        # geog_area_code = jobserver_config['geog_area_code']
        # cluster_id = jobserver_config['cluster_id']
        # return cancel_flink_job(args, geog_area_code, cluster_id, job_id)
    elif "storm" == job_info.component_type:
        from dataflow.stream.extend.job.job_for_storm import stop_storm_job

        return stop_storm_job(args, job_id)
    else:
        raise ComponentNotSupportError("the component %s is not supported." % job_info.component_type)


def get_job_conf(job_id):
    job_info = processing_job_info.get(job_id)
    job_config = json.loads(job_info.job_config)
    return (
        job_config["concurrency"],
        job_info.deploy_mode,
        job_info.deploy_config,
        job_info.code_version,
    )


def reset_checkpoint_on_migrate_kafka(job_id, head_rt, geog_area_code="inland"):
    if not job_id:
        raise JobNotExistsError()
    stream_job = processing_stream_job.get(job_id)
    processings = processing_stream_info.filter(stream_id=job_id)
    redis_client = _get_redis_client(geog_area_code)
    job_tails = stream_job.tails
    head_ids = stream_job.heads.split(",")
    # 待迁移的rt不是数据源时跳过重置
    if head_rt not in head_ids:
        return
    if stream_job.component_type == "storm":
        for processing in processings:
            processing_id = processing.processing_id
            chains = load_chain(head_rt, job_tails)
            if (
                processing.checkpoint_type == "offset"
                and processing_id in chains
                and "storages" in chains[processing_id]
                and chains[processing_id]["storages"]
            ):
                logger.info("reset storm redis checkpoint key (%s)" % processing_id)
                redis_client.delete(processing_id)
    elif stream_job.component_type == "flink":
        for processing in processings:
            if processing.checkpoint_type == "offset":
                checkpoint_name = _build_flink_offset_checkpoint_key(
                    heads=head_rt,
                    tails=job_tails,
                    processing_id=processing.processing_id,
                )
                logger.info("reset flink redis checkpoint key (%s)" % checkpoint_name)
                redis_client.delete(checkpoint_name)
