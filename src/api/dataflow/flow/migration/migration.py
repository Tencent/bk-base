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

from django.db import transaction

from dataflow.flow.models import (
    FlowExecuteLog,
    FlowInfo,
    FlowJob,
    FlowLinkInfo,
    FlowNodeInfo,
    FlowNodeProcessing,
    FlowNodeRelation,
)
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.log import flow_logger as logger
from dataflow.stream.models import ProcessingStreamInfo


@transaction.atomic
def add_flow(body):
    """
    将flow的点和线数据插入数据库，涉及表：FlowInfo,FlowNodeInfo,FlowNodeRelation,FlowJob,FlowNodeProcessing,FlowLinkInfo
    :param body: http请求传入的参数
    :return:
    """
    created_by = body["created_by"]
    created_at = body["created_at"]
    if body["updated_by"] is None or body["updated_by"] == "":
        updated_by = created_by
    else:
        updated_by = body["updated_by"]
    if body["updated_at"] is None or body["updated_at"] == "":
        updated_at = created_at
    else:
        updated_at = body["updated_at"]

    flow_id = body["flow_id"]
    FlowInfo.objects.create(
        flow_id=flow_id,
        flow_name=body["flow_name"],
        project_id=body["project_id"],
        status="no-start",
        is_locked=body["is_locked"],
        latest_version=body["latest_version"],
        bk_app_code=body["bk_app_code"],
        active=body["active"],
        created_by=body["created_by"],
        created_at=body["created_at"],
        locked_by=body["locked_by"],
        locked_at=body["locked_at"],
        updated_by=updated_by,
        updated_at=updated_at,
        description=body["description"],
    )
    # 更新模型中默认的 auto_now,auto_now_add = True 字段
    FlowInfo.objects.filter(flow_id=flow_id).update(created_at=created_at, locked_at=created_at, updated_at=updated_at)
    logger.info("insert table FlowInfo success,flow_id=%s" % flow_id)
    # 点
    nodes = body["nodes"]
    # id 到 node_id 的映射
    id_to_node_id_map = {}
    # 线
    links = body["links"]
    for node in nodes:
        node_id = FlowNodeInfo.objects.create(
            flow_id=flow_id,
            node_name=node["node_name"],
            node_config=json.dumps(node["node_config"]),
            node_type=node["node_type"],
            frontend_info=json.dumps(node["frontend_info"]),
            status="no-start",
            latest_version=node["latest_version"],
            running_version=node["running_version"],
            created_by=node["created_by"],
            created_at=node["created_at"],
            updated_by=updated_by,
            updated_at=updated_at,
            description=node["description"],
        ).node_id  # 获取自增的 node_id
        # 更新模型中默认的 auto_now,auto_now_add = True 字段
        FlowNodeInfo.objects.filter(node_id=node_id).update(created_at=created_at, updated_at=updated_at)
        logger.info("insert table FlowNodeInfo success,flow_id=%s" % flow_id)
        # 获取自增 id 放到map中
        id_to_node_id_map[node["id"]] = node_id

        node_config = node["node_config"]
        node_type = node["node_type"]

        if node_type in [
            NodeTypes.STREAM,
            NodeTypes.BATCH,
            NodeTypes.MERGE_KAFKA,
            NodeTypes.SPLIT_KAFKA,
            NodeTypes.MODEL,
        ]:
            result_table_id = "{}_{}".format(
                node_config["bk_biz_id"],
                node_config["table_name"],
            )
        else:
            result_table_id = node_config["result_table_id"]
        # FlowJob FlowNodeRelation FlowNodeProcessing
        FlowNodeRelation.objects.create(
            bk_biz_id=node_config["bk_biz_id"],
            project_id=body["project_id"],
            flow_id=flow_id,
            node_id=node_id,
            result_table_id=result_table_id,
            node_type=node_type,
            generate_type="user",
            is_head=1,
        )
        logger.info("insert table FlowNodeRelation success,flow_id=%s" % flow_id)
        if node_type in [NodeTypes.STREAM, NodeTypes.BATCH]:
            if "job_id" in node:
                job_id = node["job_id"]
                if job_id and job_id != "":
                    FlowJob.objects.create(
                        flow_id=flow_id,
                        node_id=node_id,
                        job_id=job_id,
                        job_type=node["job_type"],
                        description="job description",
                    )
                    logger.info("insert table FlowJob success,flow_id=%s" % flow_id)
        # FlowNodeProcessing
        if node_type in [
            NodeTypes.STREAM,
            NodeTypes.BATCH,
            NodeTypes.MERGE_KAFKA,
            NodeTypes.SPLIT_KAFKA,
            NodeTypes.MODEL,
        ]:
            processing_type = None
            if node_type == NodeTypes.STREAM:
                processing_type = "stream"
            elif node_type == NodeTypes.BATCH:
                processing_type = "batch"
            elif node_type in [
                NodeTypes.MERGE_KAFKA,
                NodeTypes.SPLIT_KAFKA,
            ]:
                processing_type = "transform"
            elif node_type in [NodeTypes.MODEL]:
                processing_type = (
                    "stream_model"
                    if node_config["serving_scheduler_params"]["serving_mode"] == "realtime"
                    else "batch_model"
                )
            processing_id = "{}_{}".format(
                node_config["bk_biz_id"],
                node_config["table_name"],
            )
            FlowNodeProcessing.objects.create(
                flow_id=flow_id,
                node_id=node_id,
                processing_id=processing_id,
                processing_type=processing_type,
                description=node["description"],
            )
            logger.info("insert table FlowNodeProcessing success,flow_id=%s" % flow_id)

    for link in links:
        FlowLinkInfo.objects.create(
            flow_id=flow_id,
            from_node_id=id_to_node_id_map[link["from_node_id"]],
            to_node_id=id_to_node_id_map[link["to_node_id"]],
            frontend_info=json.dumps(link["frontend_info"]),
        )
    logger.info("insert table FlowLinkInfo success,flow_id=%s" % flow_id)
    id_to_node_id_map.clear()


@transaction.atomic
def add_flow_by_yaml(body):
    """
    将flow写入数据库，和add_flow的区别：flow_id自增, 少了FlowJob表
    :param body:http传入参数
    :return:
    """
    # flow_id 自增
    cur_time = body["created_at"]
    flow_id = FlowInfo.objects.create(
        flow_name=body["flow_name"],
        project_id=body["project_id"],
        status="no-start",
        is_locked=body["is_locked"],
        latest_version=body["latest_version"],
        bk_app_code=body["bk_app_code"],
        active=body["active"],
        created_by=body["created_by"],
        created_at=body["created_at"],
        locked_by=body["locked_by"],
        locked_at=body["locked_at"],
        updated_by=body["updated_by"],
        updated_at=body["updated_at"],
        description=body["description"],
    ).flow_id  # 获取自增的 flow_id
    logger.info("insert table FlowInfo success,flow_id=%s" % flow_id)
    # 点
    nodes = body["nodes"]
    # id 到 node_id 的映射
    id_to_node_id_map = {}
    # 线
    links = body["links"]
    for node in nodes:
        node_id = FlowNodeInfo.objects.create(
            flow_id=flow_id,
            node_name=node["node_name"],
            node_config=json.dumps(node["node_config"]),
            node_type=node["node_type"],
            frontend_info=json.dumps(node["frontend_info"]),
            status="no-start",
            latest_version=node["latest_version"],
            running_version=node["running_version"],
            created_by=node["created_by"],
            created_at=node["created_at"],
            updated_by=node["updated_by"],
            updated_at=node["updated_at"],
            description=node["description"],
        ).node_id  # 获取自增的 node_id
        logger.info("insert table FlowNodeInfo success,flow_id=%s" % flow_id)
        # 获取自增 id 放到map中
        id_to_node_id_map[node["id"]] = node_id

        node_config = node["node_config"]
        node_type = node["node_type"]
        if node_type in [
            NodeTypes.STREAM,
            NodeTypes.BATCH,
            NodeTypes.MERGE_KAFKA,
            NodeTypes.SPLIT_KAFKA,
            NodeTypes.MODEL,
        ]:
            result_table_id = "{}_{}".format(
                node_config["bk_biz_id"],
                node_config["table_name"],
            )
        else:
            result_table_id = node_config["result_table_id"]
        # FlowNodeRelation
        FlowNodeRelation.objects.create(
            bk_biz_id=node_config["bk_biz_id"],
            project_id=body["project_id"],
            flow_id=flow_id,
            node_id=node_id,
            result_table_id=result_table_id,
            node_type=node_type,
            generate_type="user",
            is_head=1,
        )
        logger.info("insert table FlowNodeRelation success,flow_id=%s" % flow_id)
        # FlowNodeProcessing
        if node_type in [
            NodeTypes.STREAM,
            NodeTypes.BATCH,
            NodeTypes.MERGE_KAFKA,
            NodeTypes.SPLIT_KAFKA,
            NodeTypes.MODEL,
        ]:
            processing_type = None
            if node_type == NodeTypes.STREAM:
                processing_type = "stream"
            elif node_type == NodeTypes.BATCH:
                processing_type = "batch"
            elif node_type in [
                NodeTypes.MERGE_KAFKA,
                NodeTypes.SPLIT_KAFKA,
            ]:
                processing_type = "transform"
            elif node_type in [NodeTypes.MODEL]:
                processing_type = (
                    "stream_model"
                    if node_config["serving_scheduler_params"]["serving_mode"] == "realtime"
                    else "batch_model"
                )
            processing_id = "{}_{}".format(
                node_config["bk_biz_id"],
                node_config["table_name"],
            )
            FlowNodeProcessing.objects.create(
                flow_id=flow_id,
                node_id=node_id,
                processing_id=processing_id,
                processing_type=processing_type,
                description=node["description"],
            )
            logger.info("insert table FlowNodeProcessing success,flow_id=%s" % flow_id)
            # 新加一张表
            count_freq = node["node_config"]["count_freq"]
            if count_freq == 0:
                checkpoint_type = "offset"
            else:
                checkpoint_type = "timestamp"
            ProcessingStreamInfo.objects.create(
                processing_id=processing_id,
                stream_id="",
                concurrency=0,
                window="",
                checkpoint_type=checkpoint_type,
                processor_type="storm sql",
                processor_logic="",
                component_type="storm",
                created_by="",
                created_at=cur_time,
                updated_by="",
                updated_at=cur_time,
                description="yaml迁移插入",
            )
            logger.info("insert table ProcessingStreamInfo success,flow_id=%s" % flow_id)

    # 插入线
    for link in links:
        FlowLinkInfo.objects.create(
            flow_id=flow_id,
            from_node_id=id_to_node_id_map[link["from_node_id"]],
            to_node_id=id_to_node_id_map[link["to_node_id"]],
            frontend_info=json.dumps(link["frontend_info"]),
        )
    logger.info("insert table FlowLinkInfo success,flow_id=%s" % flow_id)
    id_to_node_id_map.clear()
    return flow_id


@transaction.atomic
def add_execute_log(body):
    """
    增加日志
    :param body:
    :return:
    """
    flow_id = body["flow_id"]
    logs = body["logs"]
    for log in logs:
        created_at = log["created_at"]
        id = FlowExecuteLog.objects.create(
            flow_id=flow_id,
            action=log["action"],
            status=log["status"],
            start_time=log["start_time"],
            created_at=log["created_at"],
            end_time=log["end_time"],
            created_by=log["created_by"],
            description=log["description"],
            version=log["version"],
            context=log["context"],
            logs_zh=log["logs_zh"],
            logs_en=log["logs_en"],
        ).id
        FlowExecuteLog.objects.filter(id=id).update(created_at=created_at)

    logger.info("insert table FlowExecuteLog success,flow_id=%s" % flow_id)
