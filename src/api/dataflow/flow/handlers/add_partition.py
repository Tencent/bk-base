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

import hashlib
import json
from itertools import chain

from conf.dataapi_settings import DATA_PORTAL_PREFIX
from django.db.models import Q
from django.utils.translation import ugettext as _

from dataflow.component.error_code.errorcodes import DataapiCommonCode as errorcodes
from dataflow.component.exceptions.comp_execptions import ResultTableException
from dataflow.flow.exceptions import FlowNotFoundError
from dataflow.flow.handlers.flow import FlowHandler
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.models import FlowInfo, FlowNodeInfo, FlowNodeRelation
from dataflow.flow.tasks import op_with_all_flow_and_nodes
from dataflow.shared.access.access_helper import AccessHelper
from dataflow.shared.auth.auth_helper import AuthHelper
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.send_message import send_message
from dataflow.shared.stream.utils.resource_util import (
    get_flink_yarn_cluster_queue_name,
    get_flink_yarn_session_queue_name,
    get_resource_group_id,
)
from dataflow.stream.api import stream_jobnavi_helper
from dataflow.stream.job.adaptors import FlinkAdaptor
from dataflow.stream.job.flink_job import FlinkJob
from dataflow.stream.job.job import Job
from dataflow.stream.models import ProcessingJobInfo, ProcessingStreamInfo, ProcessingStreamJob
from dataflow.stream.settings import DeployMode


class AddPartitionHandler(object):
    def __init__(self, operator, result_table_id, partitions, downstream_node_types, strict_mode):
        self.operator = operator
        self.result_table_id = result_table_id
        self.partitions = partitions
        self.downstream_node_types = downstream_node_types
        self.downstream_node_types_set = {x.strip() for x in self.downstream_node_types.split(",")}
        self.strict_mode = strict_mode
        # 扩分区需要操作的关联rt列表
        self.result_table_id_list_to_expand = []
        # rt分区数
        self.current_result_table_partitions = None
        # 是否需要扩rt分区
        self.need_expand_result_table_partition = True
        # process_type
        self.result_table_type = None
        # raw data id
        self.raw_data_id = ""
        # raw data 维护者列表
        self.raw_data_maintainers = []
        # 是否是清洗表
        self.clean_result_table_flag = False
        # raw data分区数
        self.current_raw_data_partitions = 0
        # 是否需要扩raw data分区
        self.need_expand_raw_data_partition = True
        # raw data是否完成扩分区（只操作一次）
        self.raw_data_expand_finished = False
        # 该rt相关的flow列表
        # self.dataflow_list = []
        # 发送通知消息相关
        self.title = "《IEG数据平台》 扩分区操作通知"

    def add_partition(self):
        self.result_table_id_list_to_expand = [self.result_table_id]
        affected_users = set()
        affected_users.add(self.operator)
        flow_name_list = ""

        try:
            # rt 下游只有 hdfs,无 kafka,则不用扩容
            meta_storages = self.get_meta_storages(affected_users)
            if not meta_storages:
                return
            # 获取result_table分区数
            self.current_result_table_partitions = self.get_current_partition()
            # 获取processing_type
            self.result_table_type = self.get_processing_type()

            if self.result_table_type == "clean":
                # 清洗表
                self.process_clean_rt()
            else:
                # 结果表
                logger.info("(%s)为结果表，无需关注清洗任务" % self.result_table_id)

            logger.info("需要扩分区的result_table列表为(%s)" % (",".join(self.result_table_id_list_to_expand)))
            for one_result_table_id in self.result_table_id_list_to_expand:
                source_dataflow_list, dataflow_as_stream_source_list, affected_users = self.get_related_dataflow(
                    one_result_table_id
                )
                flow_name_list = AddPartitionHandler.get_flow_name_list(
                    source_dataflow_list, dataflow_as_stream_source_list
                )
                affected_flow_dict = {}
                result_list = list(chain(source_dataflow_list, dataflow_as_stream_source_list))
                for i in result_list:
                    affected_flow_dict[i.flow_id] = i

                # 发送开始处理提示信息
                self.send_op_result_msg(one_result_table_id, flow_name_list, affected_users, start_flag=True)

                # 获取result_table_id所有运行中的分发任务的component
                task_components = DatabusHelper.get_running_task_components(one_result_table_id)
                logger.info("获取结果表({})对应的task_components为({})".format(one_result_table_id, task_components))

                # 检查实时计算资源是否充足
                token = self.check_add_partition(one_result_table_id)
                logger.info("检查结果表(%s)为STREAM_SOURCE的正在运行的实时计算任务的资源通过" % one_result_table_id)

                if task_components:
                    # 停止所有分发任务，如果有kafka，优先停止kafka
                    AddPartitionHandler.op_task_components(task_components, one_result_table_id, "stop")

                # 停止实时任务
                op_stop_res = self.op_stream_task(one_result_table_id, "stop", token)

                # raw data 扩分区
                self.expand_raw_data_partition(one_result_table_id)

                # 扩result table分区
                self.expand_normal_data_partition(one_result_table_id)

                # 启动分发
                if task_components:
                    AddPartitionHandler.op_task_components(task_components, one_result_table_id, "start")

                # 启动实时任务
                op_start_res = self.op_stream_task(one_result_table_id, "start", token)

                # 停止/启动任务信息收集
                stop_ex_flow_name_list = AddPartitionHandler.get_op_ex_flow_name_list(op_stop_res, affected_flow_dict)
                start_ex_flow_name_list = AddPartitionHandler.get_op_ex_flow_name_list(op_start_res, affected_flow_dict)

                # 发送处理结果消息提醒
                self.send_op_result_msg(
                    one_result_table_id,
                    flow_name_list,
                    affected_users,
                    False,
                    stop_ex_flow_name_list,
                    start_ex_flow_name_list,
                    None,
                )

        except Exception as ex:
            self.send_op_result_msg(self.result_table_id, flow_name_list, affected_users, False, None, None, ex)

    def send_op_result_msg(
        self,
        result_table_id,
        flow_name_list,
        affected_users,
        start_flag=False,
        stop_ex_flow_name_list=None,
        start_ex_flow_name_list=None,
        ex=None,
    ):
        op_ex_flow_name_list = ""
        # 操作开始
        start_op = False
        # 操作成功
        succ_op = False
        # 任务操作异常
        task_op_ex = False
        # 其他异常
        common_ex = False

        if stop_ex_flow_name_list or start_ex_flow_name_list:
            # 停止/启动任务时有异常出现，发送失败信息
            task_op_ex = True
            op_ex_flow_name_list = "停止失败任务列表( {} )，启动失败任务列表( {} )".format(
                stop_ex_flow_name_list,
                start_ex_flow_name_list,
            )
            result = "扩分区(%s)失败" % result_table_id
            message = "失败原因：部分任务停止/启动失败(%s)，请联系系统管理员处理" % op_ex_flow_name_list
        elif ex:
            # 有其他异常出现，发送失败信息
            common_ex = True
            result = "扩分区(%s)失败" % result_table_id
            message = "失败原因({})，如有异常请联系系统管理员".format(ex)
        elif start_flag:
            start_op = True
            result = "扩分区(%s)开始" % result_table_id
            message = (
                "由于数据扩分区操作的影响，直接下游的数据分发节点会自动重启，你的上述相关任务中的(%s)节点会自动重启。"
                "用户无需手工操作，如有异常请联系系统管理员" % self.downstream_node_types
            )
        else:
            # 发送成功消息
            succ_op = True
            result = "扩分区(%s)完成" % result_table_id
            message = (
                "由于数据扩分区操作的影响，直接下游的数据分发节点会自动重启，你的上述相关任务中的(%s)节点会自动重启。"
                "用户无需手工操作，如有异常请联系系统管理员" % self.downstream_node_types
            )

        content = _(
            "通知类型: {message_type}<br>"
            "任务名称: {flow_name}<br>"
            "部署类型: {op_type}<br>"
            "部署状态: {result}<br>"
            "详细信息: <br>{message}"
        ).format(
            message_type="扩数据分区，result_table_id(%s)，现有分区数(%s)，目标分区数(%s)"
            % (
                result_table_id,
                self.current_result_table_partitions,
                self.partitions,
            ),
            flow_name=flow_name_list,
            op_type="自动扩分区，无需用户手工操作",
            result=result,
            message=message,
        )
        try:
            send_message(affected_users, self.title, content, body_format="Html")
        except Exception as e:
            logger.exception(e)
            logger.warning("发送扩分区邮件失败")

        # 根据成功失败场景记录本地日志
        if start_op:
            logger.info(
                "扩分区开始: result_table_id(%s), partitions(%s), downstream_node_types(%s)"
                % (result_table_id, self.partitions, self.downstream_node_types)
            )
        if succ_op:
            logger.info(
                "扩分区完成: result_table_id(%s), partitions(%s), downstream_node_types(%s)"
                % (result_table_id, self.partitions, self.downstream_node_types)
            )
        if common_ex:
            logger.exception(ex)
            raise ex
        if task_op_ex:
            ex_msg = "扩分区失败: result_table_id(%s), partitions(%s), downstream_node_types(%s)，" "失败原因：部分任务停止/启动失败(%s)" % (
                result_table_id,
                self.partitions,
                self.downstream_node_types,
                op_ex_flow_name_list,
            )
            logger.info(ex_msg)
            raise ResultTableException(
                message=ex_msg,
                code=errorcodes.UNEXPECT_EX,
            )

    @staticmethod
    def get_op_ex_flow_name_list(op_res, affected_flow_dict):
        # 获取操作异常的flow链接列表信息
        op_ex_flow_name_list = ""
        if op_res and "fail_op_flow_list" in op_res:
            # 停止异常任务列表
            for flow_id in op_res["fail_op_flow_list"]:
                op_ex_flow = affected_flow_dict.get(flow_id)
                op_ex_flow_name_list += '<a href="{}/#/dataflow/ide/{}?project_id={}">{}</a>'.format(
                    DATA_PORTAL_PREFIX,
                    op_ex_flow.flow_id,
                    op_ex_flow.project_id,
                    op_ex_flow.flow_name,
                )
        return op_ex_flow_name_list

    def op_stream_task(self, result_table_id, op, token):
        # 启动/停止 result_table_id 相关的 stream 任务
        # op: start / stop
        downstream_node_types_set = {x.strip() for x in self.downstream_node_types.split(",")}
        op_res = op_with_all_flow_and_nodes(result_table_id, op, token, self.operator, downstream_node_types_set)
        if op_res:
            logger.info(
                "(%s)启动结果表(%s)对应的(%s)计算任务成功(%s)，失败(%s)"
                % (
                    "启动" if op == "start" else "停止",
                    result_table_id,
                    self.downstream_node_types,
                    ", ".join("%s" % fid for fid in op_res["succ_op_flow_list"]),
                    ", ".join("%s" % fid for fid in op_res["fail_op_flow_list"]),
                )
            )
        return op_res

    def expand_normal_data_partition(self, result_table_id):
        if self.need_expand_result_table_partition:
            # 扩普通rt(非raw data)的分区数
            DatabusHelper.set_result_table_partitions(result_table_id, self.partitions)
            logger.info(
                "对结果表(%s)扩分区为(%d -> %d)" % (result_table_id, self.current_result_table_partitions, self.partitions)
            )
        else:
            logger.info(
                "结果表(%s)分区已满足(%d -> %d)，无需操作" % (result_table_id, self.current_result_table_partitions, self.partitions)
            )

    def expand_raw_data_partition(self, result_table_id):
        if self.clean_result_table_flag and not self.raw_data_expand_finished:
            # 扩 raw data 分区数
            if self.need_expand_raw_data_partition:
                DatabusHelper.set_raw_data_partitions(self.raw_data_id, self.partitions)
                logger.info(
                    "对结果表(%s)对应的raw_data(%s)扩分区为(%d -> %d)"
                    % (
                        result_table_id,
                        self.raw_data_id,
                        self.current_raw_data_partitions,
                        self.partitions,
                    )
                )
            else:
                logger.info(
                    "结果表(%s)对应的raw_data(%s)分区已满足(%d -> %d)，无需操作"
                    % (
                        result_table_id,
                        self.raw_data_id,
                        self.current_raw_data_partitions,
                        self.partitions,
                    )
                )
            self.raw_data_expand_finished = True
        else:
            logger.info("对结果表(%s)无需操作对应的raw_data扩分区" % result_table_id)

    @staticmethod
    def op_task_components(task_components, result_table_id, op):
        op_components = task_components[:]
        # 启动分发，如果有kafka，优先启动kafka
        if "kafka" in op_components:
            if op == "start":
                DatabusHelper.create_task(result_table_id, ["kafka"])
            elif op == "stop":
                DatabusHelper.stop_task(result_table_id, ["kafka"])
            op_components.remove("kafka")
        if op == "start":
            DatabusHelper.create_task(result_table_id, op_components)
        elif op == "stop":
            DatabusHelper.stop_task(result_table_id, op_components)
        logger.info(
            "对结果表({})({})分发类型({})的分发任务".format(
                result_table_id, "启动" if op == "start" else "停止", ",".join([str(i) for i in task_components])
            )
        )

    def get_processing_type(self):
        result_table_type = ResultTableHelper.get_processing_type(self.result_table_id)
        if not result_table_type:
            raise ResultTableException(
                message="获取结果表%s的processing_type错误" % self.result_table_id,
                code=errorcodes.UNEXPECT_EX,
            )
        logger.info("获取结果表({})的processing_type为({})".format(self.result_table_id, result_table_type))
        return result_table_type

    def get_current_partition(self):
        current_result_table_partitions = DatabusHelper.get_result_table_partitions(self.result_table_id)
        if current_result_table_partitions > self.partitions:
            raise ResultTableException(
                message="结果表%s现有分区数(%d) > 目标分区数(%d), 无需扩容"
                % (self.result_table_id, current_result_table_partitions, self.partitions),
                code=errorcodes.UNEXPECT_EX,
            )
        elif current_result_table_partitions == self.partitions:
            self.need_expand_result_table_partition = False
            if self.strict_mode:
                raise ResultTableException(
                    message="结果表%s现有分区数(%d) = 目标分区数(%d), 无需扩容"
                    % (
                        self.result_table_id,
                        current_result_table_partitions,
                        self.partitions,
                    ),
                    code=errorcodes.UNEXPECT_EX,
                )
        logger.info(
            "结果表%s现有分区数(%d),目标分区数(%d)" % (self.result_table_id, current_result_table_partitions, self.partitions)
        )
        return current_result_table_partitions

    def get_related_dataflow(self, one_result_table_id):
        affected_users = set()
        affected_users.add(self.operator)
        # 获取rt_id相关的正在运行的dataflow列表（生产任务+使用任务）
        dataflow_list = FlowHandler.list_running_flow_by_rtid(one_result_table_id, as_queryset=True)
        # 获取rt_id作为stream_source相关的正在运行的dataflow列表（使用任务）
        dataflow_as_stream_source_list = FlowHandler.list_running_flow_by_rtid_as_source(
            one_result_table_id, as_queryset=True
        )
        logger.info(
            "获取结果表(%s)所在的正在运行的flow列表(%s)，以该结果表作为STREAM_SOURCE的正在运行的flow列表(%s)"
            % (
                one_result_table_id,
                ",".join(["{}-{}-{}".format(i.project_id, i.flow_id, i.flow_name) for i in dataflow_list]),
                ",".join(
                    ["{}-{}-{}".format(i.project_id, i.flow_id, i.flow_name) for i in dataflow_as_stream_source_list]
                ),
            )
        )

        # 生产任务
        # 如果是清洗产生的rt，则没有生产任务
        # 如果是计算产生的rt，则有生产任务
        source_dataflow_list = list(set(dataflow_list).difference(set(dataflow_as_stream_source_list)))
        if len(source_dataflow_list) > 1:
            source_flow_ids = []
            for i in source_dataflow_list:
                source_flow_ids.append(str(i.flow_id))
            raise ResultTableException(
                message=_("获取产生结果表%s的flow任务个数>1，source_flow_ids=%s，错误")
                % (one_result_table_id, ",".join(source_flow_ids)),
                code=errorcodes.UNEXPECT_EX,
            )

        # 获取所有project信息
        for one_dataflow in dataflow_list:
            # project_info = ProjectHelper.get_project(one_dataflow['project_id'])
            # affected_users.append(project_info['data']['created_by'])
            role_users = AuthHelper.list_project_role_users(one_dataflow.project_id)
            for one_role_users in role_users:
                # TODO: 是否去除特定管理员用户？
                affected_users.update(one_role_users.get("users"))
        logger.info(
            "获取结果表({})所在的所有flow的所有关联用户({})".format(one_result_table_id, ",".join([str(i) for i in affected_users]))
        )

        # 更新所有影响用户列表
        affected_users.update(self.raw_data_maintainers)
        return source_dataflow_list, dataflow_as_stream_source_list, affected_users

    @staticmethod
    def get_flow_name_list(source_dataflow_list, dataflow_as_stream_source_list):
        # 生产任务列表
        flow_name_list = ""
        for i in source_dataflow_list:
            flow_name_list += '<a href="{}/#/dataflow/ide/{}?project_id={}">{}（生产任务）</a>'.format(
                DATA_PORTAL_PREFIX,
                i.flow_id,
                i.project_id,
                i.flow_name,
            )
        # 使用任务列表
        for i in dataflow_as_stream_source_list:
            if flow_name_list:
                flow_name_list += " ， "
            flow_name_list += '<a href="{}/#/dataflow/ide/{}?project_id={}">{}</a>'.format(
                DATA_PORTAL_PREFIX,
                i.flow_id,
                i.project_id,
                i.flow_name,
            )
        return flow_name_list

    def process_clean_rt(self):
        self.clean_result_table_flag = True
        logger.info("获取结果表(%s)的processing_type为clean类型，进行处理..." % self.result_table_id)
        task_kafka_connect_info = DatabusHelper.get_task(self.result_table_id)
        if not task_kafka_connect_info:
            raise ResultTableException(
                message="获取结果表%s的kafka_connect路由信息错误" % self.result_table_id,
                code=errorcodes.UNEXPECT_EX,
            )

        # 获取processing_id
        processing_id = task_kafka_connect_info[0]["processing_id"]
        logger.info("获取结果表({})的processing_id为({})".format(self.result_table_id, processing_id))

        # 获取raw_data_id
        clean_info = DatabusHelper.get_raw_data_id(processing_id)
        if not clean_info:
            raise ResultTableException(
                message="获取processing_id(%s)的清洗配置信息错误" % processing_id,
                code=errorcodes.UNEXPECT_EX,
            )
        # logger.info(u"获取结果表(%s)的清洗配置为(%s)" % (result_table_id, clean_info))
        self.raw_data_id = clean_info["raw_data_id"]
        logger.info("获取结果表({})的raw_data_id为({})".format(self.result_table_id, self.raw_data_id))

        raw_data_info = AccessHelper.get_raw_data(self.raw_data_id, None)
        if not raw_data_info:
            raise ResultTableException(
                message="raw_data_id(%s)的 raw_data 信息错误" % self.raw_data_id,
                code=errorcodes.UNEXPECT_EX,
            )
        # 数据管理员
        self.raw_data_maintainers = {x.strip() for x in raw_data_info["maintainer"].split(",")}
        # 数据创建者
        self.raw_data_maintainers.add(raw_data_info["created_by"])
        logger.info(
            "获取raw_data({})所有maintainer({})".format(
                self.raw_data_id, ",".join([str(i) for i in self.raw_data_maintainers])
            )
        )

        self.current_raw_data_partitions = raw_data_info["storage_partitions"]
        if self.current_raw_data_partitions > self.partitions:
            raise ResultTableException(
                message="raw_data表%s现有分区数(%d) > 目标分区数(%d), 无需扩容"
                % (self.raw_data_id, self.current_raw_data_partitions, self.partitions),
                code=errorcodes.UNEXPECT_EX,
            )
        elif self.current_raw_data_partitions == self.partitions:
            self.need_expand_raw_data_partition = False
            if self.strict_mode:
                raise ResultTableException(
                    message="raw data表%s现有分区数(%d) = 目标分区数(%d), 无需扩容"
                    % (self.raw_data_id, self.current_raw_data_partitions, self.partitions),
                    code=errorcodes.UNEXPECT_EX,
                )
        logger.info(
            "获取结果表(%s)对应的raw_data(%s)的分区数为(%d)"
            % (self.result_table_id, self.raw_data_id, self.current_raw_data_partitions)
        )

        # 获取所有清洗任务列表
        clean_list = DatabusHelper.get_clean_list(self.raw_data_id)
        if not clean_list:
            raise ResultTableException(
                message=_("获取raw_data_id(%s)的清洗任务列表信息错误") % self.raw_data_id,
                code=errorcodes.UNEXPECT_EX,
            )
        # 多个清洗任务，也支持
        running_clean_list = []
        for clean_cfg in clean_list:
            if clean_cfg["status"] == "running" or clean_cfg["status"] == "started":
                running_clean_list.append(clean_cfg)
                self.result_table_id_list_to_expand.append(clean_cfg["processing_id"])
        self.result_table_id_list_to_expand = list(set(self.result_table_id_list_to_expand))

    def get_meta_storages(self, affected_users):
        # rt 下游只有 hdfs,无 kafka,则不用扩容
        meta_storages = ResultTableHelper.get_result_table_storage(self.result_table_id)
        # logger.info(u'元数据获取存储storages=%s' % json.dumps(meta_storages))
        if not meta_storages:
            content = _("通知类型: {message_type}<br>" "详细信息: <br>{message}").format(
                message_type="扩数据分区", message="网络异常，获取(%s)元数据存储失败" % self.result_table_id
            )
            try:
                send_message(affected_users, self.title, content, body_format="Html")
            except Exception as e:
                logger.exception(e)
                logger.warning("发送扩分区开始邮件失败")
            return None
        if "kafka" not in meta_storages:
            content = _("通知类型: {message_type}<br>" "详细信息: <br>{message}").format(
                message_type="扩数据分区", message="result_table_id下游无kafka存储，无需扩数据分区"
            )
            try:
                send_message(affected_users, self.title, content, body_format="Html")
            except Exception as e:
                logger.exception(e)
                logger.warning("发送扩分区开始邮件失败")
            return None
        return meta_storages

    # 检查判断是否允许扩充partition
    def check_add_partition(self, one_result_table_id):
        # 查看资源 是否允许扩充 当前并发度(优先级高)或者Worker数量的两倍 > partition,允许扩充
        # 1.查询当前 node_id(正在运行的) 一个数据源result_table_id可能被多个stream_source使用;只查询正在运行的节点
        active_node_list = []
        flow_relation_list = FlowNodeRelation.objects.filter(
            result_table_id=one_result_table_id,
            node_type=FlowNodeInfo.NodeTypes.STREAM_SOURCE,
        )
        for flow_relation in flow_relation_list:
            node_id = flow_relation.node_id
            flow_id = flow_relation.flow_id

            # 判断对应的flow是否在运行
            try:
                o_flow = FlowHandler(flow_id=flow_id)
                if o_flow.status == FlowInfo.STATUS.RUNNING:
                    stream_to_nodes = NodeUtils.get_stream_to_nodes_handler_by_id(
                        flow_id, node_id, node_types=self.downstream_node_types_set
                    )
                    active_node_list.append((flow_id, stream_to_nodes))
            except FlowNotFoundError:
                logger.warn("get flow_id(%s) not exist" % flow_id)
                pass

        logger.info("get active_node_list %s" % active_node_list)

        # set<job_id>，用于job去重
        job_have_seen = set()
        # list<job object>，需要修改concurrency的job列表
        need_modify_concurrency_job_list = []

        flink_global_queue_resources = {}
        flink_global_queue_resources_done = False
        flink_check_summary = []
        storm_check_summary = []

        # 遍历flow&job，获取资源检查相关信息
        for flow_node_tuple in active_node_list:
            one_flow_id = flow_node_tuple[0]
            logger.info("processing flow_id({}), node_id({})".format(flow_node_tuple[0], flow_node_tuple[1]))
            for node_id in flow_node_tuple[1]:
                node_handler = NODE_FACTORY.get_node_handler(node_id=node_id)
                one_job = node_handler.get_job()
                # one_job 可能无 job_id 属性
                if not hasattr(one_job, "job_id"):
                    logger.info("can not get job_id for one_job, flow({}), node({})".format(one_flow_id, node_id))
                    continue
                job_id = one_job.job_id
                if not job_id:
                    logger.info("can not get job_id for flow({}), node({})".format(one_flow_id, node_id))
                    continue
                if job_id in job_have_seen:
                    logger.info("get repeated job_id({}) for flow({}), node({})".format(job_id, one_flow_id, node_id))
                    continue
                else:
                    job_have_seen.add(job_id)

                logger.info(
                    "get job_id({}) for flow_id({}), node_id({})".format(job_id, flow_node_tuple[0], flow_node_tuple[1])
                )

                # 获取并发度配置
                stream_info = ProcessingStreamInfo.objects.filter(
                    Q(stream_id=job_id),
                    Q(component_type__startswith="storm")
                    | Q(component_type__startswith="flink")
                    | Q(component_type__startswith="spark_structured_streaming"),
                )
                if not stream_info:
                    logger.info("can not get stream_info for job_id(%s)" % (job_id))
                    continue
                concurrency = stream_info[0].concurrency
                if concurrency and concurrency != 0:
                    # 如果节点设置了并发度，需后续设置concurrency=null
                    need_modify_concurrency_job_list.append(Job(job_id))

                # 获取cluster_name
                stream_job = ProcessingStreamJob.objects.filter(stream_id=stream_info[0].stream_id)
                if not stream_job:
                    logger.info(
                        "can not get stream_job for job_id({}), stream_id({})".format(job_id, stream_info[0].stream_id)
                    )
                    continue
                cluster_name = stream_job[0].cluster_name
                cluster_group = stream_job[0].cluster_group
                current_deploy_mode = stream_job[0].deploy_mode
                component_type = stream_job[0].component_type
                logger.info(
                    "get ProcessingStreamJob cluster_name(%s), cluster_group(%s), current_deploy_mode(%s)"
                    % (cluster_name, cluster_group, current_deploy_mode)
                )

                processing_job_info = ProcessingJobInfo.objects.get(job_id=job_id)
                # 获取可能存在的配置的deploy_mode
                configured_deploy_mode = processing_job_info.deploy_mode
                logger.info("get ProcessingJobInfo configured_deploy_mode(%s)" % configured_deploy_mode)

                if processing_job_info.component_type.startswith("flink"):
                    # flink
                    if not flink_global_queue_resources_done:
                        flink_global_queue_resources = self.process_flink_resource(
                            job_id,
                            cluster_name,
                            current_deploy_mode,
                            configured_deploy_mode,
                            cluster_group,
                            component_type,
                            one_flow_id,
                            flink_global_queue_resources_done,
                            flink_check_summary,
                        )
                        # 如果全量资源信息未获取则发起请求，否则不请求，只请求成功一次即可
                        if flink_global_queue_resources:
                            flink_global_queue_resources_done = True
                elif processing_job_info.component_type.startswith("storm"):
                    self.process_storm_resource(
                        job_id,
                        one_flow_id,
                        processing_job_info.cluster_name,
                        processing_job_info.cluster_group,
                        storm_check_summary,
                    )
                else:
                    logger.warning(
                        "flow_id(%s), job_id(%s), processing_job_info.component_type(%s) 暂不支持资源检查"
                        % (one_flow_id, job_id, processing_job_info.component_type)
                    )

        # storm resource check
        # 模拟停止任务-释放资源，提交任务-申请资源的过程，逐个检查资源是否满足
        if storm_check_summary:
            from dataflow.flow.extend.handlers.add_storm_partition import AddStormPartitionHandler

            AddStormPartitionHandler.storm_resources_check(storm_check_summary)

        # flink resource check
        if flink_check_summary:
            if not flink_global_queue_resources:
                raise Exception("未能获取flink集群资源情况，暂不进行扩容操作")
            AddPartitionHandler.flink_resources_check(flink_check_summary, flink_global_queue_resources)

        # 资源检查通过后，设置ProcessingJobInfo.job_config中的concurrency为null
        if need_modify_concurrency_job_list:
            logger.info("更新(%s)个任务的processing_job_info的concurrency为null" % (len(need_modify_concurrency_job_list)))
            AddPartitionHandler.update_job_concurrency_config(need_modify_concurrency_job_list)

        # 根据result_table_id生成随机ID返回
        return hashlib.new("md5", one_result_table_id).hexdigest()

    def process_flink_resource(
        self,
        job_id,
        cluster_name,
        current_deploy_mode,
        configured_deploy_mode,
        cluster_group,
        component_type,
        one_flow_id,
        flink_global_queue_resources_done,
        flink_check_summary,
    ):
        logger.info("entering flink process for job(%s)" % job_id)
        one_flink_job = FlinkJob(job_id)
        flink_adaptor = FlinkAdaptor(one_flink_job)
        current_partitions = self.current_result_table_partitions
        geog_area_code = one_flink_job.geog_area_code
        cluster_id = one_flink_job.cluster_id
        logger.info(
            "get job_id(%s), geog_area_code(%s), cluster_id(%s), cluster_name(%s), current_deploy_mode(%s)"
            % (
                job_id,
                geog_area_code,
                cluster_id,
                cluster_name,
                current_deploy_mode,
            )
        )

        jobnavi_stream_helper = stream_jobnavi_helper.StreamJobNaviHelper(geog_area_code, cluster_id)
        target_total_partition = flink_adaptor.source_partitions + self.partitions - current_partitions

        # 目标deploy_mode
        target_deploy_mode = AddPartitionHandler.get_target_deploy_mode(configured_deploy_mode, target_total_partition)

        # 计算目标tm所需内存，目标队列名称
        target_tm_memory = AddPartitionHandler.get_flink_total_task_manager_memory(
            flink_adaptor, target_deploy_mode, target_total_partition
        )
        target_queue_name = AddPartitionHandler.get_flink_yarn_submit_queue_name(
            geog_area_code,
            cluster_group,
            component_type,
            target_deploy_mode,
        )
        logger.info("get target_tm_memory({}), target_queue_name({})".format(target_tm_memory, target_queue_name))

        # 获取当前tm内存/目标队列名称
        current_tm_memory = AddPartitionHandler.get_flink_total_task_manager_memory(flink_adaptor, current_deploy_mode)
        current_queue_name = AddPartitionHandler.get_flink_yarn_submit_queue_name(
            geog_area_code,
            cluster_group,
            component_type,
            current_deploy_mode,
        )
        logger.info("get current_tm_memory({}), current_queue_name({})".format(current_tm_memory, current_queue_name))

        # 如果全量资源信息未获取则发起请求，否则不请求，只请求成功一次即可
        flink_global_queue_resources = None
        if not flink_global_queue_resources_done:
            flink_global_queue_resources = AddPartitionHandler.get_flink_resources(
                jobnavi_stream_helper,
                current_deploy_mode,
                job_id,
                cluster_name,
            )

        # 生成flink资源统计信息
        # flowid - jobid - current_queue - target_queue - deploymode - target_deploy_mode -
        # sourcepartitions - partitions - current_tmmemory - targe_tmmemory
        flink_check_summary_item = (
            one_flow_id,
            job_id,
            current_queue_name,
            target_queue_name,
            current_deploy_mode,
            target_deploy_mode,
            current_partitions,
            self.partitions,
            current_tm_memory,
            target_tm_memory,
        )
        logger.info("flink get summary info({})".format(flink_check_summary_item))
        flink_check_summary.append(flink_check_summary_item)
        return flink_global_queue_resources

    def process_storm_resource(self, job_id, one_flow_id, cluster_name, cluster_group, storm_check_summary):
        from dataflow.flow.extend.handlers.add_storm_partition import AddStormPartitionHandler

        AddStormPartitionHandler.process_storm_resource(
            job_id,
            one_flow_id,
            cluster_name,
            cluster_group,
            storm_check_summary,
            self.current_result_table_partitions,
            self.partitions,
        )

    @staticmethod
    def update_job_concurrency_config(need_modify_concurrency_job_list):
        for one_job in need_modify_concurrency_job_list:
            one_job.update_job_conf({"concurrency": None})
            logger.info("更新jobid(%s)的processing_job_info的concurrency为null" % (one_job.job_id))

    @staticmethod
    def get_target_deploy_mode(configured_deploy_mode, target_total_partition):
        if configured_deploy_mode:
            target_deploy_mode = configured_deploy_mode
        else:
            target_deploy_mode = (
                DeployMode.YARN_CLUSTER.value if target_total_partition > 5 else DeployMode.YARN_SESSION.value
            )
        return target_deploy_mode

    @staticmethod
    def get_flink_yarn_submit_queue_name(geog_area_code, cluster_group, component_type, deploy_mode):
        resource_group_id = get_resource_group_id(geog_area_code, cluster_group, component_type)
        target_queue_name = None
        if deploy_mode == DeployMode.YARN_CLUSTER.value:
            target_queue_name = get_flink_yarn_cluster_queue_name(geog_area_code, resource_group_id)
        elif deploy_mode == DeployMode.YARN_SESSION.value:
            target_queue_name = get_flink_yarn_session_queue_name(geog_area_code, resource_group_id)
        return target_queue_name

    @staticmethod
    def get_flink_total_task_manager_memory(flink_adaptor, deploy_mode, partitions=None):
        target_tm_memory = None
        if deploy_mode == DeployMode.YARN_CLUSTER.value:
            target_tm_memory = flink_adaptor.get_yarn_cluster_total_tm_mem(partition=partitions)
        elif deploy_mode == DeployMode.YARN_SESSION.value:
            target_tm_memory = flink_adaptor.get_yarn_session_total_tm_memo(partition=partitions)
        return target_tm_memory

    @staticmethod
    def get_flink_resources(jobnavi_stream_helper, current_deploy_mode, job_id, cluster_name):
        flink_global_queue_resources = None
        if current_deploy_mode == DeployMode.YARN_CLUSTER.value:
            logger.info("entering YARN_CLUSTER mode...")

            execute_id = jobnavi_stream_helper.get_execute_id(job_id)
            logger.info("entering YARN_CLUSTER mode, execute_id(%s)" % execute_id)

            event_info_dict = {"queue": "root.dataflow.stream.default.cluster"}
            event_id = jobnavi_stream_helper.request_jobs_resources(
                execute_id, {"event_info": str(json.dumps(event_info_dict))}
            )
            logger.info("entering YARN_CLUSTER mode, event_id(%s)" % event_id)

            event_result = jobnavi_stream_helper.get_event_result(event_id)
            logger.info("get YARN_CLUSTER event result(%s)" % event_result)

            # 全量队列<--->每个队列资源余量，只请求一次
            if event_result:
                flink_global_queue_resources = eval(event_result)
                if flink_global_queue_resources:
                    logger.info("get YARN_CLUSTER global flink resource info(%s)" % flink_global_queue_resources)
        # yarn session
        elif current_deploy_mode == DeployMode.YARN_SESSION.value:
            logger.info("entering YARN_SESSION mode...")

            if not cluster_name:
                cluster_name = "debug_standard"
            execute_id = jobnavi_stream_helper.get_execute_id(cluster_name)
            logger.info("entering YARN_SESSION mode, execute_id(%s)" % execute_id)

            event_id = jobnavi_stream_helper.request_jobs_resources(execute_id)
            logger.info("entering YARN_SESSION mode, event_id(%s)" % event_id)

            event_result = jobnavi_stream_helper.get_event_result(event_id)
            logger.info("get YARN_SESSION event result(%s)" % event_result)

            # 全量队列<--->每个队列资源余量，只请求一次
            if event_result:
                flink_global_queue_resources = eval(event_result)
                if flink_global_queue_resources:
                    logger.info("get YARN_SESSION global flink resource info(%s)" % flink_global_queue_resources)

        return flink_global_queue_resources

    @staticmethod
    def flink_resources_check(flink_check_summary, flink_global_queue_resources):
        check_target_queue_list = []
        for one_check_summary_item in flink_check_summary:
            # flowid - jobid - current_queue - target_queue - deploymode - target_deploy_mode -
            # sourcepartitions - partitions - current_tmmemory - targe_tmmemory
            check_flow_id = one_check_summary_item[0]
            check_job_id = one_check_summary_item[1]
            check_current_queue = one_check_summary_item[2]
            check_target_queue = one_check_summary_item[3]
            check_current_deploy_mode = one_check_summary_item[4]
            check_target_deploy_mode = one_check_summary_item[5]
            check_current_partitions = one_check_summary_item[6]
            check_target_partitions = one_check_summary_item[7]
            check_current_tm_memory = one_check_summary_item[8]
            check_target_tm_memory = one_check_summary_item[9]
            logger.info(
                "Flink - check_flow_id(%s), check_job_id(%s), "
                "check_current_queue(%s), check_target_queue(%s), "
                "check_current_deploy_mode(%s), check_target_deploy_mode(%s), "
                "check_current_partitions(%s), check_target_partitions(%s), "
                "check_current_tm_memory(%s), check_target_tm_memory(%s)"
                % (
                    check_flow_id,
                    check_job_id,
                    check_current_queue,
                    check_target_queue,
                    check_current_deploy_mode,
                    check_target_deploy_mode,
                    check_current_partitions,
                    check_target_partitions,
                    check_current_tm_memory,
                    check_target_tm_memory,
                )
            )
            # 对列资源更新，原队列+原使用的资源，新队列-欲使用的资源
            if check_current_queue in flink_global_queue_resources:
                flink_global_queue_resources[check_current_queue] = (
                    flink_global_queue_resources.get(check_current_queue) + check_current_tm_memory
                )
            else:
                logger.warning(
                    "can not find check_current_queue(%s) in flink_global_queue_resources" % check_current_queue
                )
                raise Exception("不能在全局资源池中找到check_current_queue队列（%s）的资源信息" % check_current_queue)
            if check_target_queue in flink_global_queue_resources:
                flink_global_queue_resources[check_target_queue] = (
                    flink_global_queue_resources.get(check_target_queue) - check_target_tm_memory
                )
            else:
                logger.warning(
                    "can not find check_target_queue(%s) in flink_global_queue_resources" % check_target_queue
                )
                raise Exception("不能在全局资源池中找到check_target_queue队列（%s）的资源信息" % check_target_queue)
            check_target_queue_list.append(check_target_queue)
        for one_queue, one_resource in list(flink_global_queue_resources.items()):
            if one_queue in check_target_queue_list and one_resource < 0:
                raise Exception("flink任务准备提交的队列(%s)资源不足，不支持扩容" % one_queue)
