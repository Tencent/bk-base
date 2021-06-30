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
import random
import time

from common.transaction import auto_meta_sync
from conf.dataapi_settings import BATCH_IP_RESULT_TABLE_ID
from django.utils.translation import ugettext as _

from dataflow.flow import exceptions as Errors
from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import FlowError
from dataflow.flow.handlers.link_rules import NodeInstanceLink
from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.models import FlowInfo, FlowNodeInfo, FlowNodeInstance
from dataflow.flow.node_types import NodeTypes
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.project.project_helper import ProjectHelper


def call_func_with_rollback(func, params, rollback_func_info_list=None, retry=3):
    """
    调用某个指定函数，若失败则进行回滚调用
    @param func: 待调用函数引用
    @param params: 待调用函数参数，dict类型
    @param rollback_func_info_list: 回滚函数信息，按顺序回滚 e.g. [{'func': xxx, 'params': {}}], 若为空表示不用回滚
    @param retry: 重试次数
    @return:
    """
    if not rollback_func_info_list:
        rollback_func_info_list = []
    try:
        if params is None:
            return func()
        elif type(params).__name__ == "dict":
            return func(**params)
        else:
            return func(params)
    except Exception as e:
        # 回滚前先打印所有回滚信息
        logger.exception(e)
        for rollback_func_info in rollback_func_info_list:
            rollback_func = rollback_func_info["func"]
            rollback_func_name = getattr(rollback_func, "__name__")
            rollback_params = rollback_func_info["params"]
            logger.info("rollback_func_name: ({}), rollback_params: ({})".format(rollback_func_name, rollback_params))
        # 标记回滚整体是否成功
        rollback_result = True
        for rollback_func_info in rollback_func_info_list:
            rollback_func = rollback_func_info["func"]
            rollback_func_name = getattr(rollback_func, "__name__")
            rollback_params = rollback_func_info["params"]
            current_retry = 0
            # 当前回滚步骤是否成功的标记
            current_func_rollback_result = False
            logger.info(_("开始回滚: %s") % rollback_func_name)
            rollback_exception_message = ""
            while current_retry < retry and not current_func_rollback_result:
                try:
                    if rollback_params is None:
                        rollback_func()
                    else:
                        rollback_func(**rollback_params)
                    current_func_rollback_result = True
                    logger.info(_("回滚成功: %s") % rollback_func_name)
                except FlowError as rollback_exception:
                    time.sleep(1)
                    current_retry = current_retry + 1
                    rollback_exception_message = rollback_exception.message
            if current_retry >= retry:
                # 按顺序回滚，失败则停止回滚，因为回滚操作可能有依赖关系
                rollback_result = False
                logger.exception(
                    _("回滚调用(%(func_name)s)失败-%(message)s")
                    % {
                        "func_name": rollback_func_name,
                        "message": rollback_exception_message,
                    }
                )
                # 回滚列表中一个回滚失败即整体回滚失败
                break
        raise Errors.APIRollbackError(
            _("%(msg1)s%(msg2)s") % {"msg1": "" if rollback_result else "【回滚失败】", "msg2": "{}".format(e)}
        )


class NodeHandler(object):
    """
    节点控制类，所有节点的父节点，继承须知
    1. 必须重载的属性列表 [node_type, node_form, config_attrs]，
    2. 必须重载的方法列表 [build_output]
    """

    # 子类需重载属性
    node_type = None
    node_form = None
    config_attrs = []
    # TODO 这三个变量以后都优化或者删除
    storage_type = None
    default_storage_type = None
    channel_storage_type = None

    def __init__(self, node_id=None, current_node_type=None):
        if node_id is not None:
            node_id = int(node_id)
        self.node_id = node_id
        # 内部变量初始化
        self._node_info = None
        # TODO 元数据操作移到控制内
        self._meta_api_operate_list = {}
        # 标签信息
        self._manage_tags = None

    # ------------------------------- 节点核心操作流程 ------------------------------- #
    def add_node_info(self, username, flow_id, from_links, form_data, frontend_info=None):
        """
        添加节点流程
        """
        from_node_ids = [link["source"]["node_id"] for link in from_links]
        build_form_data = self.build_before(from_node_ids, form_data)

        c_params = self.build_create_params(username, flow_id, build_form_data, frontend_info)

        node = FlowNodeInfo.objects.create(**c_params)
        self.node_id = node.node_id
        self.node_info.create_or_update_link(from_links)
        # 返回表单参数，供控制类使用
        return build_form_data

    def update_node_info(self, username, from_node_ids=None, form_data=None, frontend_info=None):
        """
        更新节点流程
        """
        build_form_data = self.build_before(from_node_ids, form_data, False)
        u_params = self.build_update_params(username, build_form_data, frontend_info)
        for _k, _v in list(u_params.items()):
            setattr(self.node_info, _k, _v)
        self.node_info.save()
        return build_form_data

    def remove_node_info(self):
        """
        删除节点
        """
        self.node_info.delete()
        self._clear_node()

    def to_dict(self, add_has_modify=False, add_result_table_ids=True, add_related_flow=False):
        """
        获取节点信息
        """
        status = self.status
        if add_has_modify:
            has_modify = self.has_modify()
            if has_modify and status in FlowInfo.PROCESS_STATUS:
                status = FlowInfo.STATUS.WARNING
        else:
            has_modify = False

        node_dict = {
            "flow_id": self.flow_id,
            "node_id": self.node_id,
            "node_name": self.name,
            "node_config": self.get_config(),
            "node_type": self.node_type,
            "status": status,
            # 'version': self.version,      # 有性能下降，先忽略
            "version": "",
            "frontend_info": self.node_info.get_frontend_info(),
            "has_modify": has_modify,
        }

        if add_result_table_ids:
            node_dict["result_table_ids"] = self.result_table_ids
        if add_related_flow:
            node_dict["related_flow"] = {
                "status": self.flow.status,
                "flow_name": self.flow.flow_name,
                "project_id": self.flow.project_id,
            }
        return node_dict

    # ------------------------------- 节点通用继承函数 ------------------------------- #
    def build_before(self, from_node_ids, form_data, is_create=True):
        """
        创建、更新节点前的操作，等于 add_before 和 update_before，取决于 is_create 参数
        主要用途：
        1. 参数校验
        2. 部分节点需要从外部获取参数，并更新到 form_data
        """
        # 连线校验
        self.link_rules_check(from_node_ids, form_data, is_create)
        # 静态关联校验
        NodeHandler.kv_source_check(from_node_ids, form_data)
        # 更新节点时判断节点是否有 rt
        if not is_create:
            result_table_ids_num = len(self.result_table_ids)
            if result_table_ids_num == 0:
                raise Errors.NodeError(_("节点(id=%(node_id)s)对应的结果表不存在.") % {"node_id": self.node_id})
        return form_data

    def add_after(self, username, from_node_ids, form_data):
        """
        钩子函数，节点成功添加后回调
        @param {string} username 操作人
        @param {dict} form_data 经过校验清洗后的数据，结构与 self.config_attrs 保持一致
        @param {list} from_node_ids 父节点列表
        """
        return None

    def update_after(self, username, from_node_ids, form_data, prev=None, after=None):
        """
        钩子函数，节点成功更新后回调
        @param {dict} form_data 经过校验清洗后的数据，结构与 self.config_attrs 保持一致
        @param {list} from_node_ids 父节点列表
        @param {dict} prev 变更前的节点数据
        @param {dicr} after 变更后的节点数据
        """
        return None

    def remove_before(self, username):
        """
        钩子函数，节点删除前回调
        """
        pass

    def update_node_metadata_relation(self, bk_biz_id, result_table_ids, heads=None, tails=None):
        """
        更新节点与rt、processing等实体的关联关系
        @param bk_biz_id:
        @param result_table_ids:
        @param heads: 只有计算节点有 heads，tails 参数
        @param tails:
        """
        raise NotImplementedError("节点类型（%s）未定义update_node_relation方法." % self.node_type)

    # ------------------------------- 节点通用普通函数 ------------------------------- #
    def get_config(self, loading_latest=True):
        """
        获取节点当前配置
        """
        return self.clean_config(self.node_info.load_config(loading_latest), False)

    def build_create_params(self, username, flow_id, form_data, frontend_info=None):
        """
        组装节点的新建参数
        """
        node_type = self.node_type

        c_params = {
            "flow_id": flow_id,
            "node_name": form_data["name"],
            "node_type": node_type,
            "status": "no-start",
            "node_config": json.dumps(form_data),
            "latest_version": NodeUtils.gene_version(),
            "frontend_info": json.dumps(frontend_info),
            "created_by": username,
            "updated_by": username,
            "description": "",
        }
        return c_params

    def build_update_params(self, username, form_data, frontend_info=None):
        """
        组装节点的更新参数
        """
        u_params = {
            "node_id": self.node_id,
            "node_config": json.dumps(form_data),
            "frontend_info": json.dumps(frontend_info),
            "updated_by": username,
        }
        return u_params

    @classmethod
    def init_by_node(cls, node, current_node_type=None):
        node_handler = cls(node.node_id, current_node_type)
        node_handler._node_info = node
        return node_handler

    def get_node_type_display(self):
        """
        获取节点类型名称
        """
        if self.node_type in NodeTypes.DISPLAY:
            return NodeTypes.DISPLAY[self.node_type]
        else:
            return self.node_type

    def clean_config(self, config, is_create):
        """
        按照 cls.config_attrs 返回约定结构，可以设置部分字段的默认值，未来可以在结构中定义
        类型、默认值等属性，结合这个方法来规整 config 的返回
        """
        form = self.node_form(config, is_create=is_create)
        if not form.is_valid():
            _err_msg = "[Node Error] invalid node config, node_id={}, error={},".format(self.node_id, form.errors)
            if hasattr(form, "format_errmsg"):
                _err_msg = form.format_errmsg()
            logger.error(_err_msg)
            raise Errors.NodeValidError(_err_msg)

        form_data = form.cleaned_data

        # 避免 Form 表单解析，丢了部分关键字段，此处做了 None 值补全
        return {
            _attr: form_data[_attr] if _attr in form_data else None
            # for _attr in get_all_fields_from_form(form)
            for _attr in self.config_attrs
        }

    @classmethod
    def kv_source_check(cls, from_node_ids, form_data):
        from_node_ids_num = len(from_node_ids)
        if from_node_ids_num > 0:
            from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)
            bk_biz_ids = []
            for _n in from_nodes:
                # 除了以下两种情况，暂不支持不同业务的数据关联操作
                # 1. 上游为 kv_source 且是公有的
                # 2. 上游为 IP 库的离线数据源(TODO: 支持离线关联数据源后可能需要取消)
                if not (
                    (_n.node_type == NodeTypes.BATCH_SOURCE and _n.result_table_id in BATCH_IP_RESULT_TABLE_ID)
                    or (_n.node_type in NodeTypes.ALL_KV_SOURCE_CATEGORY and ResultTable(_n.result_table_id).is_public)
                ):
                    bk_biz_ids.append(_n.get_config(False)["bk_biz_id"])
            # 暂不支持仅公有静态关联结果表的数据关联操作
            # 即上游都是公有(1个或多个)静态关联是不允许的，不区分跨不跨业务
            # 这种情形恰好包含了 591_iplib 和 591_ipv6lib 单独作为上游离线静态关联数据源的情况
            # TODO: 到时若有结果表从私有上升为公有需要调整逻辑
            if len(set(bk_biz_ids)) < 1:
                raise Errors.NodeValidError(_("暂不支持仅公开数据的计算操作"))
            # 暂不支持所有情况下多个私有业务的数据关联操作
            if len(set(bk_biz_ids)) > 1:
                raise Errors.NodeValidError(_("暂不支持不同业务的数据关联操作"))
            # 将原始数据节点的业务 ID 传递
            # 下游业务必须继承私有的关联节点业务或普通节点业务
            # TODO: 目前公开数据只可能是 591 业务的数据，若之后出现非 591 业务的公有数据，可能需要调整逻辑
            if form_data["bk_biz_id"] not in bk_biz_ids:
                raise Errors.NodeValidError(_("出现非法业务ID(%(bk_biz_id)s)") % {"bk_biz_id": form_data["bk_biz_id"]})

    def link_rules_check(self, from_node_ids, form_data, is_create):
        node_type = self.node_type
        from_nodes = NodeUtils.list_from_nodes_handler(from_node_ids)
        from_result_table_ids = NodeUtils.get_from_result_table_ids(node_type, form_data)
        graph_from_result_table_ids = []
        if len(from_nodes) > len(from_result_table_ids):
            # 若用户为某个支持多个父节点的节点增加具有相同输入 RT 的上游节点，可能绕过下一步的校验
            # 这里确保节点数量大于上游 RT 的数量
            raise Errors.NodeValidError(_("连线配置错误，当前修改了上游节点，需要重新保存"))
        for _n in from_nodes:
            # 【2.2校验】
            _n_from_result_table_ids = _n.result_table_ids
            # 上游任意一个节点的所有rt必须至少有一个作为当前节点的接入rt
            judge_num = len(set(_n_from_result_table_ids) & set(from_result_table_ids))
            if judge_num == 0:
                raise Errors.NodeValidError(
                    _("节点(%(node_name)s)连线错误，其上游节点(%(from_node_name)s)的结果表应被当前节点指定使用%(message)s")
                    % {
                        "from_node_name": _n.name,
                        "node_name": form_data["name"],
                        "message": "" if is_create else _("，请检查节点连线配置并重新保存"),
                    }
                )
            # 收集当前节点在画布离线的所有上游节点的rt
            graph_from_result_table_ids.extend(_n_from_result_table_ids)
        # 2. 此时from_result_table_ids为旧信息，因此需要考虑两种情况
        # 2.1 当前画布连线rt列表需包含from_result_table_ids，设想一下用户删除了上游节点，然后直接启动
        if node_type not in NodeTypes.SOURCE_CATEGORY and len(
            set(graph_from_result_table_ids) & set(from_result_table_ids)
        ) < len(set(from_result_table_ids)):
            raise Errors.NodeValidError(
                _("连线错误，节点不可使用未经连线的父结果表，请重新保存节点(%(node_name)s)") % {"node_name": form_data["name"]}
            )
        # 3. 校验连线规则
        for _n in from_nodes:
            # 1. 校验上游节点可否连接到当前节点
            # 2. 校验上游节点数量、以及上游节点的下游节点数量是否符合要求
            # 取交集，只取出这个节点有用的 rt 输出, 考虑到可能会有多输出 rt，但只有其中某些被用到的情况
            _n_from_result_table_ids = list(set(_n.result_table_ids) & set(from_result_table_ids))
            NodeInstanceLink.validate_node_link(_n, self, _n_from_result_table_ids, from_nodes, is_create)

    def get_frontend_info(self):
        return self.node_info.get_frontend_info()

    def get_from_nodes_handler(self):
        return [NODE_FACTORY.get_node_handler(_id) for _id in self.from_node_ids]

    def get_to_nodes_handler(self):
        return [NODE_FACTORY.get_node_handler(_id) for _id in self.to_node_ids]

    @auto_meta_sync(using="default")
    def set_status(self, status):
        """
        设置节点状态
        @todo 设置 running 状态是，需要把当前的版本标记为 running_version
        """
        if status in [FlowInfo.STATUS.RUNNING]:
            self.node_info.running_version = self.node_info.latest_version

        self.node_info.status = status
        self.node_info.save()

    def is_new_version(self):
        """
        节点是否是增加的未启动的节点
        @return:
        """
        return not self.running_version

    def is_modify_version(self, ignore_running=False):
        """
        节点是否修改了配置，比较的标准
            1. 当前处于运行过程而与当前记录的最新版本号不同。或者由于节点使用的udf发布了新的版本，则当前节点为修改状态
            2. 节点运行过程中的上游连线发生增减(该情况不存在，因为运行节点及运行节点间连线不允许修改)
        @param ignore_running: 是否忽略节点的的运行状态比对是否是新版本
        @return:
        """
        # 检查标准1
        if (ignore_running or self.status in [FlowInfo.STATUS.RUNNING]) and (
            self.running_version != self.latest_version
            or (
                self.node_type
                in [
                    NodeTypes.STREAM,
                    NodeTypes.BATCH,
                    NodeTypes.BATCHV2,
                ]
                and NodeUtils.is_modify_dp_node_with_udf(self.processing_id)
            )
        ):
            return True
        # 检查标准2
        # 一种比较复杂的判断方式：
        #   通过获取当前节点运行版本对应的所有 version_ids，关联 reversion 获取当时的连线与现在的进行对比
        # 采用一种简单的方式：
        #   获取节点的 from_result_table_ids，与当前上游进行对比(对比方式为 from_result_table_ids 必须各属于上游节点)
        #   若 from_result_table_ids 多了表明删除了上游连线，少了表明增加了上游连线
        if ignore_running or self.status in [FlowInfo.STATUS.RUNNING]:
            from_nodes = self.get_from_nodes_handler()
            # 一条连线只能有一个输入
            len_from_nodes = len(from_nodes)
            if (
                self.node_type in NodeTypes.SOURCE_CATEGORY
                and self.node_type not in NodeTypes.NEW_FROM_RESULT_TABLE_IDS_CATEGORY
            ):
                len_from_nodes = len_from_nodes + 1
            if len(self.from_result_table_ids) != len_from_nodes:
                return True
            current_from_node_rts = []
            for from_node in from_nodes:
                current_from_node_rts.append(from_node.result_table_ids)
            for current_from_node_rt in current_from_node_rts:
                # 小于等于0是考虑上游节点可能是多输出，当前节点又是多输入
                if len(set(current_from_node_rt) & set(self.from_result_table_ids)) != 1:
                    return True
        return False

    def has_modify(self):
        """
        节点是否在运行状态下，修改了配置未重启
        """
        if self.status in [FlowInfo.STATUS.RUNNING] and self.running_revision_version is not None:
            # 比较运行版本和当前节点版本对应的 reversion_version 记录的 object_id 是否一致
            # 查看包含udf的node节点是否改为修改状态

            return self.running_revision_version != self.revision_version or (
                self.node_type
                in [
                    NodeTypes.STREAM,
                    NodeTypes.BATCH,
                    NodeTypes.BATCHV2,
                ]
                and NodeUtils.is_modify_dp_node_with_udf(self.processing_id)
            )
        return False

    def _clear_node(self):
        self.node_id = None
        self._node_info = None

    def _reset_node(self):
        self._node_info = None

    @staticmethod
    def _gene_default_link_id(node_id=None):
        """
        生成连线的端点id
        据悉，对于APP画布而言，初始化节点的时候node_id还没生成，需要一个id，之后有node_id后就不用这个id
        对于FlowAPI，可以不用生成这个id，这里为保证结构一致，也返回一个id，应尽量保持不冲突
        @param node_id:
        @return:
        """
        if node_id:
            return "ch_%s" % node_id
        else:
            return "ch_r%s" % str(random.randint(0, 9999)).zfill(4)

    def get_downstream_nodes(self, filter_node_types=None):
        """
        获取节点的所有下游节点
        @param filter_node_types: 指定特定节点类型
        @return:
        """
        arr = [self]
        nodes = {self.node_id: self}
        # 这里采用循环，主要是为了同时避免环的存在导致死循环，不建议使用递归
        arr_num = len(arr)
        while arr_num > 0:
            _n = arr.pop()
            for _to_node in _n.get_to_nodes_handler():
                if _to_node.node_id in nodes:
                    continue
                if filter_node_types and _to_node.node_type not in filter_node_types:
                    continue
                nodes[_to_node.node_id] = _to_node
                arr.append(_to_node)
        return list(nodes.values())

    def validate_upstream_config(self):
        from_result_table_ids = self.from_result_table_ids
        from_nodes = self.get_from_nodes_handler()
        if len(from_result_table_ids) < len(from_nodes):
            return False
        _from_nodes_rts = []
        for from_node in from_nodes:
            _from_node_rts = from_node.result_table_ids
            _from_nodes_rts.extend(_from_node_rts)
            # 若上游节点的 RT 不在当前节点的 from_result_table_ids 中, 增加了连线未保存
            if len(set(_from_node_rts) & set(from_result_table_ids)) == 0:
                return False
        # 若当前节点存在上游节点没有的 result_table_id, 删除了连线未保存
        if set(from_result_table_ids) - set(_from_nodes_rts):
            return False
        return True

    def get_job(self):
        """
        获取节点对应的job object
        :return:
        """
        # 一个节点对应的job，对应多个必定有问题
        _job_set = self.node_info.job_set
        if _job_set:
            if len(_job_set) > 1:
                raise Errors.NodeError(_("节点不可对应多个任务."))
            return _job_set[0]
        else:
            return None

    # ------------------------------- 通用类属性函数 ------------------------------- #

    @property
    def node_info(self):
        if self._node_info is None:
            self._node_info = FlowNodeInfo.objects.get(node_id=self.node_id)

        return self._node_info

    @property
    def flow_id(self):
        return self.node_info.flow_id

    @property
    def manage_tags(self):
        if self._manage_tags is None:
            manage_tags = ProjectHelper.get_project(self.project_id).get("tags").get("manage", {})
            self._manage_tags = manage_tags
        return self._manage_tags

    @property
    def geog_area_codes(self):
        tag_codes = [_info["code"] for _info in self.manage_tags.get("geog_area", [])]
        if not tag_codes:
            raise Errors.NodeError(_("当前任务所属项目地域信息不存在"))
        if len(tag_codes) > 1:
            raise Errors.NodeError(_("当前任务所属项目地域信息不唯一"))
        return tag_codes

    @property
    def project_id(self):
        return self.flow.project_id

    @property
    def flow(self):
        return self.node_info.flow

    @property
    def from_node_ids(self):
        return self.node_info.from_node_ids

    @property
    def to_node_ids(self):
        return self.node_info.to_node_ids

    @property
    def result_table_ids(self):
        return self.node_info.get_tail_rts()

    @property
    def from_nodes_info(self):
        """
        返回当前节点所有上游节点连线信息
        @return:
            [
                {
                    'id': 3,
                    'from_result_table_ids': ['1_xxxx']
                }
            ]
        """
        _from_nodes_info = []
        from_result_table_ids = set(self.from_result_table_ids)
        for from_node in self.get_from_nodes_handler():
            _from_nodes_info.append(
                {
                    "id": from_node.node_id,
                    "from_result_table_ids": list(set(from_node.result_table_ids) & from_result_table_ids),
                }
            )
        return _from_nodes_info

    @property
    def from_result_table_ids(self):
        """
        获取当前节点的源rt列表
        @return:
        """
        return NodeUtils.get_from_result_table_ids(self.node_type, self.get_config(False))

    @property
    def head_result_table_id(self):
        """
        获取当前节点的头部RT
        @return:
        """
        return self.node_info.get_head_rts()[0]

    @property
    def name(self):
        """
        默认取节点中文名称
        """
        _config = self.get_config(False)
        if "name" in _config:
            return _config["name"]

        return self.node_info.node_name

    @property
    def latest_version(self):
        """
        Flow 生成的特定版本号
        @return:
        """
        return self.node_info.latest_version

    @property
    def running_version(self):
        """
        Flow 生成的特定版本号
        @return:
        """
        return self.node_info.running_version

    @property
    def revision_version(self):
        """
        返回 revision 记录的版本号，即主键ID
        @return:
        """
        return self.node_info.revision_version

    @property
    def running_revision_version(self):
        """
        返回 revision 记录的版本号，即主键ID
        @return:
        """
        return self.node_info.running_revision_version

    @property
    def status(self):
        return self.node_info.status

    @property
    def display_status(self):
        if self.status == FlowInfo.STATUS.FAILURE or not self.validate_upstream_config():
            return FlowInfo.STATUS.FAILURE
        return self.status

    @property
    def is_support_debug(self):
        """
        是否支持调试
        """
        return FlowNodeInstance.get_node_instance(self.node_type)["support_debug"]

    @property
    def frontend_info(self):
        return self.node_info.get_frontend_info()

    @frontend_info.setter
    @auto_meta_sync(using="default")
    def frontend_info(self, info):
        """
        更新节点前端配置

        @param {Dict} info 新的前端配置
        """
        self.node_info.frontend_info = json.dumps(info)
        self.node_info.save(update_fields=["frontend_info"])

    @property
    def is_batch_node(self):
        """
        特指可拥有默认 HDFS 存储的节点，不包括离线数据源
        @return:
        """
        return self.node_type in [
            NodeTypes.BATCH,
            NodeTypes.BATCHV2,
            NodeTypes.MODEL_APP,
            NodeTypes.DATA_MODEL_BATCH_INDICATOR,
        ] or (
            self.node_type
            in [
                NodeTypes.MODEL,
                NodeTypes.PROCESS_MODEL,
                NodeTypes.MODEL_TS_CUSTOM,
            ]
            and self.serving_mode == "offline"
        )

    @property
    def channel_mode(self):
        """
        获取当前节点要运行起来所需要的 channel_mode
        @return:
        """
        channel_mode = "stream"
        # 当前 channel mode 用于 databus 区分是实时还是离线类型，用于判断是否创建对应的 DT(HDFS -> kafka)
        # 可认为离线 TDW 节点是需要创建这个 DT 的，因此这里若上游为离线 TDW 节点，传的是 stream
        if self.node_type == NodeTypes.BATCH_SOURCE or self.is_batch_node:
            channel_mode = "batch"
        elif self.node_type in NodeTypes.SOURCE_CATEGORY:
            # 对于数据源而言，channel_mode 设置为其它
            channel_mode = "other"
        return channel_mode

    @property
    def is_belong_to_topology_job(self):
        """
        标识当前节点若启动时，是否属于拓扑作业
        @return:
        """
        return False

    @property
    def count_freq(self):
        """
        当前节点统计频率，默认为 None
        @return:
        """
        return None

    @property
    def freq_seconds(self):
        """
        当前节点按秒的统计频率，默认为 None
        @return:
        """
        return None

    @property
    def delay_time(self):
        return 0
