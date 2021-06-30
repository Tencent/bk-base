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
import zlib
from datetime import datetime, timedelta

import reversion
from common.base_utils import model_to_dict
from common.fields import TransCharField, TransTextField
from common.local import get_local_param, get_request_username, set_local_param
from common.transaction import meta_sync_register
from django.contrib.contenttypes.models import ContentType
from django.core import serializers
from django.db import models
from django.db.models import Q
from django.utils import translation
from django.utils.translation import ugettext
from django.utils.translation import ugettext_lazy as _
from django.utils.translation import ugettext_noop
from rest_framework import serializers as drf_serializers
from reversion.models import Version

from dataflow.flow.api_models import ResultTable
from dataflow.flow.exceptions import DebuggerNodeSerializeError, FlowLockError, VersionControlError
from dataflow.flow.node_types import NodeTypes
from dataflow.flow.settings import LOG_SPLIT_CHAR
from dataflow.flow.utils.language import Bilingual
from dataflow.flow.utils.version import gene_version
from dataflow.pizza_settings import RELEASE_ENV, RELEASE_ENV_TGDP, RUN_VERSION
from dataflow.shared.datamanage.datamanage_helper import DatamanageHelper
from dataflow.shared.language import translate_based_on_language_config as translate
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.model.model_helper import ModelHelper
from dataflow.shared.model.process_model_helper import ProcessModelHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class BaseManager(models.Manager):
    """
    对于常用记录字段作为基类进行管理
    """

    def delete(self):
        self.get_queryset().update(active=False)

    def get_queryset(self):
        return super(BaseManager, self).get_queryset().filter(active=True)


class BaseModel(models.Model):
    """
    对于常用记录字段作为基类进行管理
    """

    objects = BaseManager()
    origin_objects = models.Manager()

    active = models.BooleanField(_("是否有效"), default=True)

    class Meta:
        abstract = True

    def delete(self, *args, **kwargs):
        """
        通过标记删除字段 is_deleted 来软删除
        """
        if self.active:
            self.active = False
            self.save()


class FlowManager(BaseManager):
    def create(self, *args, **kwargs):
        if "created_by" in kwargs:
            kwargs["updated_by"] = kwargs["created_by"]
            kwargs["locked_by"] = kwargs["created_by"]
        return super(FlowManager, self).create(*args, **kwargs)


class FlowInfo(BaseModel):
    class STATUS(object):
        NO_START = "no-start"
        RUNNING = "running"
        STARTING = "starting"
        FAILURE = "failure"
        STOPPING = "stopping"
        WARNING = "warning"

    # 表明当前flow是否处于操作状态
    PROCESS_STATUS = [STATUS.RUNNING, STATUS.STARTING, STATUS.STOPPING]

    STATUS_CHOICES = (
        (STATUS.NO_START, _("未启动")),
        (STATUS.RUNNING, _("运行中")),
        (STATUS.STARTING, _("启动中")),
        (STATUS.FAILURE, _("操作失败")),
        (STATUS.STOPPING, _("停止中")),
    )

    flow_id = models.AutoField(primary_key=True)
    flow_name = models.CharField(_("作业名称"), max_length=255)
    project_id = models.IntegerField(_("项目ID"))
    status = models.CharField(
        _("作业状态"),
        max_length=32,
        blank=True,
        null=True,
        choices=STATUS_CHOICES,
        default="no-start",
    )
    is_locked = models.IntegerField("作业锁", default=0)
    latest_version = models.CharField("作业版本", max_length=255, blank=True, null=True)
    custom_calculate_id = models.CharField("当前flow的重算id", max_length=255, default=None, blank=True, null=True)
    bk_app_code = models.CharField("通过哪个APP创建记录", max_length=255, default="dataweb")

    tdw_conf = models.TextField(_("tdw任务配置"), default="{}")

    created_by = models.CharField(_("创建人"), max_length=128, null=True)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    locked_by = models.CharField(_("锁定人"), max_length=128, null=True)
    locked_at = models.DateTimeField(_("锁定时间"), null=True)
    locked_description = TransTextField(_("任务锁描述"), null=True)
    updated_by = models.CharField(_("更新人"), max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(_("更新时间"), blank=True, null=True, auto_now=True)
    description = models.TextField(_("作业描述"), blank=True)

    objects = FlowManager()

    def check_locked(self):
        if self.is_locked:
            raise FlowLockError(
                _("%(locked_by)s %(description)s，已被锁定，不可操作，请稍候重试")
                % {
                    "locked_by": self.locked_by,
                    "description": translate(self.locked_description, "zh-cn"),
                }
            )

    def lock(self, description):
        self.check_locked()
        self.is_locked = 1
        self.locked_by = get_request_username()
        self.locked_at = datetime.now()
        self.locked_description = description
        self.save(update_fields=["is_locked", "locked_by", "locked_at", "locked_description"])

    def unlock(self):
        self.is_locked = 0
        self.save(update_fields=["is_locked"])

    def get_tdw_conf(self):
        """
        @return:
        {
            "task_info": {
                "spark": {
                    "spark_version":"xx2.3",
                }
            },
            "gaia_id":111,
            "tdwAppGroup":"g_ieg_xxx",
            "sourceServer":"127.0.0.1",
            "productId":6,
            "bgId":6
        }
        """
        tdw_conf = self.tdw_conf
        return json.loads(tdw_conf) if tdw_conf else {}

    def delete_line(self, from_node_id, to_node_id):
        delete_links = FlowLinkInfo.objects.filter(
            flow_id=self.flow_id, from_node_id=from_node_id, to_node_id=to_node_id
        )
        delete_links.delete()

    def create_line(self, from_node_id, to_node_id, frontend_info):
        _links = FlowLinkInfo.objects.filter(flow_id=self.flow_id, from_node_id=from_node_id, to_node_id=to_node_id)

        if _links.exists():
            _links.update(frontend_info=json.dumps(frontend_info))
        else:
            FlowLinkInfo.objects.create(
                flow_id=self.flow_id,
                from_node_id=from_node_id,
                to_node_id=to_node_id,
                frontend_info=json.dumps(frontend_info),
            )

    def get_components_by_version(self, version):
        try:
            components = FlowVersionLog.objects.get(flow_id=self.flow_id, flow_version=version).version_ids
        except FlowVersionLog.DoesNotExist:
            raise VersionControlError(_("任务不存在{version}版本".format(version=version)))

        component_ids = components.split(",")
        objects = Version.objects.filter(pk__in=component_ids)
        return [
            list(serializers.deserialize(_object.format, _object.serialized_data, ignorenonexistent=True))[0].object
            for _object in objects
        ]

    def list_node_versions(self):
        """
        列举当前版本，各个节点的版本号
        """
        v_object = self.version_object
        if v_object is None:
            return {}

        return v_object.list_node_versions()

    def list_running_node_versions(self):
        """
        列举运行版本，各个节点的版本号
        """
        v_object = self.running_version_object
        if v_object is None:
            return {}

        return v_object.list_node_versions()

    def partial_update(self, validated_data):
        for attr, value in list(validated_data.items()):
            setattr(self, attr, value)
        self.save()

    @property
    def nodes(self):
        return FlowNodeInfo.objects.filter(flow_id=self.flow_id)

    @property
    def flow_node_ids(self):
        nodes = self.nodes
        return [node.node_id for node in nodes]

    @property
    def links(self):
        return FlowLinkInfo.objects.filter(flow_id=self.flow_id)

    @property
    def versions(self):
        versions = FlowVersionLog.objects.filter(flow_id=self.flow_id)
        return [
            {
                "flow_id": version.flow_id,
                "version": version.flow_version,
                "created_at": str(version.created_at),
            }
            for version in versions
        ]

    @property
    def node_count(self):
        return FlowNodeInfo.objects.filter(flow_id=self.flow_id).count()

    @property
    def process_status(self):
        # 操作状态
        try:
            status = FlowExecuteLog.objects.filter(flow_id=self.flow_id).order_by("-created_at")[0].status
        except IndexError:
            # FLOW从未执行过，没有操作状态
            status = "no_status"
        return status

    @property
    def running_version_object(self):
        """
        返回运行版本对象
        """
        try:
            ins = FlowInstance.objects.get(flow_id=self.flow_id)
            return FlowVersionLog.objects.get(flow_id=self.flow_id, flow_version=ins.running_version)
        except (FlowInstance.DoesNotExist, FlowVersionLog.DoesNotExist):
            return None

    @property
    def version_object(self):
        """
        返回当前版本对象
        """
        if self.latest_version is None:
            return None

        try:
            return FlowVersionLog.objects.get(flow_id=self.flow_id, flow_version=self.latest_version)
        except FlowVersionLog.DoesNotExist:
            return None

    @property
    def instance(self):
        """
        运行实例
        """
        try:
            return FlowInstance.objects.get(flow_id=self.flow_id)
        except FlowInstance.DoesNotExist:
            pass

        return None

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_info"


@reversion.register()
class FlowLinkInfo(models.Model):
    flow_id = models.IntegerField()
    from_node_id = models.IntegerField()
    to_node_id = models.IntegerField()
    frontend_info = models.TextField(_("前端配置"), blank=True, null=True)

    def get_frontend_info(self):
        if self.frontend_info is not None:
            return json.loads(self.frontend_info)

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_link_info"
        unique_together = (("from_node_id", "to_node_id"),)


@reversion.register()
class FlowNodeInfo(models.Model):
    node_id = models.AutoField(primary_key=True)
    flow_id = models.IntegerField()
    node_name = models.CharField(_("节点名称"), max_length=255, blank=True, null=True)
    node_config = models.TextField(_("节点配置"))
    node_type = models.CharField(_("节点类型"), max_length=255)
    frontend_info = models.TextField(_("节点前端配置"), blank=True, null=True)
    status = models.CharField(_("节点状态"), max_length=9)
    latest_version = models.CharField(_("最新版本"), max_length=255)
    running_version = models.CharField(_("正在运行的版本"), max_length=255, blank=True, null=True)
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField(_("最近更新人"), max_length=255)
    updated_at = models.DateTimeField(_("更新时间"), max_length=255, auto_now=True)
    description = models.TextField(blank=True, null=True)

    def create_or_update_link(self, from_links):
        """
        更新节点连线
        """

        from_node_ids = [_link["source"]["node_id"] for _link in from_links]
        exist_links = FlowLinkInfo.objects.filter(to_node_id=self.node_id)
        delete_links = exist_links.exclude(from_node_id__in=from_node_ids)
        exist_links_from_node_ids = exist_links.values_list("from_node_id", flat=True)
        created_links = [link for link in from_links if link["source"]["node_id"] not in exist_links_from_node_ids]
        delete_links.delete()

        for _link in created_links:
            FlowLinkInfo.objects.create(
                flow_id=self.flow_id,
                to_node_id=self.node_id,
                from_node_id=_link["source"]["node_id"],
                frontend_info=json.dumps(_link),
            )

    def delete(self, *args, **kwargs):
        """
        在节点删除时移除节点连线相关、result_table_id相关、job相关信息
        """
        # 连线相关
        delete_links = FlowLinkInfo.objects.filter(Q(to_node_id=self.node_id) | Q(from_node_id=self.node_id))
        delete_links.delete()
        # result_table_id相关
        FlowNodeRelation.objects.filter(node_id=self.node_id).delete()
        # job相关
        FlowJob.objects.filter(node_id=self.node_id).delete()
        # 正式删除节点
        super(FlowNodeInfo, self).delete(*args, **kwargs)

    def _remove_exclude_rt_relation(self, result_table_ids):
        """
        将不在result_table_ids中的记录清除，防止后台做了rt更新，而flow表继续保留关系的不一致现象
        @param result_table_ids: 不可为None或空
        @return:
        """
        if result_table_ids:
            FlowNodeRelation.objects.filter(node_id=self.node_id).exclude(result_table_id__in=result_table_ids).delete()

    def update_relation(self, bk_biz_id, result_table_id, generate_type="user", is_head=True):
        flow = self.flow
        FlowNodeRelation.objects.update_or_create(
            defaults={
                "bk_biz_id": bk_biz_id,
                "project_id": flow.project_id,
                "flow_id": self.flow_id,
                "node_id": self.node_id,
                "result_table_id": result_table_id,
                "node_type": self.node_type,
                "generate_type": generate_type,
                "is_head": is_head,
            },
            node_id=self.node_id,
            result_table_id=result_table_id,
        )

    def update_as_storage(self, bk_biz_id, result_table_id):
        """
        作为存储节点，需要添加 result_table_id 与 node_id 的关系
        """
        self._remove_exclude_rt_relation([result_table_id])
        self.update_relation(bk_biz_id, result_table_id)

    def remove_as_source(self):
        """
        作为源数据节点，移除除了节点连线相关、result_table_id相关、job相关的信息
        """
        pass

    def update_as_source(self, bk_biz_id, result_table_id):
        """
        作为源数据节点，需要添加 result_table_id(即data_set_id) 与 node_id 的关系
        """
        self._remove_exclude_rt_relation([result_table_id])
        self.update_relation(bk_biz_id, result_table_id)

    def remove_as_processing(self):
        """
        作为计算节点，移除除了节点连线相关、result_table_id相关、job相关的信息
        """
        FlowNodeProcessing.objects.filter(node_id=self.node_id).delete()

    def update_as_processing(self, bk_biz_id, processing_id, processing_type, rt_info_dict):
        """
        适配一(processing_id)对多(result_table_id)
        @param bk_biz_id:
        @param processing_id:
        @param processing_type:
        @param rt_info_dict:
            {
                'rt_id1': {
                    'generate_type': 'user'/'system'
                    'is_head': True
                }
            }
        @return:
        """
        self._remove_exclude_rt_relation(list(rt_info_dict.keys()))
        for result_table_id, rt_info in list(rt_info_dict.items()):
            FlowNodeProcessing.objects.update_or_create(
                defaults={
                    "flow_id": self.flow_id,
                    "node_id": self.node_id,
                    "processing_id": processing_id,
                    "processing_type": processing_type,
                    "description": "",
                },
                node_id=self.node_id,
            )
            self.update_relation(
                bk_biz_id,
                result_table_id,
                generate_type=rt_info["generate_type"],
                is_head=rt_info["is_head"],
            )

    def get_tail_rts(self):
        """
        获取节点的输出 RT 列表
        @return {list}
        """
        rts = FlowNodeRelation.objects.filter(node_id=self.node_id, generate_type="user").only("result_table_id")
        rt_ids = [_rt.result_table_id for _rt in rts]
        return rt_ids

    def get_head_rts(self):
        """
        获取节点的头部 RT 列表
        @return {list}
        """
        rts = FlowNodeRelation.objects.filter(node_id=self.node_id, is_head=True).only("result_table_id")
        rt_ids = [_rt.result_table_id for _rt in rts]
        return rt_ids

    def reset_node_config(self, node_config):
        """
        基于远端元数据重置 node_config 信息，并返回更新后的 node_config
        @param node_config:
        @return:
        """
        # 为避免对重启操作造成性能下降，需要将结果保存于线程变量中
        result_tables_cache = get_local_param("result_tables", {})
        if self.node_type in NodeTypes.NEW_FROM_RESULT_TABLE_IDS_CATEGORY:
            for output in node_config.get("outputs", []):
                result_table_id = "{}_{}".format(output["bk_biz_id"], output["table_name"])
                o_rt = result_tables_cache.get(result_table_id, ResultTable(result_table_id))
                output["output_name"] = o_rt.result_table_name_alias
                result_tables_cache[result_table_id] = o_rt
        elif self.node_type in [
            NodeTypes.PROCESS_MODEL,
            NodeTypes.MODEL_TS_CUSTOM,
        ]:
            result_table_id = "{}_{}".format(
                node_config["bk_biz_id"],
                node_config["output_config"]["output_node"]["table_name"],
            )
            o_rt = result_tables_cache.get(result_table_id, ResultTable(result_table_id))
            node_config["output_config"]["output_node"]["table_zh_name"] = o_rt.result_table_name_alias
            result_tables_cache[result_table_id] = o_rt
        elif "output_name" in node_config:
            # TDW 存储节点较特殊
            if self.node_type == NodeTypes.TDW_STORAGE:
                result_table_id = node_config["result_table_id"]
            else:
                result_table_id = "{}_{}".format(
                    node_config["bk_biz_id"],
                    node_config["table_name"],
                )
            o_rt = result_tables_cache.get(result_table_id, ResultTable(result_table_id))
            node_config["output_name"] = o_rt.result_table_name_alias
            result_tables_cache[result_table_id] = o_rt
        set_local_param("result_tables", result_tables_cache)
        return node_config

    def load_config(self, loading_latest=True, loading_concurrently=False):
        """
        获取节点配置信息，注意，若获取一些信息无需设置 loading_latest，可加快访问速度
        @param loading_latest: 是否从模块之外获取最新的信息
        @param loading_concurrently: 是否采用并发调用，需 loading_latest 设置为 True
        @return:
        """
        node_config = json.loads(self.node_config)
        # 存储节点配置更新为从 Storekit 获取
        if self.node_type in NodeTypes.STORAGE_CATEGORY:
            cluster_type = NodeTypes.STORAGE_NODES_MAPPING[self.node_type]
            result_table_id = FlowNodeRelation.objects.filter(node_id=self.node_id)[0].result_table_id
            current_storage_configs = StorekitHelper.get_physical_table(result_table_id, cluster_type)
            if current_storage_configs and current_storage_configs["config"]:
                expires = current_storage_configs["expires"]
                # 去掉后面的 d
                if str(expires) == "-1":
                    expires_int_value = -1
                else:
                    expires_int_value = expires[: len(expires) - 1]
                node_config["expires"] = int(expires_int_value)
                node_config["cluster"] = current_storage_configs["cluster_name"]
                node_config.update(current_storage_configs["config"])

        func_info_cache = get_local_param("loading_node_config_concurrently", [])
        if loading_latest:
            if self.node_type == NodeTypes.MODEL:
                # 对于模型节点，每次加载时基于 modelflow api 获取最新 model_version_id
                model_db_obj = FlowNodeRelation.objects.get(node_id=self.node_id, generate_type="user")
                model_version_id = ModelHelper.get_processing(model_db_obj.result_table_id)["model_version_id"]
                node_config["model_version_id"] = model_version_id
            elif self.node_type in [
                NodeTypes.PROCESS_MODEL,
                NodeTypes.MODEL_TS_CUSTOM,
            ]:
                # 对于流程化建模节点，每次加载时基于 process api 获取最新 model_release_id
                model_db_obj = FlowNodeRelation.objects.get(node_id=self.node_id, generate_type="user")
                model_release_id = ProcessModelHelper.get_processing(model_db_obj.result_table_id)["model_release_id"]
                node_config["model_release_id"] = model_release_id
            elif self.node_type in [NodeTypes.DATA_MODEL_APP]:
                dedicated_config = node_config["dedicated_config"]
                if "model_instance_id" in dedicated_config and dedicated_config["model_instance_id"]:
                    model_app_data = DatamanageHelper.get_data_model_instance(
                        {"model_instance_id": dedicated_config["model_instance_id"]}
                    )
                    node_config["dedicated_config"].update(model_app_data)
            elif self.node_type in [
                NodeTypes.DATA_MODEL_STREAM_INDICATOR,
                NodeTypes.DATA_MODEL_BATCH_INDICATOR,
            ]:
                dedicated_config = node_config["dedicated_config"]
                if (
                    "model_instance_id" in dedicated_config
                    and dedicated_config["model_instance_id"]
                    and "result_table_id" in dedicated_config
                    and dedicated_config["result_table_id"]
                ):
                    model_app_indicator_data = DatamanageHelper.get_data_model_indicator(
                        {
                            "model_instance_id": dedicated_config["model_instance_id"],
                            "result_table_id": dedicated_config["result_table_id"],
                        }
                    )
                    node_config["dedicated_config"].update(model_app_indicator_data)
            elif self.node_type in NodeTypes.CORRECT_SQL_CATEGORY:
                # 增加数据修正数据
                if "correct_config_id" in node_config and node_config["correct_config_id"]:
                    # correct_config_id 配置不为空
                    correct_data = DatamanageHelper.get_data_correct(
                        {"correct_config_id": node_config["correct_config_id"]}
                    )
                    # 返回给前端的配置, 增加数据修正相关
                    node_config["data_correct"] = {
                        "is_open_correct": node_config["is_open_correct"],
                        "correct_configs": correct_data["correct_configs"],
                    }
            if self.node_type in NodeTypes.PROCESSING_CATEGORY:
                # 从 meta 获取结果表中文名称返回给前端或启动时保存操作传值
                if loading_concurrently:
                    func_info_cache.append(
                        {
                            "node_id": self.node_id,
                            "func_info": [
                                self.reset_node_config,
                                {"node_config": node_config},
                            ],
                        }
                    )
                else:
                    self.reset_node_config(node_config)
        set_local_param("loading_node_config_concurrently", func_info_cache)
        return node_config

    def get_frontend_info(self):
        if self.frontend_info is not None:
            return json.loads(self.frontend_info)
        else:
            return None

    @property
    def job_set(self):
        return FlowJob.objects.filter(node_id=self.node_id)

    @property
    def from_node_ids(self):
        return FlowLinkInfo.objects.filter(to_node_id=self.node_id).values_list("from_node_id", flat=True)

    @property
    def to_node_ids(self):
        return FlowLinkInfo.objects.filter(from_node_id=self.node_id).values_list("to_node_id", flat=True)

    @property
    def flow(self):
        return FlowInfo.objects.get(flow_id=self.flow_id)

    @property
    def revision_version(self):
        """
        节点最新版本号，来源于 reversion 记录
        """
        m_node_version = self.flow.list_node_versions()
        if self.node_id in m_node_version:
            return m_node_version[self.node_id]
        else:
            return None

    @property
    def running_revision_version(self):
        """
        节点运行版本号，目前可以通过 Flow、FlowInstance 关联关系找到
        注意：
            节点中的running_version目前没用，但是每次节点启动成功后，都会将该值置为当前节点的编辑版本(latest_version)
            后期版本改造时，要考虑下节点running_version的必要性，目前来看，可以用flow的running_version作为节点的running_version
        """
        m_node_version = self.flow.list_running_node_versions()
        if self.node_id in m_node_version:
            return m_node_version[self.node_id]
        else:
            return None

    @staticmethod
    def gene_version():
        """
        生成唯一版本号
        """
        return gene_version()

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_node_info"
        verbose_name = _("【节点配置】节点配置表")
        verbose_name_plural = _("【节点配置】节点配置表")


class FlowNodeRelation(models.Model):
    id = models.AutoField(primary_key=True)
    bk_biz_id = models.IntegerField(11)
    project_id = models.IntegerField(11)
    flow_id = models.IntegerField(11)
    node_id = models.IntegerField(11)
    result_table_id = models.CharField(_("result_table_id, data_id节点的输入数据"), max_length=255)
    node_type = models.CharField(_("节点类型"), max_length=255)
    generate_type = models.CharField(_("结果表生成类型 user/system"), max_length=32, default="user", null=False)
    is_head = models.BooleanField(_("是否是节点的头部RT"), default=True)

    def to_dict(self):
        return {
            "id": self.id,
            "bk_biz_id": self.bk_biz_id,
            "project_id": self.project_id,
            "flow_id": self.flow_id,
            "node_id": self.node_id,
            "result_table_id": self.result_table_id,
            "node_type": self.node_type,
            "generate_type": self.generate_type,
            "is_head": self.is_head,
        }

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_node_relation"
        unique_together = (
            "node_id",
            "result_table_id",
        )
        verbose_name = _("【FLOW配置】node_id 与 result_table_id 关系")
        verbose_name_plural = _("【FLOW配置】node_id 与 result_table_id 关系")


class FlowJob(models.Model):
    id = models.AutoField(primary_key=True)
    flow_id = models.IntegerField(11)
    node_id = models.IntegerField(11)
    job_id = models.CharField(_("job的id"), max_length=255)
    job_type = models.CharField(_("取值来源于processing_type_config"), max_length=255)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_job"
        verbose_name = _("【FLOW配置】flow_id, node_id 与 job_id 关系")
        verbose_name_plural = _("【FLOW配置】flow_id, node_id 与 job_id 关系")


class FlowNodeProcessing(models.Model):
    id = models.AutoField(primary_key=True)
    flow_id = models.IntegerField()
    node_id = models.IntegerField()
    processing_id = models.CharField(_("processing id"), max_length=255)
    processing_type = models.CharField(_("包括实时作业stream、离线作业batch、模型应用model_app"), max_length=255)
    description = models.TextField(blank=True, null=True)

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_processing"
        verbose_name = _("【FLOW配置】processing 与 flow 源节点关系")
        verbose_name_plural = _("【FLOW配置】processing_id 与 flow 源节点关系")


# Flow 部署相关 Models
class FlowExecuteLog(models.Model):
    class STATUS(object):
        SUCCESS = "success"
        FAILURE = "failure"
        RUNNING = "running"
        PENDING = "pending"

        APPLYING = "applying"
        READY = "ready"
        CANCELLED = "cancelled"
        TERMINATED = "terminated"
        FINISHED = "finished"

    # 作业状态可被展示为流水
    DEPLOY_STATUS = [
        STATUS.SUCCESS,
        STATUS.FAILURE,
        STATUS.RUNNING,
        STATUS.PENDING,
        STATUS.TERMINATED,
        STATUS.FINISHED,
    ]
    # 作业最终状态
    FINAL_STATUS = [
        STATUS.SUCCESS,
        STATUS.FAILURE,
        STATUS.TERMINATED,
        STATUS.CANCELLED,
        STATUS.FINISHED,
    ]
    # 作业处于正在执行状态
    ACTIVE_STATUS = [STATUS.RUNNING, STATUS.PENDING]
    # 作业部署前的状态
    DEPLOY_BEFORE_STATUS = [STATUS.APPLYING, STATUS.READY]

    STATUS_CHOICES = (
        (STATUS.SUCCESS, _("执行成功")),
        (STATUS.FAILURE, _("执行失败")),
        (STATUS.RUNNING, _("运行中")),
        (STATUS.PENDING, _("等待执行")),
        (STATUS.APPLYING, _("申请中")),
        (STATUS.READY, _("等待启动")),
        (STATUS.CANCELLED, _("撤销申请")),
        (STATUS.TERMINATED, _("终止执行")),
    )

    class ACTION_TYPES(object):
        START = "start"
        STOP = "stop"
        RESTART = "restart"
        CUSTOM_CALCULATE = "custom_calculate"

    ACTION_TYPES_CHOICES = (
        (ACTION_TYPES.START, _("任务启动")),
        (ACTION_TYPES.STOP, _("任务停止")),
        (ACTION_TYPES.RESTART, _("任务重启")),
        (ACTION_TYPES.CUSTOM_CALCULATE, _("启动补算")),
    )

    # 任务超时时间，60 分钟
    TIMEOUT_OUT_MINUTES = 60

    id = models.AutoField(primary_key=True)
    flow_id = models.IntegerField()
    action = models.CharField(_("执行类型"), max_length=32, choices=ACTION_TYPES_CHOICES)
    status = models.CharField(_("执行结果"), max_length=32, choices=STATUS_CHOICES, default=STATUS.PENDING)
    logs_zh = models.TextField(_("执行日志"), null=True)
    logs_en = models.TextField(_("执行日志_en"), null=True)
    start_time = models.DateTimeField(_("开始时间"), null=True)
    end_time = models.DateTimeField(_("结束时间"), null=True)
    context = models.TextField(_("全局上下文，参数，json格式"), null=True)
    version = models.CharField("作业版本", max_length=255, null=True)
    created_by = models.CharField(_("创建人"), max_length=128)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    description = models.TextField(_("备注信息"), blank=True)

    def set_status(self, status):
        """
        设置任务状态，自动记录启停时间
        """
        self.status = status
        if self.status == self.STATUS.RUNNING:
            self.start_time = datetime.now()
        elif self.status in self.FINAL_STATUS:
            self.end_time = datetime.now()

        self.save(update_fields=["status", "start_time", "end_time"])

    def add_log(self, msg, level="INFO", time=None, save_db=True, progress=0.0):
        """
        存入 logs 字段的日志格式为 {level}|{time}|{msg}
        @param {Bilingual or String} msg 日志信息
        @param {String} level 日志级别，可选有 INFO、WARNING、ERROR
        @param {String} time 日志时间，格式 %Y-%m-%d %H:%M:%S
        @param {Boolean} save_db 日志是否更新至 DB
        @param {Float} progress 当前日志所在流程整体进度
        """
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # msg为双语类型
        if isinstance(msg, Bilingual):
            # 将 msg 中特殊字符 | 转成不可见字符
            _log = "{level}|{time}|{msg}|{progress}".format(
                level=level,
                time=_time,
                msg=msg.zh.replace("|", LOG_SPLIT_CHAR),
                progress=progress,
            )
            _log_en = "{level}|{time}|{msg}|{progress}".format(
                level=level,
                time=_time,
                msg=msg.en.replace("|", LOG_SPLIT_CHAR),
                progress=progress,
            )
        else:
            msg = msg.replace("\n", " ")
            _log = "{level}|{time}|{msg}|{progress}".format(
                level=level,
                time=_time,
                msg=msg.replace("|", LOG_SPLIT_CHAR),
                progress=progress,
            )
            _log_en = "{level}|{time}|{msg}|{progress}".format(
                level=level,
                time=_time,
                msg=ugettext(msg).replace("|", LOG_SPLIT_CHAR),
                progress=progress,
            )

        print(_log)
        print(_log_en)

        if self.logs_zh is None:
            self.logs_zh = _log
            self.logs_en = _log_en
        else:
            self.logs_zh = "{prev}\n{new}".format(prev=self.logs_zh, new=_log)
            self.logs_en = "{prev}\n{new}".format(prev=self.logs_en, new=_log_en)
        if save_db:
            self.save()

    def apply_compression(self):
        """
        判断当前日志写入是否采用压缩方式写入
        @return:
        """
        return self.action == self.ACTION_TYPES.CUSTOM_CALCULATE

    def _compress_logs(self):
        # 将二进制字符串以 latin1 解码后存入数据库
        if self.logs_zh:
            self.logs_zh = zlib.compress(self.logs_zh).decode("latin1")
        if self.logs_en:
            self.logs_en = zlib.compress(self.logs_en).decode("latin1")

    def _decompress_logs(self):
        if self.logs_zh:
            self.logs_zh = zlib.decompress(self.logs_zh.encode("latin1"))
        if self.logs_en:
            self.logs_en = zlib.decompress(self.logs_en.encode("latin1"))

    def add_logs(self, data):
        if not data:
            return
        if self.apply_compression():
            self._decompress_logs()
        for _log in data:
            # 避免每行日志都写 DB
            _log["save_db"] = False
            self.add_log(**_log)
        if self.apply_compression():
            self._compress_logs()
        self.save()

    def get_logs(self):
        """
        获取日志列表
        """
        cleaned_logs = []
        if translation.get_language() == "en":
            log_lan = self.logs_en
        else:
            log_lan = self.logs_zh
        if self.apply_compression():
            try:
                log_lan = zlib.decompress(log_lan.encode("latin1"))
            except Exception:
                log_lan = None

        logs = [] if log_lan is None else log_lan.split("\n")
        log_type_num = 4
        for _log in logs:
            _log_parts = _log.split("|", log_type_num - 1)
            if len(_log_parts) < 3:
                # 初始版本的日志分割数为3
                logger.warning("日志格式({})错误: {}".format(self.id, _log))
                continue
            for i in range(log_type_num - len(_log_parts)):
                # 兼容旧的日志解析格式，不够补齐 None
                _log_parts.append(None)
            try:
                progress = "%.1f" % float(_log_parts[3]) if _log_parts[3] else None
            except Exception as e:
                logger.exception(e)
                progress = None
            # 取出时，将不可见字符转为 |
            cleaned_logs.append(
                {
                    "level": _log_parts[0],
                    "time": _log_parts[1],
                    "message": _log_parts[2].replace(LOG_SPLIT_CHAR, "|"),
                    "progress": progress,
                }
            )

        return cleaned_logs

    def get_progress(self):
        _context = self.get_context()
        if _context:
            # 返回 None 前端将不显示进度信息
            return "%.1f" % float(_context.get("progress")) if _context.get("progress") is not None else None
        return None

    def confirm_timeout(self):
        """
        确认任务是否超时，若超时，自动结束任务
        """
        if self.start_time is not None and self.end_time is None:
            if datetime.now() - self.start_time > timedelta(minutes=self.TIMEOUT_OUT_MINUTES):
                self.add_log(ugettext_noop("时间过长，强制中止部署流程"), level="ERROR", progress=100.0)
                self.set_status(self.STATUS.FAILURE)
                return True

        return False

    def get_context(self):
        """
        获取执行所需上下文
        """
        if self.context is None:
            return None

        _context = json.loads(self.context)
        return _context

    def save_context(self, context):
        if context is None:
            context = {}
        self.context = json.dumps(context)
        self.save(update_fields=["context"])

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_execute_log"
        verbose_name = _("【启停记录】DataFlow 启停记录")
        verbose_name_plural = _("【启停记录】DataFlow 启停记录")


class FlowVersionLog(models.Model):
    id = models.AutoField(primary_key=True)
    flow_id = models.IntegerField("flow的id")
    flow_version = models.CharField("flow版本号", max_length=255)
    version_ids = models.TextField("关联的节点&连线 RevisionID")
    created_by = models.CharField(_("创建人"), max_length=128)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    description = models.TextField("备注信息")

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_version_log"

    def __unicode__(self):
        return "[{}-{}] {}".format(self.flow_id, self.flow_version, self.components)

    @classmethod
    def list_component_ids(cls, flow_id):
        try:
            v = cls.objects.filter(flow_id=flow_id).order_by("-pk")[0]
            if v:
                component_ids = v.version_ids.split(",")
            else:
                component_ids = []
            return component_ids
        except IndexError:
            return []

    @classmethod
    def list_components(cls, flow_id):
        component_ids = cls.list_component_ids(flow_id)
        versions = Version.objects.filter(pk__in=component_ids)
        components = {str(version.id): (version.object_id, version.object_repr) for version in versions}
        return components

    @classmethod
    def save_component(cls, components, flow_id, delete=False):
        current_components = cls.list_components(flow_id)
        current_component_ids = cls.list_component_ids(flow_id)

        for component in components:
            component_id = str(component[0])
            component_value = component[1]
            is_new_component = True
            for _k, _v in list(current_components.items()):
                if _v == component_value:
                    current_component_ids.remove(_k)
                    is_new_component = False
                    if not delete:
                        current_component_ids.append(component_id)
                    break
            if is_new_component:
                current_component_ids.append(component_id)

        if components:
            version = cls.gene_version()
            cls.objects.create(
                flow_id=flow_id,
                flow_version=version,
                version_ids=",".join(current_component_ids),
                description="",
            )
            return version
        return None

    @classmethod
    def is_changed_version(cls, current_version, last_version, fields):
        flag = False
        for field in fields:
            if current_version.field_dict[field] != last_version.field_dict[field]:
                flag = True
        return flag

    @classmethod
    def version_control_save(cls, _object, fk, fields=None, delete=False):
        version = Version.objects.get_for_object(_object)
        if not version:
            raise VersionControlError(_("版本记录错误，请先记录Version"))
        if len(version) == 1:
            current_version = version[0]
        else:
            # 存在旧的版本，比较是否有变更
            current_version = version[0]
            last_version = version[1]
            if not cls.is_changed_version(current_version, last_version, fields):
                return
        components = [
            (
                current_version.id,
                (current_version.object_id, current_version.object_repr),
            )
        ]
        return cls.save_component(components, fk, delete)

    @classmethod
    def version_control_save_batch(cls, _objects, fk, delete=False):
        try:
            versions = [Version.objects.get_for_object(_object)[0] for _object in _objects]
        except Exception:
            raise VersionControlError(_("版本记录错误，请先记录Version"))
        else:
            components = [(version.id, (version.object_id, version.object_repr)) for version in versions]
        return cls.save_component(components, fk, delete)

    def list_node_versions(self):
        """
        在具体的 Flow 版本，节点对应的版本
        """
        node_type = ContentType.objects.get_for_model(FlowNodeInfo)
        if self.component_ids:
            versions = Version.objects.filter(pk__in=self.component_ids, content_type=node_type).only("id", "object_id")
        else:
            versions = []
        return {int(_v.object_id): _v.id for _v in versions}

    @property
    def component_ids(self):
        """
        获取节点对象（包括 FlowNode、FlowLink）版本ID
        """
        return [_c for _c in self.version_ids.split(",") if _c != ""]

    @staticmethod
    def gene_version():
        """
        生成唯一版本号
        """
        return gene_version()


class FlowUser(models.Model):
    username = models.CharField("username", max_length=100, unique=True)
    USERNAME_FIELD = "username"
    REQUIRED_FIELDS = []

    class Meta:
        app_label = "dataflow.flow"
        db_table = "flow_user"


class FlowInstance(models.Model):
    """
    DataFlow 运行实例，用于记录运行信息
    """

    class EXCEPTION_LEVELS(object):
        NORMAL = 0
        ABNORMAL = 1

    EXCEPTION_CHOICES = (
        (EXCEPTION_LEVELS.NORMAL, _("运行正常")),
        (EXCEPTION_LEVELS.ABNORMAL, _("运行异常")),
    )

    flow_id = models.IntegerField(primary_key=True)
    running_version = models.CharField("作业版本", max_length=255)

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_instance_version"


class FlowDebugLog(models.Model):
    """
    用于记录调试任务状态
    """

    # 状态既用于标识Flow，也用于标识结果
    class STATUS(object):
        SUCCESS = "success"
        FAILURE = "failure"
        RUNNING = "running"
        PENDING = "pending"
        TERMINATED = "terminated"

    # 流程结束状态
    DONE_STATUS_SET = (STATUS.SUCCESS, STATUS.FAILURE, STATUS.TERMINATED)

    STATUS_CHOICES = (
        (STATUS.SUCCESS, _("调试成功")),
        (STATUS.FAILURE, _("调试失败")),
        (STATUS.RUNNING, _("调试中")),
        (STATUS.PENDING, _("等待调试")),
        (STATUS.TERMINATED, _("调试中止")),
    )

    M_STATUS_CHOICES = {_s: _d for _s, _d in STATUS_CHOICES}

    id = models.AutoField(primary_key=True)
    flow_id = models.IntegerField(_("DataFlowID"))
    version = models.CharField(_("版本号"), max_length=255)

    nodes_info = models.TextField(_("节点执行信息，json格式"), null=True)
    start_time = models.DateTimeField(_("开始时间"), null=True)
    end_time = models.DateTimeField(_("结束时间"), null=True)
    status = models.CharField(_("执行结果"), max_length=32, choices=STATUS_CHOICES, default=STATUS.PENDING)
    created_by = models.CharField(_("创建人"), max_length=128)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    # 日志格式为 {level}|{time}|{msg}
    logs_zh = models.TextField(_("执行日志"), null=True)
    logs_en = models.TextField(_("执行日志_en"), null=True)
    context = models.TextField(_("全局上下文，json格式"), null=True)
    description = models.TextField(_("全局上下文，json格式"))

    def set_status(self, status):
        """
        设置状态，自动记录时间
        """
        self.status = status
        if self.status == self.STATUS.RUNNING:
            self.start_time = datetime.now()
        elif self.status in [
            self.STATUS.FAILURE,
            self.STATUS.SUCCESS,
            self.STATUS.TERMINATED,
        ]:
            self.end_time = datetime.now()

        self.save()

    def add_log(self, msg, level="INFO", time=None, stage="system"):
        """
        存入 logs 字段的日志格式为 {stage}|{level}|{time}|{msg}，为避免 msg 中存在特殊字符，如|，将 msg 放在最后面
        @param {String | Bilingual} msg 日志信息，支持双语对象
        @param {String} level 日志级别，可选有 INFO、WARNING、ERROR
        @param {String} time 日志时间，格式 %Y-%m-%d %H:%M:%S
        @param {String} stage 标识调试日志的不同阶段
        @return:
        """
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        LOG_FORMATTER = "{stage}|{level}|{time}|{msg}"
        # msg为双语类型
        if isinstance(msg, Bilingual):
            zh_msg = msg.zh.replace("\n", " ")
            en_msg = msg.en.replace("\n", " ")

            _log_zh = LOG_FORMATTER.format(level=level, time=_time, msg=zh_msg, stage=stage)
            _log_en = LOG_FORMATTER.format(level=level, time=_time, msg=en_msg, stage=stage)
        else:
            msg = msg.replace("\n", " ")

            _log_zh = LOG_FORMATTER.format(level=level, time=_time, msg=msg, stage=stage)
            _log_en = LOG_FORMATTER.format(level=level, time=_time, msg=Bilingual.do_translate(msg), stage=stage)

        print(_log_zh)
        print(_log_en)

        if self.logs_zh is None:
            self.logs_zh = _log_zh
            self.logs_en = _log_en
        else:
            self.logs_zh = "{prev}\n{new}".format(prev=self.logs_zh, new=_log_zh)
            self.logs_en = "{prev}\n{new}".format(prev=self.logs_en, new=_log_en)
        self.save(update_fields=["logs_zh", "logs_en"])

    def get_nodes_info(self):
        """
        返回节点的调试记录，默认补全字段
        """
        if self.nodes_info is None:
            return {}

        nodes_info = json.loads(self.nodes_info)
        for _node_id, _node_info in list(nodes_info.items()):
            _serializer = NodeSerializer(data=_node_info)
            if not _serializer.is_valid():
                raise DebuggerNodeSerializeError(_serializer.errors)

            nodes_info[_node_id] = _serializer.validated_data
        return nodes_info

    def save_nodes_info(self, nodes_info):
        self.nodes_info = json.dumps(nodes_info)
        self.save(update_fields=["nodes_info"])

    def get_node_info(self, node_id):
        """
        @note nodes_info 字段为JSON格式，系列化对象为字典，key为数字，查询需要统一转为字符串
        """
        node_id = str(node_id)
        _nodes_info = self.get_nodes_info()
        _node_info = _nodes_info[node_id] if node_id in _nodes_info else {}

        serializer = NodeSerializer(data=_node_info)
        if not serializer.is_valid():
            raise DebuggerNodeSerializeError(serializer.errors)

        return serializer.validated_data

    def save_node_info(self, node_id, node_info):
        """
        设置节点调试信息，操作 nodes_info 字段

        @note nodes_info 字段为JSON格式，系列化对象为字典，key为数字，查询需要统一转为字符串
        @param {Int} node_id
        @param {Dict} node_info 节点调试信息，支持局部更新
        @paramExample node_info
            {status: 'pending', output: {}, context: {}}
        """
        node_id = str(node_id)

        serializer = NodeSerializer(data=node_info, partial=True)
        if not serializer.is_valid():
            raise DebuggerNodeSerializeError(serializer.errors)
        serializer.save(node_id=node_id, debugger=self)

    def get_node_debug(self, node_id):
        """
        @param node_id:
        @return:
            {
                "status": "failure",
                "status_display": "调试成功" | "调试失败" | "调试中",
                "logs": []
            }
        """
        node_debug = {}
        node_info = self.get_node_info(node_id)
        if node_info:
            node_debug = {
                "status": node_info["status"],
                "logs": self.get_logs(stage=str(node_id)),
                "status_display": FlowDebugLog.M_STATUS_CHOICES[node_info["status"]],
            }
        return node_debug

    def get_logs(self, stage=None):
        """
        获取日志列表
        """
        cleaned_logs = []
        if translation.get_language() == "en":
            log_lan = self.logs_en
        else:
            log_lan = self.logs_zh
        logs = [] if log_lan is None else log_lan.split("\n")
        for _log in logs:
            _log_parts = _log.split("|", 3)
            if stage is None or _log_parts[0] == stage:
                cleaned_logs.append(
                    {
                        "level": _log_parts[1],
                        "time": _log_parts[2],
                        "message": _log_parts[3],
                        "progress": None,
                    }
                )

        return cleaned_logs

    @property
    def status_display(self):
        return self.M_STATUS_CHOICES[self.status]

    @property
    def is_done(self):
        return self.status in self.DONE_STATUS_SET

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_debug_log"


class FlowFileUpload(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(_("文件名称 id"), max_length=255)
    suffix = models.CharField(_("文件后缀"), max_length=255)
    md5 = models.CharField(_("文件md5值"), max_length=255)
    flow_id = models.IntegerField(11)
    node_id = models.IntegerField(11, default=None)
    path = models.CharField(_("文件路径"), max_length=255)
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField(_("最近更新人"), max_length=255)
    updated_at = models.DateTimeField(_("更新时间"), max_length=255, auto_now=True)
    description = models.TextField(blank=True, null=True)

    @classmethod
    def get_uploaded_file_obj(cls, id):
        try:
            file_info_obj = cls.objects.get(id=id)
        except FlowFileUpload.DoesNotExist:
            raise Exception(_("指定文件信息不存在(id={id})".format(id=id)))
        return file_info_obj

    @classmethod
    def get_uploaded_file_info(cls, id):
        file_info_obj = cls.get_uploaded_file_obj(id)
        return model_to_dict(file_info_obj)

    @classmethod
    def update_upload_file_info(cls, id, node_id, path, username):
        cls.objects.filter(node_id=node_id).exclude(id__in=[id]).delete()
        file_info_obj = cls.get_uploaded_file_obj(id)
        file_info_obj.__dict__.update(id=id, node_id=node_id, path=path, updated_by=username)
        file_info_obj.save()

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_file_uploaded"


class NodeSerializer(drf_serializers.Serializer):
    """
    定义 nodes_info 单个节点调试信息
    """

    context = drf_serializers.DictField(default={})
    status = drf_serializers.ChoiceField(
        choices=[ch[0] for ch in FlowDebugLog.STATUS_CHOICES],
        default=FlowDebugLog.STATUS.PENDING,
    )

    def save(self, **kwargs):
        """
        将节点调试信息保存至 FlowDebugger.nodes_info 字段
        """
        debugger = kwargs["debugger"]
        node_id = kwargs["node_id"]

        nodes_info = debugger.get_nodes_info()
        if node_id not in nodes_info:
            nodes_info[node_id] = {}

        for field, value in list(self.validated_data.items()):
            nodes_info[node_id][field] = value

        debugger.save_nodes_info(nodes_info)


class FlowUdfLog(models.Model):
    """
    用于记录函数调试/部署流水
    """

    class STATUS(object):
        SUCCESS = "success"
        FAILURE = "failure"
        RUNNING = "running"
        PENDING = "pending"
        TERMINATED = "terminated"

    STATUS_CHOICES = (
        (STATUS.SUCCESS, _("调试成功")),
        (STATUS.FAILURE, _("调试失败")),
        (STATUS.RUNNING, _("调试中")),
        (STATUS.PENDING, _("等待调试")),
        (STATUS.TERMINATED, _("调试中止")),
    )

    class ACTION_TYPES(object):
        DEBUG = "start_debug"
        DEPLOY = "start_deploy"  # 暂时不用异步轮询

    ACTION_TYPES_CHOICES = (
        (ACTION_TYPES.DEBUG, _("调试函数")),
        (ACTION_TYPES.DEPLOY, _("部署函数")),
    )

    M_STATUS_CHOICES = {_s: _d for _s, _d in STATUS_CHOICES}

    id = models.AutoField(primary_key=True)
    func_name = models.CharField(_("函数名称"), max_length=255)
    action = models.CharField(_("执行类型"), max_length=32, choices=ACTION_TYPES_CHOICES)
    version = models.CharField(_("版本号"), max_length=255)

    start_time = models.DateTimeField(_("开始时间"), null=True)
    end_time = models.DateTimeField(_("结束时间"), null=True)
    status = models.CharField(_("执行结果"), max_length=32, choices=STATUS_CHOICES, default=STATUS.PENDING)
    created_by = models.CharField(_("创建人"), max_length=128)
    created_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    # 日志格式为 {level}|{time}|{msg}
    logs_zh = models.TextField(_("执行日志"), null=True)
    logs_en = models.TextField(_("执行日志_en"), null=True)
    context = models.TextField(_("全局上下文，json格式"), null=True)
    description = models.TextField(_("全局上下文，json格式"))

    def set_status(self, status):
        """
        设置状态，自动记录时间
        """
        self.status = status
        if self.status == self.STATUS.RUNNING:
            self.start_time = datetime.now()
        elif self.status in [
            self.STATUS.FAILURE,
            self.STATUS.SUCCESS,
            self.STATUS.TERMINATED,
        ]:
            self.end_time = datetime.now()

        self.save()

    def add_log(self, display_id, msg, level="INFO", time=None, stage="1/5", detail=""):
        """
        存入 logs 字段的日志格式为 {stage}|{level}|{time}|{msg}，为避免 msg 中存在特殊字符，如|，将 msg 放在最后面
        @param {String} display_id 标识一个可显示的日志批次
        @param {String | Bilingual} msg 日志信息，支持双语对象
        @param {String} level 日志级别，可选有 INFO、WARNING、ERROR
        @param {String} time 日志时间，格式 %Y-%m-%d %H:%M:%S
        @param {String} stage 进行到的阶段
        @param {String} detail 详情
        @return:
        """
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        LOG_FORMATTER = "{display_id}|{stage}|{level}|{time}|{msg}|{detail}"
        # msg为双语类型
        if isinstance(msg, Bilingual):
            zh_msg = msg.zh.replace("\n", " ")
            en_msg = msg.en.replace("\n", " ")

            detail = detail.replace("\n", LOG_SPLIT_CHAR)

            _log_zh = LOG_FORMATTER.format(
                display_id=display_id,
                stage=stage,
                level=level,
                time=_time,
                msg=zh_msg,
                detail=detail,
            )
            _log_en = LOG_FORMATTER.format(
                display_id=display_id,
                stage=stage,
                level=level,
                time=_time,
                msg=en_msg,
                detail=detail,
            )
        else:
            msg = msg.replace("\n", " ")

            _log_zh = LOG_FORMATTER.format(
                display_id=display_id,
                stage=stage,
                level=level,
                time=_time,
                msg=msg,
                detail=detail,
            )
            _log_en = LOG_FORMATTER.format(
                display_id=display_id,
                stage=stage,
                level=level,
                time=_time,
                msg=Bilingual.do_translate(msg),
                detail=detail,
            )

        print(_log_zh)
        print(_log_en)

        if self.logs_zh is None:
            self.logs_zh = _log_zh
            self.logs_en = _log_en
        else:
            self.logs_zh = "{prev}\n{new}".format(prev=self.logs_zh, new=_log_zh)
            self.logs_en = "{prev}\n{new}".format(prev=self.logs_en, new=_log_en)
        self.save(update_fields=["logs_zh", "logs_en"])

    def display_log_status(self, level):
        """
        根据 level 和当前调试任务状态展示该条日志状态
        @param level:
        @return:
        """
        if level.lower() == "error":
            return self.STATUS.FAILURE
        elif level.lower() == "warning":
            return self.STATUS.TERMINATED
        elif level.lower() == "success":
            return self.STATUS.SUCCESS
        elif self.status == self.STATUS.SUCCESS:
            return self.STATUS.SUCCESS
        else:
            return self.STATUS.RUNNING

    def get_logs(self):
        """
        获取日志列表
        """
        cleaned_logs = []
        if translation.get_language() == "en":
            log_lan = self.logs_en
        else:
            log_lan = self.logs_zh
        logs = [] if log_lan is None else log_lan.split("\n")
        for _log in logs:
            _log_parts = _log.split("|", 5)
            # 避免日志错误返回报错
            if len(_log_parts) < 6:
                logger.warning("日志格式({})错误: {}".format(self.id, _log))
                continue
            cleaned_logs.append(
                {
                    "display_id": _log_parts[0],
                    "stage": _log_parts[1],
                    "level": _log_parts[2],
                    "time": _log_parts[3],
                    "message": _log_parts[4],
                    "detail": _log_parts[5].replace(LOG_SPLIT_CHAR, "\n"),
                    "status": self.display_log_status(_log_parts[2]),
                }
            )

        return cleaned_logs

    def get_context(self):
        """
        获取执行所需上下文
        """
        if not self.context:
            return {}

        _context = json.loads(self.context)
        return _context

    def save_context(self, context):
        if context is None:
            context = {}
        self.context = json.dumps(context)
        self.save()

    @property
    def status_display(self):
        return self.M_STATUS_CHOICES[self.status]

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_flow_udf_log"


class FlowNodeInstance(models.Model):
    id = models.AutoField(primary_key=True)
    node_type_instance_name = models.CharField(_("节点实例名称"), max_length=255)
    node_type_instance_alias = TransCharField(_("节点实例中文名称"), max_length=255)
    group_type_name = models.CharField(_("所属节点类型"), max_length=255)
    group_type_alias = TransCharField(_("所属节点类型中文名称"), max_length=255)
    order = models.IntegerField(_("顺序"))
    support_debug = models.BooleanField(_("是否支持调试"), default=False)
    available = models.BooleanField(_("是否启用该节点类型实例"), default=True)
    description = TransTextField(_("备注信息"))
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField(_("最近更新人"), max_length=255)
    updated_at = models.DateTimeField(_("更新时间"), max_length=255, auto_now=True)

    # 返回给前端, 画布左侧控件的排列信息
    @classmethod
    def list_instance(cls):
        # 返回的是一个列表
        node_info_res = []
        # 获取表中的所有信息, 只查询 available=1
        instance_list = cls.objects.filter(available=1)
        classify_instance_dict = {}
        """ 先根据 group_type_name 分类
        {
            'modeling':[instance,instance],
            'processing':[],
            'source':[],
            'storage':[]
        }
        """
        # 返回存储的信息, 主要是确定参数 'support_query' True or False
        storage_conf = StorekitHelper.get_common()
        for instance in instance_list:
            group_type_name = instance.group_type_name
            if group_type_name not in classify_instance_dict:
                classify_instance_dict[group_type_name] = []
            temp_dict = model_to_dict(instance)
            if (
                NodeTypes.STORAGE_NODES_MAPPING.get(temp_dict["node_type_instance_name"])
                in storage_conf["storage_query"]
            ):
                temp_dict["support_query"] = True
            else:
                temp_dict["support_query"] = False
            classify_instance_dict[group_type_name].append(temp_dict)
        """ 兼容前端返回
        [{
            'instances':[
                {...}
            ],
            'node_type_alias':'数据处理',
            'order': 1
        }]
        """
        # 获取节点实例是否支持查询的信息, 只有能查询的节点能展示在左侧控件中
        for group_type_name in classify_instance_dict:
            # order 为 101 , 只是取百返回
            order = classify_instance_dict[group_type_name][0]["order"] / 100
            group_type_alias = classify_instance_dict[group_type_name][0]["group_type_alias"]
            item_dict = {
                "group_type_name": group_type_name,
                "group_type_alias": group_type_alias,
                "order": order,
                "instances": [],
            }
            item_dict["instances"].extend(classify_instance_dict[group_type_name])
            node_info_res.append(item_dict)
        return node_info_res

    """
    复用 FlowNodeTypeInstanceConfig 的校验代码
    """
    # 唯一的作用，兼顾老节点类型，将 stream 重名为 realtime，batch 重命名为 offline
    NODE_TYPE_MAP = {"realtime": "stream", "offline": "batch"}

    @staticmethod
    def node_type_transfer(source_node_type):
        if source_node_type == "realtime":
            return "stream"
        elif source_node_type == "offline":
            return "batch"
        else:
            return source_node_type

    # 缓存
    support_node_type_instances_cache = []

    @classmethod
    def check_node_type_instance(cls, node_type_instance_name):
        """
        基于发布版本仅开放一些节点，存储节点是否开放由 storekit 决定
        @param node_type_instance_name:
        @return:
        """
        # TODO: 可以将版本支持的节点实例配置于 DB 或集成于之后的复合 API
        allowed_node_type_instance = {
            RELEASE_ENV: [
                NodeTypes.STREAM_SOURCE,
                NodeTypes.BATCH_SOURCE,
                NodeTypes.STREAM,
                NodeTypes.BATCH,
                NodeTypes.PROCESS_MODEL,
                NodeTypes.MODEL_TS_CUSTOM,
            ],
            RELEASE_ENV_TGDP: [
                NodeTypes.STREAM_SOURCE,
                NodeTypes.BATCH_SOURCE,
                NodeTypes.KV_SOURCE,
                NodeTypes.STREAM,
                NodeTypes.BATCH,
                NodeTypes.SPLIT_KAFKA,
                NodeTypes.MERGE_KAFKA,
                NodeTypes.PROCESS_MODEL,
                NodeTypes.MODEL_TS_CUSTOM,
                NodeTypes.MODEL,
                NodeTypes.DATA_MODEL_APP,
            ],
        }
        # 若为企业版或海外版
        allowed_node = allowed_node_type_instance.get(RUN_VERSION)
        if allowed_node:
            allowed_node = [cls.NODE_TYPE_MAP.get(_n, _n) for _n in allowed_node]
            support_node_type_instances = cls.support_node_type_instances_cache
            # 首次加载才会调用 storekit
            if not support_node_type_instances:
                support_clusters = StorekitHelper.get_support_cluster_types()
                support_storage_cluster_types = list(
                    map(
                        lambda _cluster_info: _cluster_info["cluster_type"],
                        support_clusters,
                    )
                )
                for node_type, cluster_type in list(NodeTypes.STORAGE_NODES_MAPPING.items()):
                    if cluster_type in support_storage_cluster_types:
                        allowed_node.append(node_type)
                cls.support_node_type_instances_cache = allowed_node
            return node_type_instance_name in cls.support_node_type_instances_cache
        return True

    @classmethod
    def list(cls):
        # 返回当前 flow 支持的节点类型
        o_nodes = cls.objects.filter(available=1)
        l_nodes = []
        for _node in o_nodes:
            if cls.check_node_type_instance(_node.node_type_instance_name):
                l_nodes.append(model_to_dict(_node))
        return l_nodes

    @classmethod
    def node_type_instance_config(cls):
        """
        获取节点实例信息
        """
        node_type_instance_objects = cls.list()
        _config = {}
        for _node_type_instance in node_type_instance_objects:
            _config[_node_type_instance["node_type_instance_name"]] = _node_type_instance
        return _config

    @classmethod
    def get_node_instance(cls, node_type_instance_name):
        try:
            node_type_instance_name = cls.NODE_TYPE_MAP.get(node_type_instance_name, node_type_instance_name)
            return model_to_dict(cls.objects.get(node_type_instance_name=node_type_instance_name))
        except cls.DoesNotExist:
            raise Exception(
                _("节点类型{node_type_instance_name}不存在".format(node_type_instance_name=node_type_instance_name))
            )

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_node_instance"
        verbose_name = _("【节点】节点实例表")
        verbose_name_plural = _("【节点】节点实例表")


class FlowNodeInstanceLink(models.Model):
    id = models.AutoField(primary_key=True)
    upstream = models.CharField(_("上游节点"), max_length=255)
    downstream = models.CharField(_("下游节点"), max_length=255)
    # channel_type = models.CharField(_(u"需要的channel存储，可选NULL|stream|batch，支持多个，用逗号分隔"), max_length=255)
    downstream_link_limit = models.CharField(_("上游节点的下游限制范围"), max_length=255)
    upstream_link_limit = models.CharField(_("下游节点的上游限制范围"), max_length=255)
    available = models.BooleanField(_("是否启用"), default=True)
    description = models.TextField(_("备注信息"))
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField(_("最近更新人"), max_length=255)
    updated_at = models.DateTimeField(_("更新时间"), max_length=255, auto_now=True)

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_node_instance_link"
        verbose_name = _("【节点】节点连线表")
        verbose_name_plural = _("【节点】节点连线表")


# 节点连线黑名单
class FlowNodeInstanceLinkBlacklist(models.Model):
    id = models.AutoField(primary_key=True)
    upstream = models.CharField(_("上游节点"), max_length=255)
    downstream = models.CharField(_("下游节点"), max_length=255)
    description = models.TextField(_("备注信息"))
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_node_instance_link_blacklist"
        verbose_name = _("【节点】节点连线黑名单表")
        verbose_name_plural = _("【节点】节点连线黑名单表")


# 连线链路表
class FlowNodeInstanceLinkChannel(models.Model):
    result_table_id = models.CharField(_("节点rt"), max_length=255, primary_key=True)
    channel_storage = models.CharField(_("channel存储"), max_length=255, primary_key=True)
    downstream_node_id = models.IntegerField(_("下游节点ID"), max_length=11, primary_key=True)
    channel_mode = models.CharField(_("删存储需要记channel_mode"), max_length=255)
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)

    @classmethod
    def save_channel_storage(
        cls,
        result_table_id,
        channel_storage,
        downstream_node_id,
        channel_mode,
        created_by,
    ):
        cls.objects.update_or_create(
            result_table_id=result_table_id,
            channel_storage=channel_storage,
            downstream_node_id=downstream_node_id,
            defaults={"channel_mode": channel_mode, "created_by": created_by},
        )

    @classmethod
    def is_channel_storage_exist(cls, result_table_id, channel_storage, downstream_node_id):
        # 通过主键获取数据库值
        result_list = cls.objects.filter(
            result_table_id=result_table_id,
            channel_storage=channel_storage,
            downstream_node_id=downstream_node_id,
        )
        return len(result_list) > 0

    @classmethod
    def get_channel_storage(cls, downstream_node_id):
        return cls.objects.filter(downstream_node_id=downstream_node_id)

    @classmethod
    def is_channel_storage_used(cls, result_table_id, channel_storage):
        used = cls.objects.filter(result_table_id=result_table_id, channel_storage=channel_storage)
        # 使用大于 1，说明有其它链路使用到
        return len(used) > 1

    @classmethod
    def del_channel_storage(cls, **kwargs):
        cls.objects.get(**kwargs).delete()

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_node_instance_link_channel"
        verbose_name = _("【节点】节点连线链路表")
        verbose_name_plural = _("【节点】节点连线链路表")


class FlowNodeInstanceLinkGroupV2(models.Model):
    id = models.AutoField(primary_key=True)
    node_instance_name = models.CharField(_("节点实例名称"), max_length=255)
    upstream_max_link_limit = models.CharField(_("被组合的节点上游连接限制"), max_length=255)
    description = models.TextField(_("备注信息"))
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField(_("最近更新人"), max_length=255)
    updated_at = models.DateTimeField(_("更新时间"), max_length=255, auto_now=True)

    class Meta:
        app_label = "dataflow.flow"
        db_table = "dataflow_node_instance_link_group_v2"
        verbose_name = _("【节点】节点连线组合规则表")
        verbose_name_plural = _("【节点】节点连线组合规则表")


class TaskLogFilterRule(models.Model):
    id = models.AutoField(primary_key=True)
    group_name = models.CharField(_("黑名单组名"), default="default", max_length=255)
    blacklist = models.CharField(_("单个黑名单，纯字符串"), max_length=255)
    created_by = models.CharField(_("创建人"), max_length=255)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_by = models.CharField(_("最近更新人"), max_length=255)
    updated_at = models.DateTimeField(_("更新时间"), max_length=255, auto_now=True)
    description = models.TextField(_("备注信息"))

    class Meta:
        app_label = "dataflow.tasklog"
        db_table = "dataflow_tasklog_filter_rule"
        verbose_name = _("【节点日志】日志敏感信息过滤表")
        verbose_name_plural = _("【节点日志】日志敏感信息过滤表")


# 注册元数据 SyncHook
meta_sync_register(FlowInfo)
meta_sync_register(FlowNodeInfo)
meta_sync_register(FlowNodeRelation)
meta_sync_register(FlowNodeProcessing)
