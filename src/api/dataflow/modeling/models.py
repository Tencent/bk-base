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

from common.transaction import meta_sync_register
from django.db import models
from django.utils import translation
from django.utils.translation import ugettext
from django.utils.translation import ugettext_lazy as _
from django.utils.translation import ugettext_noop

from dataflow.flow.utils.language import Bilingual
from dataflow.modeling.settings import LOG_SPLIT_CHAR
from dataflow.shared.log import modeling_logger as logger


class Algorithm(models.Model):
    algorithm_name = models.CharField(primary_key=True, max_length=64)
    algorithm_alias = models.CharField(max_length=64)
    algorithm_original_name = models.CharField(max_length=128)
    description = models.TextField(blank=True, null=True)
    algorithm_type = models.CharField(max_length=32, blank=True, null=True)
    generate_type = models.CharField(max_length=32)
    sensitivity = models.CharField(max_length=32)
    project_id = models.IntegerField(blank=True, null=True)
    run_env = models.CharField(max_length=64, blank=True, null=True)
    framework = models.CharField(max_length=64, blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "algorithm"


class AlgorithmVersion(models.Model):
    id = models.AutoField(primary_key=True)
    algorithm_name = models.CharField(max_length=64)
    version = models.IntegerField()
    logic = models.TextField(blank=True, null=True)
    config = models.TextField()
    execute_config = models.TextField()
    properties = models.TextField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "algorithm_version"
        app_label = "dataflow.modeling"
        unique_together = (("algorithm_name", "version"),)


class ModelRelease(models.Model):
    class PUBLISH_STATUS(object):
        LATEST = "latest"
        STANDBY = "standby"

    PUBLISH_STATUS_CHOICES = (
        (PUBLISH_STATUS.LATEST, _("最新")),
        (PUBLISH_STATUS.STANDBY, _("备用")),
    )

    id = models.AutoField(primary_key=True)
    model_experiment_id = models.IntegerField(null=True)
    model_id = models.CharField(max_length=128)
    active = models.SmallIntegerField(default=1)
    description = models.TextField(blank=True, null=True)
    publish_status = models.CharField(max_length=32, default="latest", choices=PUBLISH_STATUS_CHOICES)
    model_config_template = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    version_index = models.IntegerField()
    protocol_version = models.CharField(max_length=16)
    basic_model_id = models.CharField(max_length=128, blank=True, null=True)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "model_release"


class BasicModel(models.Model):
    basic_model_id = models.CharField(primary_key=True, max_length=128)
    basic_model_name = models.CharField(max_length=64)
    basic_model_alias = models.CharField(max_length=64)
    algorithm_name = models.CharField(max_length=64)
    algorithm_version = models.IntegerField()
    description = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=32)
    active = models.IntegerField(default=1)
    config = models.TextField(blank=True, null=True)
    execute_config = models.TextField(blank=True, null=True)
    properties = models.TextField(blank=True, null=True)
    experiment_id = models.IntegerField(null=True)
    experiment_instance_id = models.IntegerField(null=True)
    node_id = models.CharField(max_length=128, blank=True, null=True, default=None)
    source = models.CharField(max_length=64)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    sample_latest_time = models.DateTimeField(blank=True, null=True)
    model_id = models.CharField(max_length=128)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "basic_model"


class ModelInstance(models.Model):

    id = models.AutoField(primary_key=True)
    model_release_id = models.IntegerField()
    model_id = models.CharField(max_length=128)
    model_experiment_id = models.IntegerField(null=True)
    project_id = models.IntegerField()
    flow_id = models.IntegerField(null=True, default=None)
    flow_node_id = models.IntegerField(null=True, default=None)
    instance_config = models.TextField(blank=True, null=True)
    active = models.SmallIntegerField(default=1)
    upgrade_config = models.TextField(null=True, default=None)
    sample_feedback_config = models.TextField(null=True, default=None)
    serving_mode = models.CharField(max_length=32)
    instance_status = models.CharField(max_length=32)
    execute_config = models.TextField(null=True, default=None)
    data_processing_id = models.CharField(max_length=255)
    input_result_table_ids = models.TextField(null=True)
    output_result_table_ids = models.TextField(null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    protocol_version = models.CharField(max_length=16)
    basic_model_id = models.CharField(max_length=128, blank=True, null=True)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "model_instance"


class ModelExperiment(models.Model):
    class STATUS(object):
        FINISHED = "finished"
        DEVELOPING = "developing"
        RELEASED = "released"

    STATUS_CHOICES = (
        (STATUS.FINISHED, _("训练完成")),
        (STATUS.DEVELOPING, _("开发中")),
        (STATUS.RELEASED, _("已发布")),
    )

    class PASS_TYPE(object):
        NOT_CONFIRMED = "not_confirmed"
        PASSED = "passed"
        FAILED = "failed"
        PASS = "pass"

    PASS_TYPE_CHOICES = (
        (PASS_TYPE.NOT_CONFIRMED, _("未确认")),
        (PASS_TYPE.PASSED, _("已通过")),
        (PASS_TYPE.PASS, _("通过")),
        (PASS_TYPE.FAILED, _("失败")),
    )

    experiment_id = models.AutoField(primary_key=True)
    experiment_name = models.CharField(max_length=64)
    experiment_alias = models.CharField(max_length=64)
    description = models.TextField(null=True, default=None)
    project_id = models.IntegerField()
    status = models.CharField(_("实验状态"), null=False, choices=STATUS_CHOICES)
    pass_type = models.CharField(choices=PASS_TYPE_CHOICES)
    active = models.SmallIntegerField(default=1)
    experiment_config = models.TextField(null=True)
    model_file_config = models.TextField(null=True)
    execute_config = models.TextField(null=True)
    properties = models.TextField(null=True)
    model_id = models.CharField(max_length=64)
    template_id = models.IntegerField()
    experiment_index = models.IntegerField()
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    sample_latest_time = models.DateTimeField(default=None, null=True)
    protocol_version = models.CharField(max_length=16, default="1.0")
    experiment_training = models.SmallIntegerField(default=1)
    continuous_training = models.SmallIntegerField(default=0)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "model_experiment"


class ExperimentInstance(models.Model):
    class STATUS(object):
        FINISHED = "finished"
        DEVELOPING = "developing"
        RELEASED = "released"

    STATUS_CHOICES = (
        (STATUS.FINISHED, _("训练完成")),
        (STATUS.DEVELOPING, _("开发中")),
        (STATUS.RELEASED, _("已发布")),
    )

    class GENERATE_TYPE(object):
        SYSTEM = "system"
        USER = "user"

    GENERATE_TYPE_CHOICES = (
        (GENERATE_TYPE.SYSTEM, _("系统")),
        (GENERATE_TYPE.USER, _("用户")),
    )

    class TRAINING_METHOD(object):
        MANUAL = "manual"
        AUTO = "auto"

    TRAINING_METHOD_CHOICES = (
        (TRAINING_METHOD.MANUAL, _("手动")),
        (TRAINING_METHOD.AUTO, _("自动")),
    )

    class RUN_STATUS(object):
        INIT = "init"
        FAILED = "failed"
        RUNNING = "running"
        NOT_RUN = "not_run"
        FINISHED = "finished"
        PARTIAL_FINISHED = "partial_finished"

    RUN_STATUS_CHOICES = (
        (RUN_STATUS.INIT, _("初始化")),
        (RUN_STATUS.FAILED, _("失败")),
        (RUN_STATUS.RUNNING, _("运行中")),
        (RUN_STATUS.NOT_RUN, _("未运行")),
        (RUN_STATUS.FINISHED, _("成功")),
        (RUN_STATUS.PARTIAL_FINISHED, _("部分完成")),
    )

    id = models.AutoField(primary_key=True)
    experiment_id = models.IntegerField()
    model_id = models.CharField(max_length=128)
    generate_type = models.CharField(max_length=32, choices=GENERATE_TYPE_CHOICES)
    training_method = models.CharField(max_length=32, choices=TRAINING_METHOD_CHOICES)
    config = models.TextField(default=None)
    execute_config = models.TextField(default=None)
    properties = models.TextField(default=None)
    status = models.CharField(max_length=64, choices=STATUS_CHOICES)
    runtime_info = models.TextField(default=None)
    run_status = models.CharField(max_length=64, choices=RUN_STATUS_CHOICES)
    active = models.SmallIntegerField(default=1)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    description = models.TextField(_("备注信息"), blank=True)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "experiment_instance"


class ModelExperimentNode(models.Model):
    class ExperimentNodeOperateStatus(object):
        """模型节点操作状态"""

        INIT = "init"
        UPDATED = "updated"
        FINISHED = "finished"

    EXPERIMENT_NODE_OPERATE_STATUS_CHOICES = (
        (ExperimentNodeOperateStatus.INIT, _("初始化")),
        (ExperimentNodeOperateStatus.UPDATED, _("已更新")),
        (ExperimentNodeOperateStatus.FINISHED, _("完成")),
    )

    class ExperimentNodeRunStatus(object):
        """
        模型实验节点的运行状态枚举
        """

        NOT_RUN = "not_run"
        PENDING = "pending"
        RUNNING = "running"
        SUCCESS = "success"
        FAILED = "failed"

    EXPERIMENT_NODE_RUN_STATUS_CHOICES = (
        (ExperimentNodeRunStatus.NOT_RUN, _("未运行")),
        (ExperimentNodeRunStatus.PENDING, _("等待")),
        (ExperimentNodeRunStatus.RUNNING, _("运行中")),
        (ExperimentNodeRunStatus.SUCCESS, _("成功")),
        (ExperimentNodeRunStatus.FAILED, _("失败")),
    )
    node_id = models.CharField(primary_key=True, max_length=128)
    model_id = models.CharField(max_length=128)
    model_experiment_id = models.IntegerField()
    node_name = models.CharField(max_length=64)
    node_alias = models.CharField(max_length=64)
    node_config = models.TextField(default=None)
    properties = models.TextField(default=None)
    active = models.SmallIntegerField(default=1)
    step_name = models.CharField(max_length=64)
    node_index = models.IntegerField()
    action_name = models.CharField(max_length=64)
    node_role = models.CharField(max_length=32)
    input_config = models.TextField(default="{}")
    output_config = models.TextField(default="{}")
    execute_config = models.TextField(default="{}")
    algorithm_config = models.TextField(default="{}")
    run_status = models.CharField(max_length=32, choices=EXPERIMENT_NODE_RUN_STATUS_CHOICES)
    operate_status = models.CharField(max_length=32, choices=EXPERIMENT_NODE_OPERATE_STATUS_CHOICES)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "model_experiment_node"


class UserOperationLog(models.Model):
    operation = models.CharField(max_length=64)
    module = models.CharField(max_length=64)
    operator = models.CharField(max_length=64)
    description = models.CharField(max_length=255)
    operation_result = models.CharField(max_length=64)
    errors = models.TextField(blank=True, null=True)
    extra = models.TextField(blank=True, null=True)
    object = models.CharField(max_length=255)
    object_id = models.CharField(max_length=255)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    object_name = models.CharField(max_length=255, blank=True, null=True)
    operation_name = models.TextField(max_length=255, null=True)

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "user_operation_log"


class ModelInfo(models.Model):
    class STATUS(object):
        FINISHED = "finished"
        DEVELOPING = "developing"
        RELEASED = "released"

    STATUS_CHOICES = (
        (STATUS.FINISHED, _("训练完成")),
        (STATUS.DEVELOPING, _("开发中")),
        (STATUS.RELEASED, _("已发布")),
    )

    class SENSITIVITY(object):
        PUBLIC = "public"
        PRIVATE = "private"

    SENSITIVITY_CHOICES = (
        (SENSITIVITY.PUBLIC, _("公共")),
        (SENSITIVITY.PRIVATE, _("私有")),
    )

    model_id = models.CharField(primary_key=True, max_length=128)
    model_name = models.CharField(max_length=64)
    model_alias = models.CharField(max_length=64)
    description = models.TextField(blank=True, null=True)
    project_id = models.IntegerField()
    model_type = models.CharField(max_length=64)
    sensitivity = models.CharField(max_length=32, choices=SENSITIVITY_CHOICES)
    active = models.SmallIntegerField(default=1)
    properties = models.TextField(blank=True, null=True, default=None)
    scene_name = models.CharField(max_length=64)
    status = models.CharField(max_length=32, choices=STATUS_CHOICES)
    train_mode = models.CharField(max_length=32, default="supervised")
    run_env = models.CharField(max_length=32, default=None)
    sample_set_id = models.IntegerField(blank=True, null=True, default=None)
    input_standard_config = models.TextField(blank=True, null=True)
    output_standard_config = models.TextField(blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)
    protocol_version = models.CharField(max_length=16, default="1.0")
    sample_type = models.CharField(max_length=50, default="timeseries")
    modeling_type = models.CharField(max_length=50, default="aiops")

    class Meta:
        managed = False
        app_label = "dataflow.modeling"
        db_table = "model_info"


class StorageModel(models.Model):
    id = models.AutoField(primary_key=True)
    model_id = models.CharField("模型工程ID", max_length=128)
    physical_table_name = models.CharField("物理表名称", max_length=64)
    expires = models.CharField("过期时间", max_length=32, blank=True, null=True)
    storage_config = models.CharField(blank=True, null=True)
    storage_cluster_config_id = models.IntegerField("存储集群配置ID")
    storage_cluster_name = models.CharField("存储集群名称", max_length=32)
    storage_cluster_type = models.CharField("存储集群类型", max_length=32)
    active = models.NullBooleanField("是否可用", blank=True, null=True)
    priority = models.IntegerField("优先级", blank=True, null=True, default=0)
    generate_type = models.CharField("生成类型", max_length=32)
    content_type = models.CharField("内容类型", max_length=32, blank=True, null=True)
    created_by = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_by = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = "storage_model"
        app_label = "dataflow.modeling"


class MLSqlModelInfo(models.Model):
    class MODELTYPE(object):
        MODELFLOW = "modelflow"
        MODELING = "modeling"
        MLSQL = "model"

    MODELTYPE_CHOICES = (
        (MODELTYPE.MODELFLOW, _("工作流")),
        (MODELTYPE.MODELING, _("场景化建模")),
        (MODELTYPE.MLSQL, _("model")),
    )

    class TRAINMODE(object):
        SUPERVISED = "supervised"
        UNSUPERVISED = "unsupervised"
        HALF_SUPERVISED = "half-supervised"

    TRAINMODE_CHOICES = (
        (TRAINMODE.SUPERVISED, _("有监督")),
        (TRAINMODE.UNSUPERVISED, _("无监督")),
        (TRAINMODE.HALF_SUPERVISED, _("半监督")),
    )

    class SENSITIVITY(object):
        PUBLIC = "public"
        PRIVATE = "private"

    SENSITIVITY_CHOICES = (
        (SENSITIVITY.PUBLIC, _("公有")),
        (SENSITIVITY.PRIVATE, _("私有")),
    )

    class STATUS(object):
        UNRELEASED = "unreleased"
        RELEASED = "released"

    STATUS_CHOICES = ((STATUS.UNRELEASED, _("未发布")), (STATUS.RELEASED, _("已发布")))
    id = models.AutoField(primary_key=True)
    model_name = models.CharField(_("模型名称"), null=False)
    model_alias = models.CharField(_("模型中文名"), null=False)
    model_framework = models.CharField(_("模型所属框架"), default="spark")
    model_type = models.CharField(_("模型类型"), null=False, choices=MODELTYPE_CHOICES)
    project_id = models.IntegerField(_("项目ID"))
    algorithm_name = models.CharField(_("生成模型的算法名称"), null=False)
    model_storage_id = models.IntegerField(_("模型的存储id"))
    train_mode = models.CharField(_("训练类型"), choices=TRAINMODE_CHOICES, default="supervised")
    sensitivity = models.CharField(_("敏感级别"), choices=SENSITIVITY_CHOICES, default="private")
    active = models.IntegerField(_("软删除标志位"), null=False, default=1)
    status = models.CharField(_("模型状态"), null=False, choices=STATUS_CHOICES)
    input_standard_config = models.CharField(_("输入规范"), default="{}")
    output_standard_config = models.CharField(_("输出规范"), default="{}")
    created_by = models.CharField(_("创建人"))
    create_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    modified_by = models.CharField(_("最后修改人"))
    modified_at = models.DateTimeField(_("最后修改时间"), auto_now_add=True)
    description = models.CharField(_("描述信息"))

    class Meta:
        app_label = "dataflow.model"
        db_table = "mlsql_model_info"


class MLSqlStorageModel(models.Model):
    class GENERATETYPE(object):
        USER = "user"
        SYSTEM = "system"

    GENERATETYPE_CHOICES = (
        (GENERATETYPE.USER, _("用户产生")),
        (GENERATETYPE.SYSTEM, _("系统产生")),
    )
    id = models.AutoField(primary_key=True)
    physical_table_name = models.CharField(_("模型的存储路径"), null=False)
    expires = models.CharField(_("过期信息"), null=True)
    storage_config = models.CharField(_("存储的配置信息"), null=False, default="{}")
    storage_cluster_config_id = models.IntegerField(_("存储集群配置id"))
    active = models.IntegerField(_("软删除标志位"), default=1)
    priority = models.IntegerField(_("优先级"), default=0)
    generate_type = models.CharField(_("用户产生的or系统产生的"), choices=GENERATETYPE_CHOICES, default="system")
    created_by = models.CharField(_("创建人"))
    create_at = models.DateTimeField(_("创建时间"), auto_now_add=True)
    updated_by = models.CharField(_("最后修改人"))
    updated_at = models.DateTimeField(_("最后修改时间"), auto_now_add=True)
    description = models.CharField(_("描述信息"))
    data_type = models.CharField(_("数据格式"))

    class Meta:
        app_label = "dataflow_storage_model"
        db_table = "mlsql_storage_model"


# mlsql模型执行相关model
class MLSqlExecuteLog(models.Model):
    class STATUS(object):
        SUCCESS = "success"
        FAILURE = "failure"
        RUNNING = "running"
        CANCELLED = "cancelled"
        PENDING = "pending"

    # 作业最终状态
    FINAL_STATUS = [
        STATUS.SUCCESS,
        STATUS.FAILURE,
        STATUS.CANCELLED,
        STATUS.RUNNING,
        STATUS.PENDING,
    ]

    STATUS_CHOICES = (
        (STATUS.SUCCESS, _("执行成功")),
        (STATUS.FAILURE, _("执行失败")),
        (STATUS.RUNNING, _("运行中")),
        (STATUS.CANCELLED, _("撤销申请")),
        (STATUS.PENDING, _("等待执行")),
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

    TIMEOUT_OUT_MINUTES = 60

    id = models.AutoField(primary_key=True)
    session_id = models.CharField(_("session_id"), max_length=32)
    notebook_id = models.IntegerField(_("notebook_id"), default=0)
    cell_id = models.CharField(_("cell_id"), max_length=32)
    action = models.CharField(_("执行类型"), max_length=32, choices=ACTION_TYPES_CHOICES)
    status = models.CharField(_("执行结果"), max_length=32, choices=STATUS_CHOICES, default=STATUS.PENDING)
    logs_zh = models.TextField(_("执行日志"), null=True)
    logs_en = models.TextField(_("执行日志_en"), null=True)
    start_time = models.DateTimeField(_("开始时间"), null=True)
    end_time = models.DateTimeField(_("结束时间"), null=True)
    context = models.TextField(_("全局上下文，参数，json格式"), null=True)
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

    def add_log(self, msg, status=STATUS.PENDING, stage=None, level="INFO", time=None, detail=""):
        """
        存入 logs 字段的日志格式为 {stage}|{level}|{time}|{msg}|{status}|{detail}
        :param msg: 日志信息，支持双语对象
        :param status: 当前任务运行状态
        :param stage: 进行到的阶段
        :param level: 日志级别，可选有 INFO、WARNING、ERROR
        :param time: 日志时间，格式 %Y-%m-%d %H:%M:%S
        :param detail: 详情
        :return:
        """
        _time = time if time is not None else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_format = "{stage}|{level}|{time}|{msg}|{status}|{detail}"
        # msg为双语类型
        if isinstance(msg, Bilingual):
            zh_msg = msg.zh.replace("\n", " ")
            en_msg = msg.en.replace("\n", " ")

            detail = detail.replace("\n", LOG_SPLIT_CHAR)

            # 将 msg 中特殊字符 | 转成不可见字符
            _log = log_format.format(
                stage=stage,
                level=level,
                time=_time,
                msg=zh_msg.replace("|", LOG_SPLIT_CHAR),
                status=status,
                detail=detail,
            )
            _log_en = log_format.format(
                stage=stage,
                level=level,
                time=_time,
                msg=en_msg.replace("|", LOG_SPLIT_CHAR),
                status=status,
                detail=detail,
            )
        else:
            msg = msg.replace("\n", " ")
            _log = log_format.format(
                stage=stage,
                level=level,
                time=_time,
                msg=msg.replace("|", LOG_SPLIT_CHAR),
                status=status,
                detail=detail,
            )
            _log_en = log_format.format(
                stage=stage,
                level=level,
                time=_time,
                msg=ugettext(msg).replace("|", LOG_SPLIT_CHAR),
                status=status,
                detail=detail,
            )

        if self.logs_zh is None:
            self.logs_zh = _log
            self.logs_en = _log_en
        else:
            self.logs_zh = "{prev}\n{new}".format(prev=self.logs_zh, new=_log)
            self.logs_en = "{prev}\n{new}".format(prev=self.logs_en, new=_log_en)
        self.save(update_fields=["logs_zh", "logs_en"])

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
        for _log in logs:
            _log_parts = _log.split("|")
            if len(_log_parts) < 5:
                # 初始版本的日志分割数为5
                logger.warning("日志格式({})错误: {}".format(self.id, _log))
                continue
            # 取出时，将不可见字符转为 |
            cleaned_logs.append(
                {
                    "stage": _log_parts[0],
                    "level": _log_parts[1],
                    "time": _log_parts[2],
                    "message": _log_parts[3].replace(LOG_SPLIT_CHAR, "|"),
                    "status": self.STATUS.SUCCESS if self.status == self.STATUS.SUCCESS else _log_parts[4],
                    "detail": _log_parts[5],
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
        db_table = "mlsql_execute_log"
        verbose_name = _("【启停记录】DataFlow 启停记录")
        verbose_name_plural = _("【启停记录】DataFlow 启停记录")


# 元数据系统间同步
meta_sync_register(ModelInfo)
meta_sync_register(BasicModel)
meta_sync_register(ModelRelease)
meta_sync_register(ModelInstance)
meta_sync_register(Algorithm)
meta_sync_register(AlgorithmVersion)
meta_sync_register(ModelExperiment)
meta_sync_register(ExperimentInstance)
meta_sync_register(ModelExperimentNode)
# meta_sync_register(UserOperationLog)
