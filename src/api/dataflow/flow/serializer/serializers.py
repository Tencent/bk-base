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
import time

from common.base_utils import custom_params_valid
from common.exceptions import ValidationError
from conf.dataapi_settings import MULTI_GEOG_AREA
from django.utils.translation import ugettext as _
from rest_framework import serializers

from dataflow.flow.exceptions import FlowPermissionNotAllowedError, ValidError
from dataflow.flow.handlers.link_rules import NodeInstanceLink
from dataflow.shared.meta.project.project_helper import ProjectHelper


def tags_validate(attrs):
    if MULTI_GEOG_AREA:
        tags = attrs.get("tags")
        if not tags:
            raise ValidationError(_("tags 字段必填。"))
    else:
        tags = ["inland"]
    attrs["tags"] = tags


class DeploySerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class UpdateGraphSerializer(serializers.Serializer):
    # action = serializers.ChoiceField(label=_(u'节点操作'), choices=(
    #     ('update_nodes', u'更新节点'),
    #     ('delete_lines', u'删除连线'),
    #     ('create_lines', u'创建连线'),
    #     ('save_draft', u'保存草稿')
    # ))
    action = serializers.CharField(label=_("节点操作"))
    data = serializers.ListField(required=True, label=_("待更新的信息"))

    # 自定义错误信息
    def validate_action(self, action):
        ACTIONS = ["update_nodes", "delete_lines", "create_lines", "save_draft"]
        if action not in ACTIONS:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(ACTIONS))
        return action


class MultiFlowSerializer(serializers.Serializer):
    flow_ids = serializers.ListField(label=_("flow_id列表"), child=serializers.IntegerField(label=_("flow_id")))


class FlowActionSerializer(serializers.Serializer):
    is_latest = serializers.BooleanField(label=_("是否读取最新数据"), required=False)


class FlowStartActionSerializer(serializers.Serializer):
    # 最新：from_tail，继续：continue(默认)，最早：from_head
    consuming_mode = serializers.CharField(label=_("从最新/继续/最早位置消费"), required=False)
    cluster_group = serializers.CharField(label=_("计算集群组"), required=False)

    # 自定义错误信息
    def validate_consuming_mode(self, consuming_mode):
        modes = ["from_tail", "from_head", "continue"]
        if consuming_mode not in modes:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(modes))
        return consuming_mode


class FileUploadSerializer(serializers.Serializer):
    file = serializers.FileField(label=_("文件"))
    type = serializers.CharField(label=_("上传文件类型"))

    def validate_type(self, type):
        upload_types = ["tdw", "sdk"]
        if type not in upload_types:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(upload_types))
        return type

    def validate_file(self, attrs):
        # 文件大小限制为300M
        if attrs.size > 300 * 1024 * 1024:
            raise ValidError(_("上传文件大小超出最大限制(300M)"))
        return attrs


class CreateFlowSerializer(serializers.Serializer):
    class TDWConfSerializer(serializers.Serializer):

        task_info = serializers.JSONField(label=_("任务信息"))
        gaia_id = serializers.IntegerField(label=_("计算集群ID"))
        tdwAppGroup = serializers.CharField(label=_("应用组ID"))
        sourceServer = serializers.CharField(label=_("源服务器"))

        def validate_task_info(self, value):
            support_task_types = ["spark"]
            for task_type in list(value.keys()):
                if task_type not in support_task_types:
                    raise ValidError(
                        _("任务类型错误(%(task_type)s)，当前仅支持(%(support_task_types)s)")
                        % {
                            "task_type": task_type,
                            "support_task_types": "、".join(support_task_types),
                        }
                    )
            return value

    project_id = serializers.IntegerField(label=_("项目ID"))
    flow_name = serializers.CharField(label=_("flow名称"))
    tdw_conf = serializers.JSONField(required=False, label=_("TDW 参数"))

    def validate_tdw_conf(self, value):
        if not value:
            value = {}
        else:
            params = CreateFlowSerializer.TDWConfSerializer(data=value)
            params.is_valid(raise_exception=True)
            value = params.validated_data
        return value

    def validate(self, attrs):
        tdw_conf = attrs.get("tdw_conf", {})
        if tdw_conf:
            # 判断项目是否有引用组权限
            res_data = ProjectHelper.get_project(attrs.get("project_id"))
            if tdw_conf["tdwAppGroup"] not in res_data["tdw_app_groups"]:
                raise FlowPermissionNotAllowedError(
                    _("当前项目无权限使用应用组(%(tdw_app_group)s)") % {"tdw_app_group": tdw_conf["tdwAppGroup"]}
                )
        return attrs


class UpdateFlowSerializer(CreateFlowSerializer):
    project_id = serializers.IntegerField(required=False, label=_("项目ID"))


class CreateTDWSourceSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(label=_("业务ID"))
    table_name = serializers.RegexField(label=_("表名"), regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$", max_length=255)
    cluster_id = serializers.CharField(label=_("集群ID"), max_length=255)
    db_name = serializers.CharField(label=_("数据库名称"), max_length=255)
    tdw_table_name = serializers.CharField(label=_("存量表名"), max_length=255)
    output_name = serializers.CharField(label=_("输出中文名"), max_length=255)
    description = serializers.CharField(label=_("描述"))
    associated_lz_id = serializers.CharField(required=False, label=_("洛子ID"))


class UpdateTDWSourceSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(label=_("业务ID"))
    table_name = serializers.RegexField(label=_("表名"), regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$", max_length=255)
    output_name = serializers.CharField(required=False, label=_("输出中文名"), max_length=255)
    description = serializers.CharField(required=False, label=_("描述"))
    associated_lz_id = serializers.CharField(required=False, label=_("洛子ID"))


class RemoveTDWSourceSerializer(serializers.Serializer):
    bk_biz_id = serializers.IntegerField(label=_("业务ID"))
    table_name = serializers.RegexField(label=_("表名"), regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$", max_length=255)


class ModelingJobSerializer(serializers.Serializer):
    task_id = serializers.IntegerField(label=_("任务ID"))


class ExecuteSqlSerializer(serializers.Serializer):
    project_id = serializers.IntegerField(required=True, label=_("项目ID"))


class CreateModelSerializer(serializers.Serializer):
    def validate(self, attrs):
        return attrs


class LocationSerializer(serializers.Serializer):
    x = serializers.FloatField(label=_("横坐标"))
    y = serializers.FloatField(label=_("纵坐标"))


class CreateFlowWithNodesSerializer(CreateFlowSerializer):
    class CreateNodeSerializer(serializers.Serializer):
        class FromNodesSerializer(serializers.Serializer):
            id = serializers.IntegerField(label=_("上游节点id，若是创建全新flow，可定义为任意整数"))
            from_result_table_ids = serializers.ListField(label=_("上游节点result_table_id列表"))
            frontend_info = LocationSerializer(required=False, many=False, label=_("节点画布位置坐标"))

        id = serializers.IntegerField(label=_("节点id，若是创建全新flow，可定义为任意整数"))
        from_nodes = FromNodesSerializer(label=_("上游节点信息"), allow_empty=True, many=True)
        node_type = serializers.CharField(label=_("节点类型"))
        bk_biz_id = serializers.IntegerField(label=_("业务ID"))
        name = serializers.CharField(label=_("节点名称"))

        def validate_node_type(self, value):
            SUPPORT_NODE_TYPES = NodeInstanceLink.validate_support_node_types()
            # value = NODE_TYPE_MAP.get(value, value)
            if value not in SUPPORT_NODE_TYPES:
                raise ValidError(
                    _("非法操作(node_type=%(node_type)s)，目前仅支持节点类型（%(support_node_types)s）")
                    % {
                        "node_type": value,
                        "support_node_types": ", ".join(SUPPORT_NODE_TYPES),
                    }
                )
            return value

    nodes = CreateNodeSerializer(required=True, label=_("节点列表信息"), many=True)


class InitFlowWithNodesSerializer(serializers.Serializer):
    class CreateNodeSerializer(serializers.Serializer):
        class FromNodesSerializer(serializers.Serializer):
            id = serializers.IntegerField(label=_("上游节点id，若是创建全新flow，可定义为任意整数"))
            from_result_table_ids = serializers.ListField(label=_("上游节点result_table_id列表"))
            frontend_info = LocationSerializer(required=False, many=False, label=_("节点画布位置坐标"))

        id = serializers.IntegerField(label=_("节点id，若是创建全新flow，可定义为任意整数"))
        from_nodes = FromNodesSerializer(label=_("上游节点信息"), allow_empty=True, many=True)
        node_type = serializers.CharField(label=_("节点类型"))
        bk_biz_id = serializers.IntegerField(label=_("业务ID"))
        name = serializers.CharField(label=_("节点名称"))

        def validate_node_type(self, value):
            SUPPORT_NODE_TYPES = NodeInstanceLink.validate_support_node_types()
            # value = NODE_TYPE_MAP.get(value, value)
            if value not in SUPPORT_NODE_TYPES:
                raise ValidError(
                    _("非法操作(node_type=%(node_type)s)，目前仅支持节点类型（%(support_node_types)s）")
                    % {
                        "node_type": value,
                        "support_node_types": ", ".join(SUPPORT_NODE_TYPES),
                    }
                )
            return value

    nodes = CreateNodeSerializer(required=True, label=_("节点列表信息"), allow_empty=False, many=True)


class NodeSerializer(serializers.Serializer):
    class LinkSerializer(serializers.Serializer):
        class SourcePointSerializer(serializers.Serializer):
            node_id = serializers.IntegerField(label=_("节点id"))
            id = serializers.CharField(label=_("id"))
            arrow = serializers.CharField(label=_("方向"))

        class TargetPointSerializer(serializers.Serializer):
            id = serializers.CharField(label=_("id"))
            arrow = serializers.CharField(label=_("方向"))

        source = SourcePointSerializer(required=True, many=False, label=_("连线始端信息"))
        target = TargetPointSerializer(required=True, many=False, label=_("连线末端信息"))

    from_links = LinkSerializer(label=_("父亲连线信息列表"), many=True)
    config = serializers.DictField(label=_("节点配置"))
    frontend_info = LocationSerializer(required=True, many=False, label=_("节点画布位置坐标"))


class CreateNodeSerializer(NodeSerializer):
    """
    创建节点参数校验器
    """

    node_type = serializers.CharField(label=_("节点类型"))

    # 自定义错误信息
    def validate_node_type(self, value):
        SUPPORT_NODE_TYPES = NodeInstanceLink.validate_support_node_types()
        # value = NODE_TYPE_MAP.get(value, value)
        if value not in SUPPORT_NODE_TYPES:
            raise ValidError(
                _("非法操作(node_type=%(node_type)s)，目前仅支持节点类型(%(support_node_types)s)")
                % {
                    "node_type": value,
                    "support_node_types": ", ".join(SUPPORT_NODE_TYPES),
                }
            )
        return value


class UpdateNodeSerializer(NodeSerializer):
    """
    更新节点参数校验器
    """

    pass


class PartialUpdateNodeSerializer(serializers.Serializer):
    """
    更新节点部分信息接口初步校验器
    """

    class FromNodesSerializer(serializers.Serializer):
        id = serializers.IntegerField(label=_("上游节点id，若是创建全新flow，可定义为任意整数"))
        from_result_table_ids = serializers.ListField(label=_("上游节点result_table_id列表"))

    from_nodes = FromNodesSerializer(label=_("上游节点信息"), required=False, allow_empty=True, many=True)


class ApplyCustomCalculateJobSerializer(serializers.Serializer):
    # 传入时间格式
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    node_ids = serializers.ListField(label=_("待补算节点id列表"), allow_empty=False)
    start_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("起始时间"))
    end_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("截止时间"))
    with_child = serializers.BooleanField(label=_("同时重算下游节点"))

    @classmethod
    def get_format_time(cls, param_value):
        time_array = time.strptime(param_value, cls.TIME_FORMAT)
        return int(time.mktime(time_array) * 1000)


class ApplyCustomCalculateJobConfirmSerializer(serializers.Serializer):
    class STATUS(object):
        CANCELLED = "cancelled"
        APPROVED = "approved"
        REJECTED = "rejected"

    confirm_types = [STATUS.CANCELLED, STATUS.APPROVED, STATUS.REJECTED]
    id = serializers.RegexField(regex=r"^\d+#\d+$", required=True, error_messages={"invalid": _("id格式错误")})
    confirm_type = serializers.CharField(label=_("撤销，同意，驳回"))
    message = serializers.CharField(required=False, allow_blank=True, label=_("message"))
    operator = serializers.CharField(label=_("审批人"))

    # 自定义错误信息
    def validate_confirm_type(self, confirm_type):
        if confirm_type not in ApplyCustomCalculateJobConfirmSerializer.confirm_types:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(ApplyCustomCalculateJobConfirmSerializer.confirm_types))
        return confirm_type


class UpdateJobSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label="结果表")


class QueryDebugDetailSerializer(serializers.Serializer):
    node_id = serializers.IntegerField(label=_("节点id"))


class MonitorSerializer(serializers.Serializer):
    """
    监控打点校验Serializer，支持传入detail_time_filter，
    可灵活设定各个监控类型的时间区间和聚合时间
    """

    monitor_type = serializers.CharField(label=_("监控类型"), required=False, initial="")
    interval = serializers.RegexField(
        regex=r"^\d+(m|h|d|w)?$",
        required=False,
        error_messages={"invalid": _("时间区间格式错误")},
    )
    group_time = serializers.RegexField(
        regex=r"^\d+(m|h|d|w)?$",
        required=False,
        error_messages={"invalid": _("聚合时间格式错误")},
    )
    detail_time_filter = serializers.JSONField(required=False)

    def __init__(self, *args, **kwargs):
        """
        初始化时需传入关键字 monitor_types
        @param {List} monitor_types MonitorHandler.DEFAULT_MONITOR_TYPES
        """
        self.default_monitor_types = kwargs.pop("default_monitor_types", [])
        self.supported_monitor_types = kwargs.pop("supported_monitor_types", [])
        super(MonitorSerializer, self).__init__(self, *args, **kwargs)

    def validate_monitor_type(self, value):
        if value:
            value_list = value.split(",")
            if any(_v not in self.supported_monitor_types for _v in value_list):
                raise ValidError(_("不支持当前的监控类型(%(support_monitor_type)s)") % {"support_monitor_type": value})
            value = value_list
        else:
            value = self.default_monitor_types
        return value

    def validate_detail_time_filter(self, value):
        if value:
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                raise ValidError(_("时间过滤参数必须是JSON"))
        else:
            value = []
        for _v in value:
            params = MonitorSerializer(
                default_monitor_types=self.supported_monitor_types,
                supported_monitor_types=self.supported_monitor_types,
                data=_v,
            )
            params.is_valid(raise_exception=True)

        return value

    def validate(self, attrs):
        """
        参数整合，把interval等参数整合到对应监控类型中
        @param {Dict} attrs 清洗过的参数
        """
        monitor_type = attrs.get("monitor_type", "")
        if not monitor_type:
            attrs.update({"monitor_type": self.default_monitor_types})
        attrs.update({"time_filter": {}})
        detail_time_filter = attrs.get("detail_time_filter", [])
        for monitor_type in self.supported_monitor_types:
            attrs["time_filter"].update({monitor_type: {}})

        for time_filter in detail_time_filter:
            attrs["time_filter"].update(
                {
                    time_filter["monitor_type"]: {
                        "interval": time_filter.get("interval", None),
                        "group_time": time_filter.get("group_time", None),
                    }
                }
            )
            _time_filter = attrs["time_filter"][time_filter["monitor_type"]]
            if not _time_filter.get("interval"):
                _time_filter.pop("interval")
            if not _time_filter.get("group_time"):
                _time_filter.pop("group_time")
        return attrs


class CreateFunctionSerializer(serializers.Serializer):
    class CodeConfigSerializer(serializers.Serializer):
        class DependenciesSerializer(serializers.Serializer):
            group_id = serializers.CharField(label="groupId")
            artifact_id = serializers.CharField(label="artifactId")
            version = serializers.CharField(label="version")

        dependencies = DependenciesSerializer(required=False, many=True, label="代码配置")
        code = serializers.CharField(label="code")

    func_name = serializers.CharField(label="函数名称")
    func_alias = serializers.CharField(label="函数中文名")
    func_language = serializers.CharField(label="函数编程语言")
    func_udf_type = serializers.CharField(label="udf类型")
    input_type = serializers.ListField(label="输入参数")
    return_type = serializers.ListField(label="返回参数")
    explain = serializers.CharField(label="函数说明")
    example = serializers.CharField(label="使用样例")
    example_return_value = serializers.CharField(label="样例返回")
    code_config = CodeConfigSerializer(required=True, many=False, label="代码配置")

    def validate_func_language(self, func_language):
        support_func_language = ["java", "python"]
        if func_language not in support_func_language:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_func_language))
        return func_language

    def validate_func_udf_type(self, func_udf_type):
        support_func_udf_type = ["udf", "udtf", "udaf"]
        if func_udf_type not in support_func_udf_type:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_func_udf_type))
        return func_udf_type


class UpdateFunctionSerializer(CreateFunctionSerializer):
    func_name = serializers.CharField(required=False, label="函数名称")

    def validate(self, attrs):
        if "func_name" in attrs:
            del attrs["func_name"]
        return attrs


class CodeFrameSerializer(serializers.Serializer):
    func_language = serializers.CharField(label="函数编程语言")
    func_udf_type = serializers.CharField(label="udf类型")
    input_type = serializers.ListField(label="输入参数")
    return_type = serializers.ListField(label="返回参数")

    def validate_func_language(self, func_language):
        support_func_language = ["java", "python"]
        if func_language not in support_func_language:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_func_language))
        return func_language

    def validate_input_type(self, input_type):
        support_type = ["string", "int", "long", "float", "double"]
        if input_type and list(set(input_type).difference(set(support_type))):
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_type))
        return input_type

    def validate_return_type(self, return_type):
        support_type = ["string", "int", "long", "float", "double"]
        if return_type and list(set(return_type).difference(set(support_type))):
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_type))
        return return_type


class EditFunctionSerializer(serializers.Serializer):
    ignore_locked = serializers.BooleanField(label="是否忽略锁定状态编辑函数")


class StartFunctionDebuggerSerializer(serializers.Serializer):
    class DebugDataSerializer(serializers.Serializer):
        class SchemaSerializer(serializers.Serializer):
            field_name = serializers.CharField(label="field_name")
            field_type = serializers.CharField(label="field_type")

        schema = SchemaSerializer(required=True, many=True, label="数据schema")
        value = serializers.ListField(child=serializers.ListField(label="调试数据列表"))

    calculation_types = serializers.ListField(label="计算类型", allow_empty=False)
    sql = serializers.CharField(label="sql")
    debug_data = DebugDataSerializer(required=True, many=False, label="调试数据")
    result_table_id = serializers.CharField(label="结果表")

    def validate_calculation_types(self, calculation_types):
        support_calculation_types = ["stream", "batch"]
        if calculation_types and list(set(calculation_types).difference(set(support_calculation_types))):
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_calculation_types))
        return calculation_types

    def validate_debug_data(self, debug_data):
        schema = debug_data["schema"]
        value = debug_data["value"]
        value_len = list({len(i) for i in value})
        if len(value_len) != 1:
            raise ValidError(_("调试数据列表长度不一致"))
        if value_len[0] != len(schema):
            raise ValidError(_("调试数据列表长度与字段结构不一致"))
        return debug_data


class FunctionDebugDataSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label="结果表")


class DeployFunctionSerializer(serializers.Serializer):
    release_log = serializers.CharField(label="发布日志")


class RetrieveFunctionSerializer(serializers.Serializer):
    add_project_info = serializers.BooleanField(required=False, label="是否返回项目信息")
    add_flow_info = serializers.BooleanField(required=False, label="是否返回项目信息")
    add_node_info = serializers.BooleanField(required=False, label="是否返回项目信息")
    add_release_log_info = serializers.BooleanField(required=False, label="是否返回项目信息")


class FunctionRelatedInfoSerializer(serializers.Serializer):
    object_type = serializers.CharField(label=_("函数信息关联对象类型"))
    object_id = serializers.ListField(required=False, child=serializers.IntegerField(), label=_("对象ID列表"))

    def validate_object_type(self, object_type):
        support_type = ["project", "flow"]
        if object_type not in support_type:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_type))
        return object_type


class CheckExampleSqlSerializer(serializers.Serializer):
    sql = serializers.CharField(label=_("SQL使用样例"))


class BatchStatusListSerializer(serializers.Serializer):
    # 传入时间格式
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    start_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("起始时间"))
    end_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("截止时间"))

    @classmethod
    def get_format_time(cls, time_str):
        time_array = time.strptime(time_str, cls.TIME_FORMAT)
        return int(time.mktime(time_array) * 1000)

    @classmethod
    def format_timestamp(cls, timestamp):
        time_array = time.localtime(timestamp / 1000)
        return time.strftime(cls.TIME_FORMAT, time_array)


class ModelAppStatusListSerializer(serializers.Serializer):
    # 传入时间格式
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    start_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("起始时间"))
    end_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("截止时间"))

    @classmethod
    def get_format_time(cls, time_str):
        time_array = time.strptime(time_str, cls.TIME_FORMAT)
        return int(time.mktime(time_array) * 1000)

    @classmethod
    def format_timestamp(cls, timestamp):
        time_array = time.localtime(timestamp / 1000)
        return time.strftime(cls.TIME_FORMAT, time_array)


class BatchCheckExecution(serializers.Serializer):
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    schedule_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("调度时间"))

    @classmethod
    def get_format_time(cls, time_str):
        time_array = time.strptime(time_str, cls.TIME_FORMAT)
        return int(time.mktime(time_array) * 1000)


class CreateDataMakeupSerializer(serializers.Serializer):
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    source_schedule_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("调度时间"))
    target_schedule_time = serializers.DateTimeField(format=TIME_FORMAT, label=_("调度时间"))
    with_child = serializers.BooleanField(label=_("同时补齐下游节点"))
    with_storage = serializers.BooleanField(label=_("下发到数据存储"))

    @classmethod
    def get_format_time(cls, time_str):
        time_array = time.strptime(time_str, cls.TIME_FORMAT)
        return int(time.mktime(time_array) * 1000)


class OpFlowSerializer(serializers.Serializer):
    flow_id = serializers.IntegerField(label=_("flow ID"))
    action = serializers.CharField(label=_("操作动作"))
    op_type = serializers.CharField(label=_("操作类型"), allow_null=True, required=False)


class AddPartitionSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label="结果表")
    partitions = serializers.IntegerField(label="目标扩充数")
    downstream_node_types = serializers.CharField(required=False, label="下游计算节点类型")
    strict_mode = serializers.BooleanField(required=False, label="True/False,是否严格模式，是否运行源和目标分区数相等时扩分区操作")
    # bk_username = serializers.CharField(label=u'操作者名称')


class ParamVerifySerializer(serializers.Serializer):
    scheduling_type = serializers.CharField(required=True, label="调度类型，batch/stream")
    scheduling_content = serializers.JSONField(required=True, label=_("调度配置"))
    from_nodes = serializers.JSONField(required=False, label=_("from节点配置"))
    to_nodes = serializers.JSONField(required=False, label=_("to节点配置"))

    def validate_scheduling_type(self, scheduling_type):
        support_scheduling_type = ["batch", "stream"]
        if scheduling_type not in support_scheduling_type:
            raise ValidError(_("非法调度类型，目前仅支持（%s）") % ", ".join(support_scheduling_type))
        return scheduling_type


class RestartStreamSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label="结果表")
    token = serializers.CharField(label="token")


class StopStreamSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label="结果表")
    token = serializers.CharField(label="token")


class StartStreamSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label="结果表")
    token = serializers.CharField(label="token")


class StopShipperStreamSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label="数据源ID")
    result_table_id = serializers.CharField(label="清洗的result table id")
    cluster_type = serializers.CharField(label="数据分发的存储类型")


class StartShipperStreamSerializer(serializers.Serializer):
    raw_data_id = serializers.IntegerField(label="数据源ID")
    result_table_id = serializers.CharField(label="清洗的result table id")
    cluster_type = serializers.CharField(label="数据分发的存储类型")


class GetLatestMsgSerializer(serializers.Serializer):
    result_table_id = serializers.CharField(label="result_table_id")
    node_type = serializers.CharField(label=_("当前节点类型"))

    def validate_node_type(self, value):
        SUPPORT_NODE_TYPES = NodeInstanceLink.validate_support_node_types()
        if value not in SUPPORT_NODE_TYPES:
            raise ValidError(
                _("非法操作(node_type=%(node_type)s)，目前仅支持节点类型（%(support_node_types)s）")
                % {
                    "node_type": value,
                    "support_node_types": ", ".join(SUPPORT_NODE_TYPES),
                }
            )
        return value


class NodeCodeFrameSerializer(serializers.Serializer):
    programming_language = serializers.CharField(label="func_language")
    node_type = serializers.CharField(label=_("当前节点类型"))
    input_result_table_ids = serializers.ListField(label="input result table ids")

    def validate_programming_language(self, programming_language):
        support_func_language = ["java", "python"]
        if programming_language not in support_func_language:
            raise ValidError(_("非法操作，目前仅支持（%s）") % ", ".join(support_func_language))
        return programming_language

    def validate_node_type(self, value):
        SUPPORT_NODE_TYPES = NodeInstanceLink.validate_support_node_types()
        if value not in SUPPORT_NODE_TYPES:
            raise ValidError(
                _("非法操作(node_type=%(node_type)s)，目前仅支持节点类型（%(support_node_types)s）")
                % {
                    "node_type": value,
                    "support_node_types": ", ".join(SUPPORT_NODE_TYPES),
                }
            )
        return value


class GetYarnLogSerializer(serializers.Serializer):
    container_info = serializers.JSONField(
        required=False,
        label=_("container信息{app_id, job_name, container_host, " "container_id}"),
    )
    log_info = serializers.JSONField(
        required=False,
        label=_("log信息{log_component_type, log_type, log_format, " "log_length}"),
    )
    pos_info = serializers.JSONField(required=False, label=_("日志位置信息{process_start, process_end}"))
    search_info = serializers.JSONField(
        required=False,
        label=_("日志检索信息{search_words, search_direction," "scroll_direction}"),
    )


class GetK8sLogSerializer(serializers.Serializer):
    container_info = serializers.JSONField(required=False, label=_("container信息{container_hostname, container_id}"))
    log_info = serializers.JSONField(required=False, label=_("log信息{log_component_type, log_type, log_format}"))
    pos_info = serializers.JSONField(required=False, label=_("日志位置信息{process_start, process_end}"))
    search_info = serializers.JSONField(
        required=False,
        label=_("日志检索信息{search_words, search_direction," "scroll_direction}"),
    )


class GetK8sContainerListSerializer(serializers.Serializer):
    job_name = serializers.CharField(label=_("job_name"))
    log_component_type = serializers.CharField(required=False, label=_("log_component_type, jobmanager / taskmanager"))


class GetYarnLogLengthSerializer(serializers.Serializer):
    app_id = serializers.CharField(label=_("app_id"))
    job_name = serializers.CharField(required=False, label=_("job_name"))
    log_component_type = serializers.CharField(label=_("component_type, executor/jobmanager/taskmanager"))
    log_type = serializers.CharField(required=False, label=_("log_type,日志类型[log|stdout]"))
    container_host = serializers.CharField(required=False, label=_("container_host,指定获取特定的container_host的日志数据"))
    container_id = serializers.CharField(required=False, label=_("container_id,指定获取特定的container_id的日志数据"))


class GetCommonContainerInfoSerializer(serializers.Serializer):
    app_id = serializers.CharField(required=False, label=_("app_id"))
    job_name = serializers.CharField(required=False, label=_("job_name"))
    log_component_type = serializers.CharField(
        required=False, label=_("component_type, executor/jobmanager/taskmanager")
    )
    log_type = serializers.CharField(required=False, label=_("log_type,日志类型[log|stdout]"))
    deploy_mode = serializers.CharField(required=False, label=_("deploy_mode,部署类型[yarn|k8s]"))


class GetFlowJobSubmitHistoySerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("job id"))
    start_time = serializers.CharField(required=False, label=_("start_time, 开始时间，格式: 2020-04-16 15:23:45"))
    end_time = serializers.CharField(required=False, label=_("end_time, 结束时间，格式: 2020-04-16 15:23:45"))
    get_app_id_flag = serializers.BooleanField(required=False, label=_("是否同时获取appid信息，如果True，返回时间可能较长"))


class GetFlowJobSubmitLogFileSizeSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("job id"), required=False)
    execute_id = serializers.CharField(required=False, label=_("execute id"))
    cluster_id = serializers.CharField(label="cluster id", required=False)
    geog_area_code = serializers.CharField(label="geog area code", required=False)


class GetFlowJobSubmitLogSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("job id"), required=False)
    cluster_id = serializers.CharField(label="cluster id", required=False)
    geog_area_code = serializers.CharField(label="geog area code", required=False)
    execute_id = serializers.CharField(required=False, label=_("execute id"))
    log_info = serializers.JSONField(required=False, label=_("log信息{log_type, log_format}"))
    pos_info = serializers.JSONField(required=False, label=_("日志位置信息{process_start, process_end}"))
    search_info = serializers.JSONField(
        required=False,
        label=_("日志检索信息{search_words, search_direction," "scroll_direction}"),
    )


class GetFlowJobAppIdSerializer(serializers.Serializer):
    job_id = serializers.CharField(label=_("job id"))
    execute_id = serializers.CharField(required=False, label=_("execute id"))


class JobSerializer(serializers.Serializer):
    module = serializers.ChoiceField(
        label="module",
        choices=(("stream", "stream"), ("batch", "batch"), ("model_app", "model_app")),
    )
    project_id = serializers.IntegerField(label=_("项目id"))
    cluster_group = serializers.CharField(required=False, allow_null=True, label=_("运行的集群组"))
    processings = serializers.ListField(label=_("processing列表"))
    code_version = serializers.CharField(label=_("代码号"), allow_null=True, required=False)
    deploy_config = serializers.DictField(label=_("部署参数"), allow_null=True, required=False)
    component_type = serializers.CharField(label=_("component_type"))
    job_config = serializers.DictField(label="job_config")
    deploy_config = serializers.DictField(required=False, label="deploy_config")

    def validate(self, attrs):
        if "deploy_config" not in attrs:
            attrs["deploy_config"] = {}
        return attrs


class StartJobSerializer(serializers.Serializer):
    is_restart = serializers.BooleanField(label=_("是否为重启操作"))
    tags = serializers.ListField(required=False, label=_("地域标签信息"))

    def validate(self, attrs):
        tags_validate(attrs)
        return attrs


class SyncStatusSerializer(serializers.Serializer):
    operate_info = serializers.CharField(label=_("运行的集群组"))

    class OperateInfoSerializer(serializers.Serializer):
        operate = serializers.ChoiceField(label=_("同步的操作类型"), choices=(("start", _("启动")), ("stop", _("停止"))))
        execute_id = serializers.IntegerField(label="execute_id", required=False)
        event_id = serializers.IntegerField(label="event_id", required=False)

    def validate(self, attrs):
        operate_info = attrs.get("operate_info")
        try:
            _operate_info = json.loads(operate_info)
            custom_params_valid(SyncStatusSerializer.OperateInfoSerializer, _operate_info)
        except Exception:
            raise ValidationError("Validate error: %s" % operate_info)
        return attrs


class TaskLogBlacklistSerializer(serializers.Serializer):
    group_name = serializers.CharField(required=False, label=_("黑名单名称"))
    blacklist = serializers.CharField(required=True, label=_("单个黑名单，纯字符串"))


class TaskLogBlacklistRetrieveSerializer(serializers.Serializer):
    force_flag = serializers.BooleanField(required=False, label=_("是否强制获取最新黑名单列表"))


class RetrieveJobSerializer(serializers.Serializer):
    run_mode = serializers.CharField(label=_("运行模式"))
    job_type = serializers.CharField(label=_("任务类型"))
