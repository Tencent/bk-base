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

from common.base_utils import custom_params_valid
from common.exceptions import ValidationError
from django.utils.translation import ugettext as _
from rest_framework import serializers


class DeploySerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class JobV1Serializer(serializers.Serializer):
    class JobserverConfigSerializer(serializers.Serializer):
        geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
        cluster_id = serializers.CharField(required=True, label=_("jobnavi集群ID"))

    api_version = serializers.CharField(required=False, label=_("api_version"))
    processing_id = serializers.CharField(label=_("处理ID"))
    code_version = serializers.CharField(required=False, allow_null=True, label=_("代码版本"))
    cluster_group = serializers.CharField(required=False, label=_("集群组"))
    cluster_name = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("细分集群名称"))
    deploy_mode = serializers.CharField(required=False, label=_("部署模式"))
    deploy_config = serializers.CharField(required=False, label=_("部署配置"))
    jobserver_config = JobserverConfigSerializer(required=True, label=_("作业服务配置"))
    job_config = serializers.DictField(required=False, label=_("作业配置"))

    def validate(self, attr):
        value = attr
        if (
            "job_config" in value
            and "submit_args" in value["job_config"]
            and isinstance(value["job_config"]["submit_args"], dict)
            and "batch_type" in value["job_config"]["submit_args"]
            and value["job_config"]["submit_args"]["batch_type"] == "spark_python_code"
        ):
            job_config = custom_params_valid(BatchCodeDictSerializer, value["job_config"])
            value["job_config"] = job_config
            return value
        else:
            return value


class JobV2Serializer(serializers.Serializer):
    class JobserverConfigSerializer(serializers.Serializer):
        geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
        cluster_id = serializers.CharField(required=True, label=_("jobnavi集群ID"))

    api_version = serializers.CharField(required=False, label=_("api_version"))
    processing_id = serializers.CharField(label=_("处理ID"))
    code_version = serializers.CharField(required=False, allow_null=True, label=_("代码版本"))
    cluster_group = serializers.CharField(required=False, label=_("集群组"))
    cluster_name = serializers.CharField(required=False, allow_blank=True, allow_null=True, label=_("细分集群名称"))
    deploy_mode = serializers.CharField(required=False, label=_("部署模式"))
    deploy_config = serializers.CharField(required=False, label=_("部署配置"))
    jobserver_config = JobserverConfigSerializer(required=True, label=_("作业服务配置"))
    job_config = serializers.DictField(required=False, label=_("作业配置"))


class JobSerializer(serializers.Serializer):
    api_version = serializers.CharField(required=False, label=_("api_version"))

    def validate(self, attr):
        if "api_version" in attr and attr["api_version"] == "v2":
            params = custom_params_valid(JobV2Serializer, self.initial_data)
            params["api_version"] = attr["api_version"]
            return params
        else:
            return custom_params_valid(JobV1Serializer, self.initial_data)


class BatchCodeDictSerializer(serializers.Serializer):
    class SDKProcessorLogicSerializer(serializers.Serializer):
        user_main_class = serializers.CharField(required=True, label=_("sdk入口函数"))
        user_package_path = serializers.CharField(required=True, label=_("sdk包路径"))
        user_args = serializers.CharField(required=True, allow_blank=True, label=_("用户"))

    class JobInfoSubmitArgsSerializer(serializers.Serializer):
        class OutPutSerializer(serializers.Serializer):
            data_set_id = serializers.CharField(required=True, label=_("输出表id"))
            data_set_type = serializers.CharField(required=True, label=_("输出表类型"))

        class AdvancedSerializer(serializers.Serializer):
            start_time = serializers.IntegerField(required=False, allow_null=True, label=_("启动时间"))
            force_execute = serializers.BooleanField(required=False, label=_("创建就执行"))
            self_dependency = serializers.BooleanField(required=False, label=_("自依赖"))
            recovery_enable = serializers.BooleanField(required=False, label=_("任务可失败重试"))
            recovery_times = serializers.IntegerField(required=False, label=_("重试次数"))
            recovery_interval = serializers.CharField(required=False, label=_("重试间隔"))
            engine_conf = serializers.DictField(required=False, label=_("engine conf"))
            self_dependency_config = serializers.DictField(required=False, label=_("自依赖配置"))

        count_freq = serializers.IntegerField(required=True, label=_("统计频率"))
        delay = serializers.IntegerField(required=True, label=_("统计延迟"))
        schedule_period = serializers.CharField(required=True, label=_("schedule_period"))
        result_tables = serializers.DictField(required=True, label=_("表信息"))
        outputs = OutPutSerializer(many=True, label=_("处理的输出信息"))
        accumulate = serializers.BooleanField(required=True, label=_("是否为累加"))
        data_start = serializers.IntegerField(required=True, label=_("数据起始时间"))
        data_end = serializers.IntegerField(required=True, label=_("数据截止时间"))
        batch_type = serializers.CharField(required=True, label=_("batch_type"))
        advanced = AdvancedSerializer(required=False, label=_("高级选项"))

        def validate_result_tables(self, value):
            class ResultTablesSerializer(serializers.Serializer):
                window_size = serializers.IntegerField(required=True, label=_("window_size"))
                window_delay = serializers.IntegerField(required=True, label=_("window_delay"))
                window_size_period = serializers.CharField(required=True, label=_("window_size_period"))
                is_managed = serializers.IntegerField(required=True, label=_("is_managed"))
                dependency_rule = serializers.CharField(required=True, label=_("dependency_rule"))
                type = serializers.CharField(required=True, label=_("type"))

            for k, v in list(value.items()):
                custom_params_valid(serializer=ResultTablesSerializer, params=v)
            return value

    processor_type = serializers.CharField(required=True, label=_("processor_type"))
    component_type = serializers.CharField(required=True, label=_("component_type"))
    processor_logic = SDKProcessorLogicSerializer(required=True, label=_("SDK配置"))
    submit_args = JobInfoSubmitArgsSerializer(required=True, label=_("submit_args"))


class QueueSerializer(serializers.Serializer):
    name = serializers.CharField(label=_("队列名"))


class JobRetrySerializer(serializers.Serializer):
    schedule_time = serializers.IntegerField(label=_("调度时间"))
    data_dir = serializers.CharField(label=_("数据路径"))
    batch_storage_type = serializers.CharField(required=False, label=_("存储类型"))


class JobExecuteSerializer(serializers.Serializer):
    data_time = serializers.CharField(label=_("数据时间"))


class TableSerializer(serializers.Serializer):
    window_size = serializers.IntegerField(label=_("窗口长度"))
    window_size_period = serializers.CharField(label=_("窗口长度周期"))
    window_delay = serializers.IntegerField(label=_("窗口延迟"))
    dependency_rule = serializers.CharField(label=_("依赖规则"))


class ProcessingsBatchV1Serializer(serializers.Serializer):
    class DictSerializer(serializers.Serializer):
        class AdvancedSerializer(serializers.Serializer):
            start_time = serializers.IntegerField(required=False, allow_null=True, label=_("启动时间"))
            exec_oncreate = serializers.BooleanField(required=False, label=_("创建就执行"))
            self_dependency = serializers.BooleanField(required=False, label=_("自依赖"))
            recovery_enable = serializers.BooleanField(required=False, label=_("任务可失败重试"))
            recovery_times = serializers.IntegerField(required=False, label=_("重试次数"))
            recovery_interval = serializers.CharField(required=False, label=_("重试间隔"))

        count_freq = serializers.IntegerField(label=_("统计频率"))
        description = serializers.CharField(label=_("描述信息"))
        accumulate = serializers.BooleanField(label=_("是否为累加"))
        data_start = serializers.IntegerField(label=_("数据起始时间"))
        data_end = serializers.IntegerField(label=_("数据截止时间"))
        delay = serializers.IntegerField(label=_("延迟"))
        schedule_period = serializers.CharField(label=_("调度周期"))
        cluster = serializers.CharField(label=_("集群信息"))
        advanced = AdvancedSerializer(required=False, label=_("高级选项"))

    class OutPutSerializer(serializers.Serializer):
        bk_biz_id = serializers.IntegerField(label=_("蓝鲸业务ID"))
        table_name = serializers.CharField(label=_("结果表名"))

    sql = serializers.CharField(required=False, allow_null=True, allow_blank=True, label=_("sql"))
    dict = DictSerializer(label=_("配置字典"))
    project_id = serializers.IntegerField(label=_("项目标识"))
    result_tables = serializers.DictField(label=_("表信息"))
    processing_id = serializers.CharField(required=False, label=_("处理ID"))
    outputs = OutPutSerializer(many=True, label=_("处理的输出信息"))
    tags = serializers.ListField(label=_("标签列表"), allow_empty=False)

    def validate_result_tables(self, value):
        if not self.initial_data["dict"]["accumulate"]:
            for k, v in list(value.items()):
                custom_params_valid(serializer=TableSerializer, params=v)
        return value

    def validate_sql(self, value):
        if self.initial_data["dict"]["batch_type"] != "tdw_jar" and not value:
            raise ValidationError(_("sql不能为空"))
        return value


class ProcessingsBatchSerializer(serializers.Serializer):

    api_version = serializers.CharField(required=False, label=_("api_version"))

    def validate(self, attr):
        if "api_version" in attr and attr["api_version"] == "v2":
            params = custom_params_valid(ProcessingsBatchV2Serializer, self.initial_data)
            params["api_version"] = attr["api_version"]
            return params
        else:
            return custom_params_valid(ProcessingsBatchV1Serializer, self.initial_data)


class HDFSUtilSerializer(serializers.Serializer):
    path = serializers.CharField(label=_("路径信息"))


class HDFSUploadSerializer(serializers.Serializer):
    path = serializers.CharField(label=_("路径信息"))
    file = serializers.FileField(label=_("文件"))


class HDFSMoveSerializer(serializers.Serializer):
    is_overwrite = serializers.BooleanField(label=_("是否覆盖"))
    from_path = serializers.CharField(label=_("源路径信息"))
    to_path = serializers.CharField(label=_("目标路径信息"))


class HDFSCleanSerializer(serializers.Serializer):
    paths = serializers.ListField(label=_("路径信息列表"), child=serializers.CharField(label=_("path")))


class ConfigSerializer(serializers.Serializer):
    cluster_domain = serializers.CharField(required=True, label=_("对应组件主节点域名"))
    cluster_group = serializers.CharField(required=True, label=_("集群组"))
    cluster_name = serializers.RegexField(
        required=True,
        regex=r"^[a-zA-Z]+(_|[a-zA-Z0-9]|\.)*$",
        label=_("集群名称"),
        error_messages={"invalid": _("由英文字母、数字、下划线、小数点组成，且需以字母开头")},
    )
    version = serializers.CharField(required=True, label=_("集群版本"))
    component_type = serializers.CharField(required=True, label=_("組件类型"))
    cluster_label = serializers.CharField(required=False, label=_("集群标签"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
    cpu = serializers.FloatField(required=False, label=_("CPU"))
    memory = serializers.FloatField(required=False, label=_("内存"))
    disk = serializers.FloatField(required=False, label=_("磁盘"))


class GetNodeInfoSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "result_table_id": "xxxx"
    }
    """

    result_table_id = serializers.CharField(label=_("result table id"))


class DebugSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class DebugCreateSerializer(DebugSerializer):
    heads = serializers.CharField(required=True, label=_("heads"))
    tails = serializers.CharField(required=True, label=_("tails"))


class SaveErrorDataSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": "1234",
        "result_table_id": "1_abc",
        "error_code": "xxx",
        "error_message": "xxxxx",
        "debug_date": "2018-09-19 17:40:58"
    }
    """

    job_id = serializers.CharField(label=_("job id"))
    result_table_id = serializers.CharField(label=_("result table id"))
    error_code = serializers.CharField(label=_("error code"))
    error_message = serializers.CharField(label=_("error message"))
    error_message_en = serializers.CharField(label=_("error message for English"))
    debug_date = serializers.DecimalField(max_digits=13, decimal_places=0, label=_("debug date"), allow_null=True)


class MetricInfoSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": "1234",
        "input_total_count": 100,
        "output_total_count": 100,
        "result_table_id": "2_abc"
    }
    """

    job_id = serializers.CharField(label=_("job id"))
    input_total_count = serializers.IntegerField(label=_("input count"))
    output_total_count = serializers.IntegerField(label=_("output count"))
    result_table_id = serializers.CharField(label=_("result table id"))


class SaveResultDataSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "job_id": "1234",
        "result_table_id": "1_abc",
        "result_data": "[{"gsid": 130011101},{"gsid": "xxxxxx"}]",
        "debug_date": 1537152075939,
        "thedate": 20180917
    }
    """

    job_id = serializers.CharField(label=_("job id"))
    result_table_id = serializers.CharField(label=_("result table id"))
    result_data = serializers.CharField(label=_("result data"))
    debug_date = serializers.DecimalField(max_digits=13, decimal_places=0, label=_("debug date"), allow_null=True)
    thedate = serializers.IntegerField(label=_("the date"))


class StartCustomCalculateSerializer(serializers.Serializer):
    custom_calculate_id = serializers.CharField(required=True, label=_("custom_calculate_id"))
    data_start = serializers.IntegerField(required=True, label=_("起始时间"))
    data_end = serializers.IntegerField(required=True, label=_("截止时间"))
    type = serializers.CharField(required=True, label=_("自定义计算类型"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
    rerun_processings = serializers.CharField(required=True, label=_("rerun_processings"))
    rerun_model = serializers.CharField(required=False, label=_("rerun_model"))


class StopCustomCalculateSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class CustomCalculateListExecuteSerializer(serializers.Serializer):
    processing_id = serializers.CharField(required=True, label=_("processing_id"))


class CustomCalculateAnalyzeSerializer(serializers.Serializer):
    rerun_processings = serializers.CharField(required=True, label=_("rerun_processings"))
    rerun_model = serializers.CharField(required=False, label=_("rerun_model"))
    data_start = serializers.IntegerField(required=True, label=_("起始时间"))
    data_end = serializers.IntegerField(required=True, label=_("截止时间"))
    type = serializers.CharField(required=True, label=_("自定义计算类型"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class CreateDataMakeupSerializer(serializers.Serializer):
    processing_id = serializers.CharField(required=True, label=_("processing_id"))
    rerun_processings = serializers.CharField(required=True, allow_blank=True, label=_("rerun_processings"))
    rerun_model = serializers.CharField(required=False, label=_("rerun_model"))
    target_schedule_time = serializers.IntegerField(required=True, label=_("截止调度时间"))
    source_schedule_time = serializers.IntegerField(required=True, label=_("起始调度时间"))
    with_storage = serializers.BooleanField(required=True, label=_("起始调度时间"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class DataMakeupStatusListSerializer(serializers.Serializer):
    data_start = serializers.IntegerField(required=True, label=_("截止时间"))
    data_end = serializers.IntegerField(required=True, label=_("起始时间"))
    processing_id = serializers.CharField(required=True, label=_("processing_id"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class DataMakeupCheckExecSerializer(serializers.Serializer):
    schedule_time = serializers.IntegerField(required=True, label=_("起始时间"))
    processing_id = serializers.CharField(required=True, label=_("processing_id"))
    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))


class DataMakeupSqlColumnSerializer(serializers.Serializer):
    sql = serializers.CharField(required=True, label=_("sql"))


class InteractiveCreateSerializer(serializers.Serializer):
    server_id = serializers.CharField(required=True, label=_("server_id"))
    bk_user = serializers.CharField(required=True, label=_("bk_user"))
    bk_project_id = serializers.IntegerField(required=False, label=_("bk_project_id"))
    node_label = serializers.CharField(required=False, label=_("node_label"), allow_null=True)
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))
    preload_py_files = serializers.ListField(required=False, label=_("preload_py_files"), allow_empty=True)
    preload_files = serializers.ListField(required=False, label=_("preload_files"), allow_empty=True)
    preload_jars = serializers.ListField(required=False, label=_("preload_jars"), allow_empty=True)
    cluster_group = serializers.CharField(required=False, label=_("集群组"))
    engine_conf = serializers.DictField(required=False, label=_("engine_conf"))


class InteractiveServerCreateSerializer(serializers.Serializer):
    node_label = serializers.CharField(required=False, label=_("node_label"), allow_null=True)
    cluster_group = serializers.CharField(required=False, label=_("cluster_group"))
    bk_user = serializers.CharField(required=False, label=_("bk_user"))
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))


class InteractiveServersStatusSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))


class InteractiveKillServersSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))


class InteractiveCreateSessionSerializer(serializers.Serializer):
    bk_user = serializers.CharField(required=True, label=_("bk_user"))
    bk_project_id = serializers.IntegerField(required=False, label=_("bk_project_id"))
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))
    preload_py_files = serializers.ListField(required=False, label=_("preload_py_files"), allow_empty=True)
    preload_files = serializers.ListField(required=False, label=_("preload_files"), allow_empty=True)
    preload_jars = serializers.ListField(required=False, label=_("preload_jars"), allow_empty=True)
    engine_conf = serializers.DictField(required=False, label=_("engine_conf"))


class InteractiveSessionStatusSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))


class InteractiveRunCodeSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))
    code = serializers.CharField(required=True, label=_("code"))
    context = serializers.ListField(required=False, label=_("代码检测"), allow_empty=True)
    blacklist_group_name = serializers.CharField(required=False, label=_("blacklist_group_name"))


class InteractiveCodeStatusSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))


class InteractiveCodeCancelSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))


class CustomJobCreateSerializer(serializers.Serializer):
    job_id = serializers.CharField(required=True, label=_("job_id"))
    job_type = serializers.CharField(required=True, label=_("job_type"))
    project_id = serializers.CharField(required=False, label=_("project_id"))
    username = serializers.CharField(required=True, label=_("username"))
    processing_type = serializers.CharField(required=True, label=_("processing_type"))
    processing_logic = serializers.CharField(required=True, label=_("processing_logic"))
    node_label = serializers.CharField(required=False, label=_("node_label"))
    cluster_group = serializers.CharField(required=False, label=_("cluster_group"))
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))
    tags = serializers.ListField(required=False, label=_("tags"), allow_empty=True)
    inputs = serializers.ListField(required=True, label=_("inputs"), allow_empty=False)
    outputs = serializers.ListField(required=True, label=_("outputs"), allow_empty=False)
    engine_conf = serializers.DictField(required=False, label=_("engine_conf"))
    deploy_config = serializers.DictField(required=False, label=_("deploy_config"))
    queryset_params = serializers.DictField(required=False, label=_("queryset_params"))


class CustomJobDeleteSerializer(serializers.Serializer):
    queryset_delete = serializers.BooleanField(required=False, label=_("queryset_delete"))


class RegisterExpireCustomJobsSerializer(serializers.Serializer):
    geog_area_code = serializers.CharField(required=True, label=_("geog_area_code"))


class JobsUpdateNodeLabelSerializer(serializers.Serializer):
    node_label = serializers.CharField(required=True, label=_("node_label"), allow_null=True)


class JobsUpdateEngineConfSerializer(serializers.Serializer):
    engine_conf = serializers.DictField(
        required=False,
        label=_("engine_conf"),
        allow_null=True,
        child=serializers.CharField(),
    )


class ProcessingUpdateBatchAdvanceJobConfigSerializer(serializers.Serializer):
    start_time = serializers.IntegerField(required=False, allow_null=True, label=_("启动时间"))
    force_execute = serializers.BooleanField(required=False, label=_("创建就执行"))
    self_dependency = serializers.BooleanField(required=False, label=_("自依赖"))
    recovery_enable = serializers.BooleanField(required=False, label=_("任务可失败重试"))
    recovery_times = serializers.IntegerField(required=False, label=_("重试次数"))
    recovery_interval = serializers.CharField(required=False, label=_("重试间隔"))
    self_dependency_config = serializers.DictField(required=False, label=_("self_dependency_config"))
    engine_conf = serializers.DictField(
        required=False,
        label=_("engine_conf"),
        allow_null=True,
        child=serializers.CharField(),
    )


class ProcessingsBatchV2Serializer(serializers.Serializer):
    outputs = serializers.ListField(required=True, label=_("outputs"))
    dedicated_config = serializers.DictField(required=True, label=_("dedicated_config"))
    window_info = serializers.ListField(required=True, label=_("window_info"))
    project_id = serializers.CharField(required=True, label=_("project_id"))
    tags = serializers.ListField(required=True, label=_("标签列表"), allow_empty=False)


class ProcessingCheckBatchParamSerializer(serializers.Serializer):
    scheduling_type = serializers.CharField(required=False, label=_("scheduling_type"))
    scheduling_content = serializers.DictField(required=False, label=_("scheduling_content"))
    from_nodes = serializers.ListField(required=False, label=_("from_nodes"))
    to_nodes = serializers.ListField(required=False, label=_("to_nodes"))
