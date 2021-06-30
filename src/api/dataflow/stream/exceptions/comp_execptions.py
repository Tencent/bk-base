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

from django.utils.translation import ugettext_lazy as _

from dataflow.stream.exceptions.stream_exception import StreamException


class StreamCode(object):
    UNEXPECTED_ERR = ("999", _("非预期异常"))
    DEFAULT_ERR = ("000", _("可预期的异常"))
    JOB_LOCK_ERR = ("001", _("任务已被锁住"))
    JOBNAVI_ERR = ("002", _("调用jobnavi接口失败"))
    SOURCE_ERR = ("003", _("数据源不合法，请检查数据源的存储信息"))
    DATA_PROCESSING_ERR = ("004", _("数据处理逻辑不合法"))
    DELETE_PROCESSING_ERR = ("005", _("正在运行的任务不允许删除节点"))
    CLUSTER_NAME_ERR = ("006", _("集群名称不合法"))
    OUTPUTS_ERR = ("007", _("数据处理的输出不合法"))
    COMPONENT_ERR = ("008", _("组件不支持"))
    CLUSTER_NOT_FOUND_ERR = ("009", _("集群不存在"))
    SYNC_STORM_TOPOLOGY_ERR = ("010", _("同步实时任务状态失败, 请重试。"))
    CLUSTER_RESOURCES_LACK_ERR = ("011", _("实时计算资源不足，请联系数据平台管理员。"))
    SUBMIT_STORM_JOB_ERR = ("012", _("实时计算任务启动异常"))
    REDIS_OPERATING_ERR = ("013", _("redis操作异常"))
    KILL_STORM_JOB_ERR = ("014", _("实时计算任务停止异常"))
    GET_CODE_VERSION_ERR = ("015", _("获取代码版本异常，请联系管理员"))
    UPSTREAM_NODE_ERR = ("016", _("上游节点异常"))
    RESOURCE_EXIST_ERR = ("017", _("集群已存在，请检查"))
    ASSOCIATE_DATA_SOURCE_ERR = ("018", _("关联数据源异常"))
    PROCESSING_EXISTS_ERR = ("019", _("处理逻辑已存在"))
    SUBMIT_FLINK_JOB_ERR = ("020", _("实时任务启动异常"))
    CANCEL_FLINK_JOB_ERR = ("021", _("实时任务停止异常"))
    YARN_RESOURCES_LACK_ERR = ("022", _("实时计算资源不足，请联系数据平台管理员。"))
    JOB_NOT_EXISTS_ERR = ("023", _("任务不存在"))
    GROWS_BEYOND_64KB_ERR = ("024", _("运行时异常(SQL codgen大于64KB)，请检查实时节点。"))
    FLINK_STREAMING_ERR = ("025", _("运行时异常，请检查实时节点。"))
    STORM_NOT_SUPPORT_ALLOWED_LATENESS = ("026", _("当前任务不支持【计算延迟数据】，请联系数据平台管理员。"))
    REDIS_INFO_ERR = ("027", _("redis信息异常"))
    STORM_NOT_SUPPORT_STATIC_MULTI_KEY = ("028", _("当前任务不支持多个字段的关联，请创建新任务再进行操作。"))
    SINK_ERR = ("029", _("数据输出不合法，请检查数据输出的存储信息"))
    CODE_CHECK_ERR = ("030", _("代码不符合要求"))
    STORM_NOT_SUPPORT_IPCITY_JOIN = ("030", _("当前任务为历史任务，不支持IpCity的IP关联，请创建新任务。"))
    DELETE_JOB_ERR = ("031", _("删除作业配置失败"))
    FTP_SERVER_INFO_ERR = ("032", _("不同地域不可使用相同文件服务器"))
    RESULT_TABLE_EXISTS_ERR = ("033", _("结果表已存在，不允许重复创建"))
    CODE_VERSION_NOT_FOUND_ERR = ("034", _("当前 CODE 默认版本未配置，请联系管理员。"))
    SUBMIT_JOB_GEOG_AREA_TAG_ERR = ("035", _("获取当前提交任务地区标签为空，请联系系统管理员。"))
    PROCESSING_PARAMETER_NOT_EXISTS_ERR = ("036", _("处理逻辑参数不能为空"))
    STREAM_WINDOW_CONFIG_CHECK_ERR = ("037", _("实时计算窗口校验错误，请联系数据平台管理员。"))
    PERMISSION_DENIED_ERR = ("038", _("没有操作权限"))
    JOB_EXISTS_ERR = ("039", _("任务已存在"))
    YAML_FORMAT_ERR = ("040", _("yaml 格式错误"))
    RESOURCE_NOT_FOUND_ERR = ("041", _("无法找到有效的资源选项"))
    FLINK_CLUSTER_TYPE_ERR = ("042", _("不支持的flink集群类型"))


class StreamExpectedException(StreamException):
    """
    可预期的异常
    """

    def __init__(self, *args):
        raw_message = args[0] if len(args) > 0 else self.MESSAGE
        self.MESSAGE = raw_message
        super(StreamExpectedException, self).__init__(raw_message)


class StreamUnexpectedException(StreamException):
    """
    未知异常
    """

    CODE = StreamCode.UNEXPECTED_ERR[0]
    MESSAGE = StreamCode.UNEXPECTED_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(ComponentNotSupportError, self).__init__(self.MESSAGE)


class JobLockError(StreamExpectedException):
    CODE = StreamCode.JOB_LOCK_ERR[0]
    MESSAGE = StreamCode.JOB_LOCK_ERR[1]


class JobNaviError(StreamExpectedException):
    CODE = StreamCode.JOBNAVI_ERR[0]
    MESSAGE = StreamCode.JOBNAVI_ERR[1]


class SourceIllegal(StreamExpectedException):
    CODE = StreamCode.SOURCE_ERR[0]
    MESSAGE = StreamCode.SOURCE_ERR[1]


class SinkIllegal(StreamExpectedException):
    CODE = StreamCode.SINK_ERR[0]
    MESSAGE = StreamCode.SINK_ERR[1]


class DataProcessingError(StreamExpectedException):
    CODE = StreamCode.DATA_PROCESSING_ERR[0]
    MESSAGE = StreamCode.DATA_PROCESSING_ERR[1]


class DeleteProcessingError(StreamExpectedException):
    CODE = StreamCode.DELETE_PROCESSING_ERR[0]
    MESSAGE = StreamCode.DELETE_PROCESSING_ERR[1]


class ClusterNameIllegalError(StreamExpectedException):
    CODE = StreamCode.CLUSTER_NAME_ERR[0]
    MESSAGE = StreamCode.CLUSTER_NAME_ERR[1]


class OutputsIllegalError(StreamExpectedException):
    CODE = StreamCode.OUTPUTS_ERR[0]
    MESSAGE = StreamCode.OUTPUTS_ERR[1]


class ComponentNotSupportError(StreamExpectedException):
    CODE = StreamCode.COMPONENT_ERR[0]
    MESSAGE = StreamCode.COMPONENT_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(ComponentNotSupportError, self).__init__(self.MESSAGE)


class ClusterNotFoundError(StreamExpectedException):
    CODE = StreamCode.CLUSTER_NOT_FOUND_ERR[0]
    MESSAGE = StreamCode.CLUSTER_NOT_FOUND_ERR[1]


class SyncStormTopologyError(StreamExpectedException):
    CODE = StreamCode.SYNC_STORM_TOPOLOGY_ERR[0]
    MESSAGE = StreamCode.SYNC_STORM_TOPOLOGY_ERR[1]


class ClusterResourcesLackError(StreamExpectedException):
    CODE = StreamCode.CLUSTER_RESOURCES_LACK_ERR[0]
    MESSAGE = StreamCode.CLUSTER_RESOURCES_LACK_ERR[1]


class SubmitStormJobError(StreamExpectedException):
    CODE = StreamCode.SUBMIT_STORM_JOB_ERR[0]
    MESSAGE = StreamCode.SUBMIT_STORM_JOB_ERR[1]


class SubmitFlinkJobError(StreamExpectedException):
    CODE = StreamCode.SUBMIT_FLINK_JOB_ERR[0]
    MESSAGE = StreamCode.SUBMIT_FLINK_JOB_ERR[1]


class RedisOperatingError(StreamExpectedException):
    CODE = StreamCode.REDIS_OPERATING_ERR[0]
    MESSAGE = StreamCode.REDIS_OPERATING_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(RedisOperatingError, self).__init__(self.MESSAGE)


class KillStormJobError(StreamExpectedException):
    CODE = StreamCode.KILL_STORM_JOB_ERR[0]
    MESSAGE = StreamCode.KILL_STORM_JOB_ERR[1]


class CancelFlinkJobError(StreamExpectedException):
    CODE = StreamCode.CANCEL_FLINK_JOB_ERR[0]
    MESSAGE = StreamCode.CANCEL_FLINK_JOB_ERR[1]


class GetCodeVersionError(StreamExpectedException):
    CODE = StreamCode.GET_CODE_VERSION_ERR[0]
    MESSAGE = StreamCode.GET_CODE_VERSION_ERR[1]


class UpstreamError(StreamExpectedException):
    CODE = StreamCode.UPSTREAM_NODE_ERR[0]
    MESSAGE = StreamCode.UPSTREAM_NODE_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(UpstreamError, self).__init__(self.MESSAGE)


class ResourceExistError(StreamExpectedException):
    CODE = StreamCode.RESOURCE_EXIST_ERR[0]
    MESSAGE = StreamCode.RESOURCE_EXIST_ERR[1]


class AssociateDataSourceError(StreamExpectedException):
    CODE = StreamCode.ASSOCIATE_DATA_SOURCE_ERR[0]
    MESSAGE = StreamCode.ASSOCIATE_DATA_SOURCE_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(AssociateDataSourceError, self).__init__(self.MESSAGE)


class ProcessingExistsError(StreamExpectedException):
    CODE = StreamCode.PROCESSING_EXISTS_ERR[0]
    MESSAGE = StreamCode.PROCESSING_EXISTS_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(ProcessingExistsError, self).__init__(self.MESSAGE)


class YarnResourcesLackError(StreamExpectedException):
    CODE = StreamCode.YARN_RESOURCES_LACK_ERR[0]
    MESSAGE = StreamCode.YARN_RESOURCES_LACK_ERR[1]


class GrowsBeyond64KBError(StreamExpectedException):
    CODE = StreamCode.GROWS_BEYOND_64KB_ERR[0]
    MESSAGE = StreamCode.GROWS_BEYOND_64KB_ERR[1]


class JobNotExistsError(StreamExpectedException):
    CODE = StreamCode.JOB_NOT_EXISTS_ERR[0]
    MESSAGE = StreamCode.JOB_NOT_EXISTS_ERR[1]


class FlinkStreamingError(StreamExpectedException):
    CODE = StreamCode.FLINK_STREAMING_ERR[0]
    MESSAGE = StreamCode.FLINK_STREAMING_ERR[1]


class FlinkStreamYarnResourceLackError(StreamExpectedException):
    CODE = StreamCode.YARN_RESOURCES_LACK_ERR[0]
    MESSAGE = StreamCode.YARN_RESOURCES_LACK_ERR[1]


class StormNotSupportAllowedLatenessError(StreamExpectedException):
    CODE = StreamCode.STORM_NOT_SUPPORT_ALLOWED_LATENESS[0]
    MESSAGE = StreamCode.STORM_NOT_SUPPORT_ALLOWED_LATENESS[1]


class CheckpointInfoError(StreamExpectedException):
    CODE = StreamCode.REDIS_INFO_ERR[0]
    MESSAGE = StreamCode.REDIS_INFO_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(CheckpointInfoError, self).__init__(self.MESSAGE)


class StormNotSupportStaticMultiKeyError(StreamExpectedException):
    CODE = StreamCode.STORM_NOT_SUPPORT_STATIC_MULTI_KEY[0]
    MESSAGE = StreamCode.STORM_NOT_SUPPORT_STATIC_MULTI_KEY[1]


class StreamCodeCheckError(StreamExpectedException):
    CODE = StreamCode.CODE_CHECK_ERR[0]
    MESSAGE = StreamCode.CODE_CHECK_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(StreamCodeCheckError, self).__init__(self.MESSAGE)


class StormNotSupportIpCityJoinError(StreamExpectedException):
    CODE = StreamCode.STORM_NOT_SUPPORT_IPCITY_JOIN[0]
    MESSAGE = StreamCode.STORM_NOT_SUPPORT_IPCITY_JOIN[1]


class DeleteJobError(StreamExpectedException):
    CODE = StreamCode.DELETE_JOB_ERR[0]
    MESSAGE = StreamCode.DELETE_JOB_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(DeleteJobError, self).__init__(self.MESSAGE)


class FtpServerInfoError(StreamExpectedException):
    CODE = StreamCode.FTP_SERVER_INFO_ERR[0]
    MESSAGE = StreamCode.FTP_SERVER_INFO_ERR[1]


class ResultTableExistsError(StreamExpectedException):
    CODE = StreamCode.RESULT_TABLE_EXISTS_ERR[0]
    MESSAGE = StreamCode.RESULT_TABLE_EXISTS_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(ResultTableExistsError, self).__init__(self.MESSAGE)


class CodeVersionNotFoundError(StreamExpectedException):
    CODE = StreamCode.CODE_VERSION_NOT_FOUND_ERR[0]
    MESSAGE = StreamCode.CODE_VERSION_NOT_FOUND_ERR[1]


class SubmitJobGeogAreaTagError(StreamExpectedException):
    CODE = StreamCode.SUBMIT_JOB_GEOG_AREA_TAG_ERR[0]
    MESSAGE = StreamCode.SUBMIT_JOB_GEOG_AREA_TAG_ERR[1]


class ProcessingParameterNotExistsError(StreamExpectedException):
    CODE = StreamCode.PROCESSING_PARAMETER_NOT_EXISTS_ERR[0]
    MESSAGE = StreamCode.PROCESSING_PARAMETER_NOT_EXISTS_ERR[1]


class YamlJobLockError(StreamExpectedException):
    CODE = StreamCode.JOB_LOCK_ERR[0]
    MESSAGE = StreamCode.JOB_LOCK_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(YamlJobLockError, self).__init__(self.MESSAGE)


class YamlOperationPermissionDeniedError(StreamExpectedException):
    CODE = StreamCode.PERMISSION_DENIED_ERR[0]
    MESSAGE = StreamCode.PERMISSION_DENIED_ERR[1]


class YamlDeleteProcessingError(StreamExpectedException):
    CODE = StreamCode.DELETE_PROCESSING_ERR[0]
    MESSAGE = StreamCode.DELETE_PROCESSING_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(YamlDeleteProcessingError, self).__init__(self.MESSAGE)


class StreamWindowConfigCheckError(StreamExpectedException):
    CODE = StreamCode.STREAM_WINDOW_CONFIG_CHECK_ERR[0]
    MESSAGE = StreamCode.STREAM_WINDOW_CONFIG_CHECK_ERR[1]


class JobExistsError(StreamExpectedException):
    CODE = StreamCode.JOB_EXISTS_ERR[0]
    MESSAGE = StreamCode.JOB_EXISTS_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE == args[0]
        super(JobExistsError, self).__init__(self.MESSAGE)


class YamlFormatError(StreamExpectedException):
    CODE = StreamCode.YAML_FORMAT_ERR[0]
    MESSAGE = StreamCode.YAML_FORMAT_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(YamlFormatError, self).__init__(self.MESSAGE)


class StreamResourceNotFoundError(StreamExpectedException):
    CODE = StreamCode.RESOURCE_NOT_FOUND_ERR[0]
    MESSAGE = StreamCode.RESOURCE_NOT_FOUND_ERR[1]


class FlinkClusterTypeError(StreamExpectedException):
    CODE = StreamCode.FLINK_CLUSTER_TYPE_ERR[0]
    MESSAGE = StreamCode.FLINK_CLUSTER_TYPE_ERR[1]
