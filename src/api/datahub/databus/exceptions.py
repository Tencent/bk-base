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

from common.errorcodes import ErrorCode as BkErrorCode
from common.exceptions import BaseAPIError
from django.utils.translation import ugettext_lazy as _


class DatabusAPIError(BaseAPIError):
    MODULE_CODE = BkErrorCode.BKDATA_DATAAPI_DATABUS


class CleanConfVerifyError(DatabusAPIError):
    CODE = "001"
    MESSAGE = _(u"PipeExtractorException")


class ProcessingNotFound(DatabusAPIError):
    CODE = "002"
    MESSAGE = _(u"Processing id not found")


class ChannelNotFound(DatabusAPIError):
    CODE = "003"
    MESSAGE = "Channel not found"


class ImportHdfsNotNotFound(DatabusAPIError):
    CODE = "004"
    MESSAGE = _(u"查询离线导入任务失败,任务Id:{task_id}")


class ImportHdfsHistoryCleanError(DatabusAPIError):
    CODE = "005"
    MESSAGE = _(u"删除{days}天以前的离线导入历史任务失败！")


class ImportHdfsEarliestGetError(DatabusAPIError):
    CODE = "006"
    MESSAGE = _(u"查询最近离线导入任务列表失败！")


class ImportHdfsTaskUpdateError(DatabusAPIError):
    CODE = "007"
    MESSAGE = _(
        u"更新离线导入任务失败,任务Id:{task_id}, result_table_id:{result_table_id}, data_dir:{data_dir}, finished:{finished}"
    )


class ImportHdfsCreateError(DatabusAPIError):
    CODE = "008"
    MESSAGE = _(u"离线导入任务创建失败！")


class NotSupportEventStorageError(DatabusAPIError):
    CODE = "009"
    MESSAGE = _(u"总线事件创建失败，不支持的存储类型:{storage}!")


class StorageEventNotifyJobError(DatabusAPIError):
    CODE = "019"
    MESSAGE = _(u"总线事件调用离线任务失败,result_table_id:{result_table_id}")


class StorageEventCreateError(DatabusAPIError):
    CODE = "026"
    MESSAGE = _(u"总线事件创建失败,result_table_id:{result_table_id}")


class CreateResultTableError(DatabusAPIError):
    CODE = "010"
    MESSAGE = _(u"创建ResultTable失败：{rt_id}")


class UpdateResultTableError(DatabusAPIError):
    CODE = "011"
    MESSAGE = _(u"更新ResultTable失败：{message}")


class CreateRtStorageError(DatabusAPIError):
    CODE = "012"
    MESSAGE = _(u"创建ResultTable的存储失败：{message}")


class NoAvailableInnerChannelError(DatabusAPIError):
    CODE = "013"
    MESSAGE = _(u"没有可用的inner channel，请先初始化inner channel的信息")


class CleanConfigError(DatabusAPIError):
    CODE = "014"
    MESSAGE = _(u"清洗配置错误")


class CreateKafkaTopicError(DatabusAPIError):
    CODE = "015"
    MESSAGE = _(u"初始化数据kafka topic失败。{topic} -> {kafka_bs}")


class KafkaTopicNotExistsError(DatabusAPIError):
    CODE = "016"
    MESSAGE = _(u"kafka topic partition不存在！{topic}-{partition}")


class KafkaTopicOffsetNotExistsError(DatabusAPIError):
    CODE = "017"
    MESSAGE = _(u"kafka topic partition中offset不存在！{tp}-{offset}")


class KafkaStorageNotExistsError(DatabusAPIError):
    CODE = "018"
    MESSAGE = _(u"{rt_id} kafka存储不存在！")


class AlterKafkaPartitionsError(DatabusAPIError):
    CODE = "019"
    MESSAGE = _(u"修改kafka topic({topic})分区数量失败！")


class ImportHdfsTaskCheckError(DatabusAPIError):
    CODE = "020"
    MESSAGE = _(u"离线任务状态检查失败！")


class DataNodeValidError(DatabusAPIError):
    CODE = "021"
    MESSAGE = _(u"固化节点配置参数校验失败！")


class TransfromProcessingCreateError(DatabusAPIError):
    CODE = "022"
    MESSAGE = _(u"固化节点算子创建失败！")


class NotSupportDataNodeError(DatabusAPIError):
    CODE = "023"
    MESSAGE = _(u"以下固化节点暂未支持,节点类型:{node_type}")


class ResultTableDelError(DatabusAPIError):
    CODE = "024"
    MESSAGE = _(u"调用结果表删除接口失败,接口表id:{result_table_id}")


class TransfromProcessingNotFoundError(DatabusAPIError):
    CODE = "025"
    MESSAGE = _(u"查询指定id的固化节点不存在,节点id:{processing_id}")


class DataNodeCreateError(DatabusAPIError):
    CODE = "026"
    MESSAGE = _(u"固化节点创建失败！")


class DataNodeUpdateError(DatabusAPIError):
    CODE = "027"
    MESSAGE = _(u"固化节点更新失败,节点id:{processing_id}！")


class DataNodeDeleteError(DatabusAPIError):
    CODE = "028"
    MESSAGE = _(u"固化节点删除失败,节点id:{processing_id}！")


class CreateDataProcessingError(DatabusAPIError):
    CODE = "029"
    MESSAGE = _(u"创建DataProcessing失败：{message}")


class RawDataNotExistsError(DatabusAPIError):
    CODE = "030"
    MESSAGE = _(u"RawData不存在：{raw_data_id}")


class GetStorageClustersError(DatabusAPIError):
    CODE = "031"
    MESSAGE = _(u"获取存储集群信息失败")


class StorageNotSupportedError(DatabusAPIError):
    CODE = "032"
    MESSAGE = _(u"存储类型不支持: {cluster_type}")


class CleanAndRawDataNotMatchError(DatabusAPIError):
    CODE = "033"
    MESSAGE = _(u"清洗ResultTable和源数据不匹配: {result_table_id} {raw_data_id}")


class RtStorageExistsError(DatabusAPIError):
    CODE = "034"
    MESSAGE = _(u"清洗ResultTable已关联存储{cluster_type}")


class RtStorageNotExistsError(DatabusAPIError):
    CODE = "035"
    MESSAGE = _(u"清洗ResultTable未关联存储{cluster_type}")


class CreateTableFailedError(DatabusAPIError):
    CODE = "036"
    MESSAGE = _(u"创建物理表失败。{message}")


class GetRtStorageError(DatabusAPIError):
    CODE = "037"
    MESSAGE = _(u"获取ResultTable的存储信息失败。{message}")


class RtKafkaStorageNotExistError(DatabusAPIError):
    CODE = "038"
    MESSAGE = _(u"ResultTable({result_table_id})的Kafka存储不存在。")


class NoAvailableConfigChannelError(DatabusAPIError):
    CODE = "039"
    MESSAGE = _(u"没有可用的config channel，请先初始化config channel的信息")


class InitDatabusError(DatabusAPIError):
    CODE = "040"
    MESSAGE = _(u"初始化参数错误。{message}")


class TaskStorageNotFound(DatabusAPIError):
    CODE = "041"
    MESSAGE = _(u"ResultTable({result_table_id})未找到存储类型({storage})的配置信息")


class TaskCreateErr(DatabusAPIError):
    CODE = "042"
    MESSAGE = _(u"ResultTable({result_table_id})的存储类型({storage})任务创建失败:{message}")


class TaskCountErr(DatabusAPIError):
    CODE = "043"
    MESSAGE = _(u"ResultTable({result_table_id})的存储类型({storage})任务数({count})异常(大于1)")


class TaskNotFound(DatabusAPIError):
    CODE = "044"
    MESSAGE = _(u"ResultTable({result_table_id})的存储类型({storage})任务未找到")


class TaskComponentNotFound(DatabusAPIError):
    CODE = "045"
    MESSAGE = _(u"ResultTable({result_table_id})的存储类型({storage})任务打点信息未找到")


class TaskParamMissing(DatabusAPIError):
    CODE = "046"
    MESSAGE = _(u"请求参数必须包含{param}")


class TaskChannelNotFound(DatabusAPIError):
    CODE = "047"
    MESSAGE = _(u"无法找到目标channel")


class TaskNoSuitableCluster(DatabusAPIError):
    CODE = "048"
    MESSAGE = _(u"无法为任务({task})寻找到合适的运行集群({cluster})")


class TaskTopicNotFound(DatabusAPIError):
    CODE = "049"
    MESSAGE = _(u"未查询到源kafka topic({topic})信息")


class TaskInsertDBErr(DatabusAPIError):
    CODE = "050"
    MESSAGE = _(u"任务写入数据库(databus_connector_task)失败")


class TaskChannelConfigNotFound(DatabusAPIError):
    CODE = "051"
    MESSAGE = _(u"未找到kafka集群({kafka})对应的集群配置(databus_channel_cluster_config)信息")


class TaskRawDataNotFound(DatabusAPIError):
    CODE = "052"
    MESSAGE = _(u"未找到data_id({id})对应的原始数据信息配置(access_raw_data)信息")


class TaskStorageConnConfigErr(DatabusAPIError):
    CODE = "053"
    MESSAGE = _(u"集群连接配置错误")


class TaskStorageConfigErr(DatabusAPIError):
    CODE = "054"
    MESSAGE = _(u"存储配置参数格式错误")


class TaskDruidZKEmpty(DatabusAPIError):
    CODE = "055"
    MESSAGE = _(u"druid集群连接配置中zookeeper.connect为空")


class TaskConnectClusterNoFound(DatabusAPIError):
    CODE = "056"
    MESSAGE = _(u"未找到符合({cluster})的集群")


class TaskStartErr(DatabusAPIError):
    CODE = "057"
    MESSAGE = _(u"启动任务失败，任务名称:{task}，集群:{cluster}")


class TaskStorageNotSupport(DatabusAPIError):
    CODE = "058"
    MESSAGE = _(u"不支持存储类型{storage}")


class TaskDeleteErr(DatabusAPIError):
    CODE = "059"
    MESSAGE = _(u"集群({cluster})请求删除任务({task})失败")


class TaskClusterUrlNotFound(DatabusAPIError):
    CODE = "060"
    MESSAGE = _(u"集群({cluster})的url查询失败")


class TaskGenConfigErr(DatabusAPIError):
    CODE = "061"
    MESSAGE = _(u"生成(任务：{task},集群：{cluster})配置失败")


class TaskDatanodeConfigNotFound(DatabusAPIError):
    CODE = "062"
    MESSAGE = _(u"未找到对应的任务配置(databus_transform_process)信息")


class TaskAccessResourceNotFound(DatabusAPIError):
    CODE = "063"
    MESSAGE = _(u"未找到data_id({data_id})对应的TDW配置(access_resource_info)信息")


class TaskTDWConfigErr(DatabusAPIError):
    CODE = "064"
    MESSAGE = _(u"TDW配置参数错误")


class TaskDataScenarioNotSupport(DatabusAPIError):
    CODE = "065"
    MESSAGE = _(u"不支持来源为({Scenario})的任务")


class UpdateDataProcessingError(DatabusAPIError):
    CODE = "066"
    MESSAGE = _(u"更新DataProcessing失败：{message}")


class StoreKitStorageRtError(DatabusAPIError):
    CODE = "067"
    MESSAGE = _(u"新增rt与存储的关联关系失败")


class ExistDataStorageConfError(DatabusAPIError):
    CODE = "068"
    MESSAGE = _(u"已经存在入库配置，请勿重复入库")


class NotFoundStorageConfError(DatabusAPIError):
    CODE = "069"
    MESSAGE = _(u"入库记录不存在")


class NotFoundShipperTaskError(DatabusAPIError):
    CODE = "070"
    MESSAGE = _(u"入库任务配置不存在")


class NotFoundTaskClassError(DatabusAPIError):
    CODE = "071"
    MESSAGE = _(u"任务类不存在")


class NotFoundRtError(DatabusAPIError):
    CODE = "072"
    MESSAGE = _(u"rt信息不存在")


class NotFoundShipperError(DatabusAPIError):
    CODE = "073"
    MESSAGE = _(u"入库配置不存在")


class JsonFormatError(DatabusAPIError):
    CODE = "074"
    MESSAGE = _(u"json格式错误")


class StorageError(DatabusAPIError):
    CODE = "075"
    MESSAGE = _(u"本次入库失败")


class AlterKafkaTopicError(DatabusAPIError):
    CODE = "076"
    MESSAGE = _(u"修改kafka topic({topic})配置失败！")


class StopOldShipperTaskError(DatabusAPIError):
    CODE = "077"
    MESSAGE = _(u"停止老分发集群任务失败, rt_id:{rt_id} , storages:{storages}")


class StorageClusterNotExistsError(DatabusAPIError):
    CODE = "078"
    MESSAGE = _(u"{storage}-{cluster}存储集群不存在！")


class CleanNotFoundError(DatabusAPIError):
    CODE = "079"
    MESSAGE = _(u"清洗不存在,rt_id: {result_table_id}")


class NotJsonFiledError(DatabusAPIError):
    CODE = "080"
    MESSAGE = _(u"该字段非json赋值节点, field:{field}")


class ParamFormatError(DatabusAPIError):
    CODE = "081"
    MESSAGE = _(u"{param}格式错误，要求{format}")


class LhotseIDNotFoundError(DatabusAPIError):
    CODE = "082"
    MESSAGE = _(u"{result_table_id}的tdw相关洛子(lhotse)任务id未找到")


class TaskCycleNotSupportError(DatabusAPIError):
    CODE = "083"
    MESSAGE = _(u"不支持任务周期, 频率是{freq} 周期是{unit}")


class MetaResponseError(DatabusAPIError):
    CODE = "084"
    MESSAGE = _(u"meta返回结构错误, field={field}")


class AccessResourceNotFound(DatabusAPIError):
    CODE = "085"
    MESSAGE = _(u"未找到data_id({data_id})对应的资源配置(access_resource_info)信息")


class ResourceNotJson(DatabusAPIError):
    CODE = "086"
    MESSAGE = _(u"data_id({data_id})资源配置(access_resource_info)信息不是合法json")


class DataStructNotFound(DatabusAPIError):
    CODE = "087"
    MESSAGE = _(u"不存在对应数据结构配置")


class RawdataHasOneMoreCleanError(DatabusAPIError):
    CODE = "088"
    MESSAGE = _(u"源数据包含多个清洗配置:{raw_data_id}")


class GetRtExtraFailedError(DatabusAPIError):
    CODE = "089"
    MESSAGE = _(u"获取Rt表extra信息失败。{message}")


class TaskStopErr(DatabusAPIError):
    CODE = "090"
    MESSAGE = _(u"停止任务失败。")


class GetRtDataProcessingError(DatabusAPIError):
    CODE = "091"
    MESSAGE = _(u"从meta获取data processing信息失败:{message}")


class RtDataProcessingNumError(DatabusAPIError):
    CODE = "092"
    MESSAGE = _(u"RT的data processing信息input数量为:{input_num}，期望1。")


class CheckRunningConfigError(DatabusAPIError):
    CODE = "093"
    MESSAGE = _(u"检查线上运行任务配置失败")


class ConfigConflictError(DatabusAPIError):
    CODE = "094"
    MESSAGE = _(u"topic配置冲突，当前运行配置项：{type}={old_value}，新配置为{new_value}。可能引起老任务失败，如确认必须修改，请与管理员联系！")


class ClusterPermissionError(DatabusAPIError):
    CODE = "095"
    MESSAGE = _(u"集群组权限错误")


class RouteConfError(DatabusAPIError):
    CODE = "096"
    MESSAGE = _(u"路由配置错误")


class SchemaDiffError(DatabusAPIError):
    CODE = "097"
    MESSAGE = _(u"{rt1} 和 {rt2} 字段不同!")


class SourceDuplicateError(DatabusAPIError):
    CODE = "098"
    MESSAGE = _(u"合流源节点出现重复!")


class RegionTagError(DatabusAPIError):
    CODE = "099"
    MESSAGE = _(u"rt={rt}和channel={channel}区域配置不一致")


class TaskConfigErr(DatabusAPIError):
    CODE = "100"
    MESSAGE = _(u"任务配置错误")


class TdwShipperHistoryCleanError(DatabusAPIError):
    CODE = "101"
    MESSAGE = _(u"删除{days}天以前的tdw分发历史任务失败！")


class MigrationCannotOperatError(DatabusAPIError):
    CODE = "102"
    MESSAGE = _(u"{type}任务不支持执行")


class MigrationOperatFailError(DatabusAPIError):
    CODE = "103"
    MESSAGE = _(u"任务执行失败：{message}")


class MigrationNotFoundError(DatabusAPIError):
    CODE = "104"
    MESSAGE = _(u"任务id不存在对应任务")


class MigrationCreateError(DatabusAPIError):
    CODE = "105"
    MESSAGE = _(u"创建任务失败:{message}")


class MigrationTooLongError(DatabusAPIError):
    CODE = "106"
    MESSAGE = _(u"创建任务失败: 迁移任务时间不应超过{days}天")


class MigrationTimeError(DatabusAPIError):
    CODE = "107"
    MESSAGE = _(u"创建任务失败: 迁移任务起始时间应该小于结束时间")


class MigrationRunningError(DatabusAPIError):
    CODE = "108"
    MESSAGE = _(u"创建任务失败: 存在相同result_table，源集群和目的集群的迁移任务正在执行中，请等待完成后再新增任务")


class NotSupportDeleteCleanErr(DatabusAPIError):
    CODE = "109"
    MESSAGE = _(u"暂不支持删除存在下游存储/计算节点的清洗任务")


class RunKafkaCmdErr(DatabusAPIError):
    CODE = "110"
    MESSAGE = _(u"操作kafka失败")


class StoreKitApiError(DatabusAPIError):
    CODE = "111"
    MESSAGE = _(u"调用storekitapi失败{message}")


class AddAuthErr(DatabusAPIError):
    CODE = "112"
    MESSAGE = _(u"添加权限失败，任务名称:{task}，权限类型:{type}")


class NotFoundError(DatabusAPIError):
    CODE = "113"
    MESSAGE = _("{type}:{key} not found!")


class CapacityInsufficient(DatabusAPIError):
    CODE = "114"
    MESSAGE = _("{cluster_name}: capacity insufficient!")


class NotSupportErr(DatabusAPIError):
    CODE = "115"
    MESSAGE = _(u"暂不支持")


class RequestErr(DatabusAPIError):
    CODE = "116"
    MESSAGE = _(u"request error, {msg}")


class CreateTopicErr(DatabusAPIError):
    CODE = "117"
    MESSAGE = _("create topic failed, {msg}")


class IncrementsPartitionsErr(DatabusAPIError):
    CODE = "118"
    MESSAGE = _("increments partitions failed, {msg}")


class DeleteTopicErr(DatabusAPIError):
    CODE = "119"
    MESSAGE = _("delete topic failed, {msg}")


class NotExistHdfsStorageErr(DatabusAPIError):
    CODE = "120"
    MESSAGE = _(u"请先关联hdfs存储")


class MetaDeleteDTError(DatabusAPIError):
    CODE = "121"
    MESSAGE = _("delete data transferring error, {message}")


class CcVersionError(DatabusAPIError):
    CODE = "122"
    MESSAGE = _("query cc version failed, {message}")


class AuthError(DatabusAPIError):
    CODE = "123"
    MESSAGE = _(u"队列授权失败，当前RT未关联队列存储，请先关联queue")


class TaskTransportStorageError(DatabusAPIError):
    CODE = "124"
    MESSAGE = _(u"不支持的source和sink传输类型组合 {source_type}:{sink_type} !")


class TaskTransportRtError(DatabusAPIError):
    CODE = "125"
    MESSAGE = _(u"参数错误：不允许rt_id和存储类型都相同! {params}")


class TaskTransportStatusError(DatabusAPIError):
    CODE = "126"
    MESSAGE = _(u"获取迁移任务状态失败{message}")


class TcaplusAccessTokenException(DatabusAPIError):
    CODE = "127"
    MESSAGE = _(u"获取tcaplus token失败{message}")


class TcaplusGetDomainException(DatabusAPIError):
    CODE = "128"
    MESSAGE = _(u"获取tcaplus 服务端域名和端口失败{message}")


class SyncGseException(DatabusAPIError):
    CODE = "129"
    MESSAGE = _(u"同步路由信息到gse失败")


class MethodNotSupportError(DatabusAPIError):
    CODE = "130"
    MESSAGE = _(u"此方法不被支持")
