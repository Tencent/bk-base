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
from common.errorcodes import ErrorCode
from common.exceptions import BaseAPIError
from django.utils.translation import ugettext as _


class StorekitException(BaseAPIError):
    """
    storekit 异常父类
    """

    MODULE_CODE = ErrorCode.BKDATA_DATAAPI_STOREKIT


class StorageClusterConfigException(StorekitException):
    """
    storekit 集群配置异常父类
    """

    CODE = "100"


class StorageClusterConfigCreateException(StorekitException):
    """
    创建集群配置异常
    """

    CODE = "101"
    MESSAGE = _("添加存储配置失败, cluster_type: {cluster_type}, cluster_name: {cluster_name}, 异常原因: {e}")


class StorageGetResultTableException(StorekitException):
    """
    获取结果表存储配置异常
    """

    CODE = "102"
    MESSAGE = _("从元数据中没有找到{result_table_id}对应的结果表信息，请管理员检查元数据。")


class MetaApiException(StorekitException):
    """
    meta api 异常
    """

    CODE = "103"
    MESSAGE = _("元数据接口异常。{message}")


class RtStorageNotExistsError(StorekitException):
    """
    结果表存储配置不存在
    """

    CODE = "104"
    MESSAGE = _("{result_table_id}未关联存储{type}")


class NoActiveNameNodeError(StorekitException):
    """
    无激活的name node
    """

    CODE = "105"
    MESSAGE = _("No active namenode in HDFS cluster")


class SchemaMismatchError(StorekitException):
    """
    存储表中字段类型和结果表中字段类型定义不一致
    """

    CODE = "106"
    MESSAGE = _("存储表中字段类型和结果表中字段类型定义不一致，请修改这些字段名：{cols}。{msg}")


class EsRestRequestError(StorekitException):
    """
    请求es集群接口异常
    """

    CODE = "107"
    MESSAGE = _("请求es集群接口异常。{msg}")


class EsBadIndexError(StorekitException):
    """
    ElasticSearch集群中索引名称异常
    """

    CODE = "108"
    MESSAGE = _("ElasticSearch集群中索引名称异常，无法写入。{msg}")


class RtStorageExistsError(StorekitException):
    """
    结果表已关联存储
    """

    CODE = "109"
    MESSAGE = _("{result_table_id}已关联存储{cluster_type}。")


class RollbackNotSupportError(StorekitException):
    """
    集群类型不支持删除回滚
    """

    CODE = "110"
    MESSAGE = _("集群类型不支持删除回滚：{cluster_type}")


class HdfsRestRequestError(StorekitException):
    """
    请求hdfs集群接口异常
    """

    CODE = "111"
    MESSAGE = _("请求hdfs集群接口异常。{msg}")


class DataManageApiException(StorekitException):
    """
    请求hdfs集群接口异常
    """

    CODE = "112"
    MESSAGE = _("DataManage接口异常。{message}")


class AuthApiException(StorekitException):
    """
    权限接口异常
    """

    CODE = "119"
    MESSAGE = _("权限接口异常。{message}")


class StorageClusterExpiresConfigException(StorekitException):
    """
    存储集群过期配置异常
    """

    CODE = "200"


class StorageClusterExpiresConfigCreateException(StorekitException):
    """
    添加集群过期配置失败
    """

    CODE = "201"
    MESSAGE = _(
        "添加集群过期配置失败, cluster_type: {cluster_type}, cluster_name: {cluster_name}, "
        "cluster_subtype: {cluster_subtype}, 异常原因: {e}"
    )


class StorageScenarioConfigException(StorekitException):
    """
    存储场景配置异常
    """

    CODE = "300"


class StorageResultTableException(StorekitException):
    """
    结果表存储配置异常
    """

    CODE = "400"


class StorageRTConfigESNotSupportJsonException(StorekitException):
    """
    字段不是object类型，不支持JSON格式配置
    """

    CODE = "401"
    MESSAGE = _("字段{field_name}不是object类型，不支持JSON格式配置")


class StorageRTESConfigKeyNotSupportException(StorekitException):
    """
    存储ES配置项不支持key
    """

    CODE = "402"
    MESSAGE = _("存储ES配置项不支持{key}")


class StorageRTMySQLIncludeSystemTimeException(StorekitException):
    """
    不能选择系统时间字段
    """

    CODE = "405"
    MESSAGE = _("请不要选择系统时间字段, 系统时间字段包括: {system_time_fields}")


class StorageConfigMySQLException(StorekitException):
    """
    存储没有索引字段配置项，请检查此配置项
    """

    CODE = "406"
    MESSAGE = _("存储没有索引字段配置项，请检查此配置项")


class FieldsNotInRTException(StorekitException):
    """
    字段不在结果表的字段列表中，请传入正确的字段
    """

    CODE = "407"
    MESSAGE = _("字段{except_fields}不在结果表的字段列表中，请传入正确的字段")


class EsException(StorekitException):
    """
    es 异常
    """

    CODE = "500"


class ESHasNoIndexException(StorekitException):
    """
    没有对应的索引
    """

    CODE = "501"
    MESSAGE = _("{result_table_id}没有对应的索引")


class ESConstructMappingException(StorekitException):
    """
    ES索引结构创建异常，请检查字段配置属性
    """

    CODE = "502"
    MESSAGE = _("ES索引结构创建异常，请检查字段配置属性。索引名称：{index_name}")


class MysqlException(StorekitException):
    """
    mysql 异常
    """

    CODE = "700"


class MySQLConnDBException(StorekitException):
    """
    连接MySQL数据库异常
    """

    CODE = "701"
    MESSAGE = _("连接MySQL数据库异常")


class MySQLDropPartitionException(StorekitException):
    """
    MySQL删除分区异常
    """

    CODE = "702"
    MESSAGE = _("MySQL删除分区异常")


class MySQLFieldsDeleteException(StorekitException):
    """
    MySQL删除字段异常
    """

    CODE = "703"
    MESSAGE = _("请在sql尾部追加字段。MySQL表({table_name})已存在，并且rt表字段小于已有表的字段数，已有表的表结构是: {table_structure}")


class MySQLFieldsModifyException(StorekitException):
    """
    MySQL修改字段异常
    """

    CODE = "704"
    MESSAGE = _("请在sql尾部追加字段。MySQL表({table_name})已存在，并且rt表的顺序与已有表的顺序不一致，已有表的表结构是: {table_structure}")


class MysqlExecuteException(StorekitException):
    """
    MySQL执行语句异常
    """

    CODE = "705"
    MESSAGE = _("MySQL执行语句异常")


class TableNameTooLongException(StorekitException):
    """
    表名太长
    """

    CODE = "806"
    MESSAGE = _("{table_name}表名太长，超过mysql限定的最大长度({max_length})")


class QueryTagTargetException(StorekitException):
    """
    查询meta标签实体关联信息异常
    """

    CODE = "113"
    MESSAGE = _("查询meta标签实体关联信息异常,{message}")


class QueryGeogTagException(StorekitException):
    """
    查询地域标签信息异常
    """

    CODE = "114"
    MESSAGE = _("查询地域标签信息异常,{message}")


class IllegalTagException(StorekitException):
    """
    非法tag
    """

    CODE = "115"
    MESSAGE = _("非法tag")


class GeogAreaError(StorekitException):
    """
    地理区域错误
    """

    CODE = "116"
    MESSAGE = _("地理区域错误")


class GcsQueryCapacityException(StorekitException):
    """
    查询容量信息异常
    """

    CODE = "117"
    MESSAGE = _("查询容量信息异常,message:{message}")


class NotSupportFieldTypeException(StorekitException):
    """
    当前存储不支持该数据类型
    """

    CODE = "121"
    MESSAGE = _("当前存储不支持该数据类型, 存储类型:{storage_type}, 字段类型:{field_type}")


class DruidZkConfException(StorekitException):
    """
    集群zk元数据信息异常
    """

    CODE = "124"
    MESSAGE = _("druid:集群zk元数据信息异常")


class DruidCreateTaskErrorException(StorekitException):
    """
    创建任务异常
    """

    CODE = "125"
    MESSAGE = _("druid:创建任务异常, 结果表:{result_table_id}")


class NotSupportTaskTypeException(StorekitException):
    """
    不支持该该任务类型
    """

    CODE = "126"
    MESSAGE = _("不支持该该任务类型, 类型:{task_type}")


class DruidQueryTaskErrorException(StorekitException):
    """
    查询任务列表异常
    """

    CODE = "127"
    MESSAGE = _("druid:查询任务列表异常")


class DruidShutDownTaskException(StorekitException):
    """
    停止任务失败
    """

    CODE = "128"
    MESSAGE = _("停止任务失败,{message}")


class DruidQueryDataSourceException(StorekitException):
    """
    获取表结构异常
    """

    CODE = "129"
    MESSAGE = _("获取表结构异常,{message}")


class DruidQueryWorkersException(StorekitException):
    """
    获取worker异常
    """

    CODE = "130"
    MESSAGE = _("获取worker异常,{message}")


class DruidQueryExpiresException(StorekitException):
    """
    获取datasource过期时间规则异常
    """

    CODE = "131"
    MESSAGE = _("获取datasource过期时间规则异常,{message}")


class DruidUpdateExpiresException(StorekitException):
    """
    更新datasource过期时间规则异常
    """

    CODE = "132"
    MESSAGE = _("更新datasource过期时间规则异常,{message}")


class ExpiresFormatException(StorekitException):
    """
    过期时间格式错误
    """

    CODE = "133"
    MESSAGE = _("过期时间格式错误")


class DruidDeleteDataException(StorekitException):
    """
    删除druid数据异常
    """

    CODE = "132"
    MESSAGE = _("删除druid数据异常,{message}")


class StorageConfigException(StorekitException):
    """
    存储配置项错误，请检查配置项
    """

    CODE = "133"
    MESSAGE = _("存储配置项错误，请检查配置项")


class IgniteInfoException(StorekitException):
    """
    查询cache基础信息异常
    """

    CODE = "135"
    MESSAGE = _("查询cache基础信息异常,{message}")


class IgniteQueryException(StorekitException):
    """
    ignite执行sql异常
    """

    CODE = "136"
    MESSAGE = _("ignite执行sql异常,{message}")


class DruidQueryHistoricalException(StorekitException):
    """
    获取historical容量异常
    """

    CODE = "137"
    MESSAGE = _("获取historical容量异常,{message}")


class DruidQueryLeaderException(StorekitException):
    """
    获取leader地址异常
    """

    CODE = "138"
    MESSAGE = _("获取leader地址异常,{message}")


class DruidRetentionRuleException(StorekitException):
    """
    设置Druid DataSource Retention Rules异常
    """

    CODE = "139"
    MESSAGE = _("设置Druid DataSource Retention Rules异常,{message}")


class IgniteConfigException(StorekitException):
    """
    ignite存储没有主键字段配置项
    """

    CODE = "140"
    MESSAGE = _("ignite存储没有主键字段配置项，请检查此配置项")


class MaxRecordConfigException(StorekitException):
    """
    超出允许最大数据量
    """

    CODE = "141"
    MESSAGE = _("超出允许最大数据量")


class DruidZKPathException(StorekitException):
    """
    zk path异常
    """

    CODE = "142"
    MESSAGE = _("zk path异常")


class DruidHttpRequestException(StorekitException):
    """
    zk druid http 请求放回异常
    """

    CODE = "143"
    MESSAGE = _("zk druid http 请求放回异常")


class IcebergInfoException(StorekitException):
    """
    查询iceberg table基础信息异常
    """

    CODE = "144"
    MESSAGE = _("查询iceberg table基础信息异常,{message}")


class IcebergCreateTableException(StorekitException):
    """
    建表异常
    """

    CODE = "145"
    MESSAGE = _("建表异常,{message}")


class IcebergAlterTableException(StorekitException):
    """
    变更表结构异常
    """

    CODE = "146"
    MESSAGE = _("变更表结构异常,{message}")


class IcebergDropTableException(StorekitException):
    """
    删表异常
    """

    CODE = "147"
    MESSAGE = _("删表异常,{message}")


class ClusterSyncResourceException(StorekitException):
    """
    集群同步资源管理系统失败
    """

    CODE = "148"
    MESSAGE = _("集群同步资源管理系统失败")


class ClusterTagException(StorekitException):
    """
    集群标签异常
    """

    CODE = "149"
    MESSAGE = _("集群标签异常")


class ResourceQueryException(StorekitException):
    """
    查询集群资源管理信息失败
    """

    CODE = "150"
    MESSAGE = _("查询集群资源管理信息失败")


class RetryException(StorekitException):
    """
    multiple retries failed
    """

    CODE = "151"
    MESSAGE = _("multiple retries failed")


class ClusterDisableException(StorekitException):
    """
    cluster disabled
    """

    CODE = "152"
    MESSAGE = _("cluster disabled")


class IllegalArgumentException(StorekitException):
    """
    参数不合法
    """

    CODE = "153"
    MESSAGE = _("参数不合法")


class MigrateIcebergException(StorekitException):
    """
    迁移HDFS存储到iceberg格式失败
    """

    CODE = "154"
    MESSAGE = _("迁移HDFS存储到iceberg格式失败。{message}")


class NotFoundHiveServer(StorekitException):
    """
    not found hiveServer
    """

    CODE = "156"
    MESSAGE = _("not found hiveServer")


class AlertBillError(StorekitException):
    """
    alter bill error
    """

    CODE = "157"
    MESSAGE = _("alter bill error")


class ClusterNotFoundException(StorekitException):
    """
    cluster not found
    """

    CODE = "158"
    MESSAGE = _("{cluster_type}_{cluster_name}: cluster not found")


class IcebergChangeDataException(StorekitException):
    """
    修改表数据异常
    """

    CODE = "159"
    MESSAGE = _("iceberg table修改表数据异常,{message}")


class ClickHouseStorageConfigException(StorekitException):
    """
    clickhouse storage config异常
    """

    CODE = "174"
    MESSAGE = _("clickhouse storage config异常,{message}")


class UpdateConfValError(StorekitException):
    """
    failed to update conf value
    """

    CODE = "175"
    MESSAGE = _("failed to update conf value")


class GetConfValError(StorekitException):
    """
    failed to get conf value
    """

    CODE = "176"
    MESSAGE = _("failed to get conf value")


class ClickHousePrepareException(StorekitException):
    """
    clickhouse Prepare执行异常
    """

    CODE = "177"
    MESSAGE = _("clickhouse Prepare执行异常,{message}")


class NotSupportStorageException(StorekitException):
    """
    不支持的存储类型异常
    """

    CODE = "178"
    MESSAGE = _("不支持的存储类型异常,{message} {type}")


class ClickHouseAddIndexException(StorekitException):
    """
    failed to add index
    """

    CODE = "179"
    MESSAGE = _("failed to add index, {message}")


class ClickHouseDropIndexException(StorekitException):
    """
    failed to drop index
    """

    CODE = "180"
    MESSAGE = _("failed to drop index, {message}")


class ClickHouseConfigException(StorekitException):
    """
    failed to operation ck config
    """

    CODE = "181"
    MESSAGE = _("failed to operation ck config, {message}")


class IcebergUpdateLocationException(StorekitException):
    """
    failed to update table location
    """

    CODE = "182"
    MESSAGE = _("failed to update table location, {message}")
