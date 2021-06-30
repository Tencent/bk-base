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


from common.api.base import DataDRFAPISet
from datahub.common.const import DATAHUB
from datahub.storekit.settings import HUB_MANAGER_API_URL


class _DataHubApi(object):
    def __init__(self):
        # ignite sql 查询
        self.ignite_sql_query = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/ignite/cache/sqlQuery",
            primary_key=None,
            module=DATAHUB,
            description="根据sql查询",
            default_timeout=60,  # 设置默认超时时间
        )

        # ignite sql 执行
        self.ignites_sql_execute = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/ignite/cache/sqlExecute",
            primary_key=None,
            module=DATAHUB,
            description="执行sql",
            default_timeout=60,  # 设置默认超时时间
        )

        # ignite 查询缓存基础信息
        self.ignite_cache_info = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/ignite/cache/info",
            primary_key=None,
            module=DATAHUB,
            description="查询cache基本信息",
            custom_config={},
        )

        # iceberg 创建表
        self.iceberg_create_table = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/createTable",
            primary_key=None,
            module=DATAHUB,
            description="建表",
            default_timeout=60,  # 设置默认超时时间
        )

        # iceberg 修改表
        self.iceberg_alter_table = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/alterTable",
            primary_key=None,
            module=DATAHUB,
            description="变更表结构",
            default_timeout=60,  # 设置默认超时时间
        )

        # iceberg 修改表分区
        self.iceberg_change_partition_spec = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/changePartitionSpec",
            primary_key=None,
            module=DATAHUB,
            description="变更表分区",
            default_timeout=60,  # 设置默认超时时间
        )

        # iceberg 查询表基础信息
        self.iceberg_info = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/info",
            primary_key=None,
            module=DATAHUB,
            description="查询iceberg table基本信息",
            custom_config={},
        )

        # iceberg 查询表schema信息
        self.iceberg_schema_info = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/schemaInfo",
            primary_key=None,
            module=DATAHUB,
            description="获取表schema信息",
            custom_config={},
        )

        # iceberg 查询catalog列表
        self.list_catalog = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/listCatalog",
            primary_key=None,
            module=DATAHUB,
            description="查询iceberg table所在catalog的全部库表信息",
            custom_config={},
        )

        # 强制释放锁
        self.force_release_lock = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/forceReleaseLock",
            primary_key=None,
            module=DATAHUB,
            description="强制释放iceberg table在hive表上的锁",
            custom_config={},
        )

        # rename iceberg表
        self.iceberg_rename_table = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/renameTable",
            primary_key=None,
            module=DATAHUB,
            description="修改iceberg表的表名称",
            custom_config={},
        )

        # 删除iceberg表
        self.iceberg_drop_table = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/dropTable",
            primary_key=None,
            module=DATAHUB,
            description="查询iceberg table基本信息",
            custom_config={},
        )

        # iceberg表快照信息
        self.iceberg_snapshots = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/snapshots",
            primary_key=None,
            module=DATAHUB,
            description="查询iceberg table快照信息",
            custom_config={},
        )

        # iceberg更新location信息
        self.iceberg_update_location = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/updateLocation",
            primary_key=None,
            module=DATAHUB,
            description="更新表location信息",
            custom_config={},
        )

        # 获取location信息
        self.iceberg_location = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/location",
            primary_key=None,
            module=DATAHUB,
            description="获取表location信息",
            custom_config={},
        )

        # 获取iceberg分区信息
        self.iceberg_partition_info = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/partitionInfo",
            primary_key=None,
            module=DATAHUB,
            description="获取表分区信息",
            custom_config={},
        )

        # 获取iceberg提交信息
        self.iceberg_commit_msgs = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/commitMsgs",
            primary_key=None,
            module=DATAHUB,
            description="查询iceberg table commitMsgs信息",
            custom_config={},
        )

        # 获取iceberg分区路径
        self.iceberg_partition_paths = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/partitionPaths",
            primary_key=None,
            module=DATAHUB,
            description="查询iceberg table 分区路径信息",
            custom_config={},
        )

        # 获取iceberg表提交offset记录
        self.iceberg_offset_msgs = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/resetOffset",
            primary_key=None,
            module=DATAHUB,
            description="将hdfs的offset写入commitMsgs信息",
            custom_config={},
        )

        # 读取iceberg表数据
        self.iceberg_read_records = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/readRecords",
            primary_key=None,
            module=DATAHUB,
            description="条件查询iceberg table数据",
            custom_config={},
        )

        # 读取iceberg表样例数据
        self.iceberg_sample_records = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/sampleRecords",
            primary_key=None,
            module=DATAHUB,
            description="随机采样iceberg table数据",
            custom_config={},
        )

        # 更新iceberg表配置
        self.iceberg_update_properties = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/updateProperties",
            primary_key=None,
            module=DATAHUB,
            description="更新iceberg table属性",
            custom_config={},
        )

        # 删除iceberg表数据文件
        self.iceberg_delete_datafiles = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/deleteDatafiles",
            primary_key=None,
            module=DATAHUB,
            description="删除iceberg表数据文件",
            custom_config={},
        )

        # iceberg表过期清理
        self.iceberg_expire = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/expire",
            primary_key=None,
            module=DATAHUB,
            description="清理iceberg table过期时间外的数据",
            custom_config={},
            default_timeout=1800,  # 设置默认超时时间
        )

        # iceberg表合并小文件
        self.iceberg_compact = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/compact",
            primary_key=None,
            module=DATAHUB,
            description="合并指定时间范围的小文件",
            custom_config={},
            default_timeout=1800,  # 设置默认超时时间
        )

        # 删除iceberg表分区
        self.iceberg_delete_partitions = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/deletePartitions",
            primary_key=None,
            module=DATAHUB,
            description="删除iceberg table指定分区",
            custom_config={},
        )

        # 删除iceberg表数据
        self.iceberg_delete_data = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/deleteData",
            primary_key=None,
            module=DATAHUB,
            description="条件删除iceberg table数据",
            custom_config={},
        )

        # 更新iceberg表数据
        self.iceberg_update_data = DataDRFAPISet(
            url=HUB_MANAGER_API_URL + "/iceberg/updateData",
            primary_key=None,
            module=DATAHUB,
            description="条件更新iceberg table数据",
            custom_config={},
        )


DataHubApi = _DataHubApi()
