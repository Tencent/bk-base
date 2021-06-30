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
from importlib import import_module

from conf.settings import (
    BKDATA_BIZ_ID,
    BKPUB_PROJECT_ID,
    DEFAULT_IGNITE_CLUSTER,
    RUN_VERSION,
)

_constants_module_cache = None


def get_var(var_name):
    global _constants_module_cache

    if _constants_module_cache is None:
        module_path = f"collection.templates.{RUN_VERSION}.constants"
        _constants_module_cache = import_module(module_path)
    return getattr(_constants_module_cache, var_name)


BKPUB_PROJECT_ID = BKPUB_PROJECT_ID
BKDATA_BIZ_ID = BKDATA_BIZ_ID
DEFAULT_IGNITE_CLUSTER = DEFAULT_IGNITE_CLUSTER

CMDB_HOST_FIELDS = get_var("CMDB_HOST_FIELDS")
CMDB_HOST_TABLE_NAME = "bkpub_cmdb_host"
CMDB_HOST_TABLE_ALIA = "CMDB 业务主机信息"
CMDB_HOST_PK_NAME = "bk_host_id"
CMDB_HOST_DATAMODEL_NAME = "BKPUB_CMDB_HOST_RELS"
CMDB_HOST_DATAMODEL_TABLE_NAME = "bkpub_cmdb_host_rels"

CMDB_MODULE_FIELDS = get_var("CMDB_MODULE_FIELDS")
CMDB_MODULE_TABLE_NAME = "bkpub_cmdb_module"
CMDB_MODULE_TABLE_ALIA = "CMDB 业务模块信息"
CMDB_MODULE_PK_NAME = "bk_module_id"
CMDB_MODULE_DATAMODEL_NAME = "BKPUB_CMDB_MODULE"
CMDB_MODULE_DATAMODEL_TABLE_NAME = "bkpub_cmdb_module_info"

CMDB_RELATION_FIELDS = get_var("CMDB_RELATION_FIELDS")
CMDB_RELATION_TABLE_NAME = "bkpub_cmdb_relation"
CMDB_RELATION_TABLE_ALIA = "CMDB 主机关系信息"
CMDB_RELATION_PK_NAME = "bk_host_id"

CMDB_SET_FIELDS = get_var("CMDB_SET_FIELDS")
CMDB_SET_TABLE_NAME = "bkpub_cmdb_set"
CMDB_SET_TABLE_ALIA = "CMDB 业务集群信息"
CMDB_SET_PK_NAME = "bk_set_id"
CMDB_SET_DATAMODEL_NAME = "BKPUB_CMDB_SET"
CMDB_SET_DATAMODEL_TABLE_NAME = "bkpub_cmdb_set_info"

CMDB_BIZ_FIELDS = get_var("CMDB_BIZ_FIELDS")
CMDB_BIZ_TABLE_NAME = "bkpub_cmdb_biz"
CMDB_BIZ_TABLE_ALIA = "CMDB 业务信息"
CMDB_BIZ_PK_NAME = "bk_biz_id"
CMDB_BIZ_DATAMODEL_NAME = "BKPUB_CMDB_BIZ_MODEL"
CMDB_BIZ_DATAMODEL_TABLE_NAME = "bkpub_cmdb_biz_info"

USER_INFO_TABLE_NAME = "bkpub_user_info"
USER_INFO_TABLE_ALIA = "usermanage 系统用户信息"
USER_INFO_PK_NAME = "id"
USER_INFO_DATAMODEL_NAME = "BKPUB_USERMGR_USER_INFO"
USER_INFO_DATAMODEL_TABLE_NAME = "bkpub_usermgr_user_info"


# 线程池大小
POOL_SIZE = 10

# cmdb 各对象下属性字段列表的缓存时间
CMDB_FIELDS_TTL = 1800

# cmdb cursor
cmdb_cursor_key_template = "pkpub.collect_cmdb_cursor_{object_type}"

DATACHECK_MEASUREMENT = "datamanager_collection_datacheck"
