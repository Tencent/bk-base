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

from common.base_utils import model_to_dict
from datahub.common.const import (
    ACCESS_CONF_INFO,
    BK_BIZ_ID,
    BK_USERNAME,
    CONNECTION,
    CREATED_AT,
    DESCRIPTION,
    FILE_NAME,
    HDFS_HOST,
    HDFS_PATH,
    HDFS_PORT,
    HOSTS,
    ID,
    OFFLINEFILE,
    PORT,
    RAW_DATA_ALIAS,
    RAW_DATA_ID,
    RAW_DATA_NAME,
    RESOURCE,
    SCOPE,
    UPDATED_AT,
)
from django.utils.translation import ugettext_noop

from ...collectors.utils.language import Bilingual
from ...exceptions import CollectorError, CollerctorCode
from ...models import AccessRawData, AccessRawDataTask, AccessResourceInfo, AccessTask
from ...utils.upload_hdfs_util import (
    get_active_namenode,
    get_default_storage,
    get_hdfs_upload_path,
    rename_file,
)
from ..base_collector import BaseAccess, BaseAccessTask
from .serializers import FileResourceSerializer


class OfflineFileAccess(BaseAccess):
    """
    文件上传接入
    """

    is_disposable = True
    data_scenario = OFFLINEFILE
    access_serializer = FileResourceSerializer
    task_disable = True

    def create_after(self):
        # 创建执行记录
        task_id = BaseAccessTask.create(
            self.access_param[BK_BIZ_ID],
            self.access_param[BK_USERNAME],
            self.data_scenario,
            {},
            AccessTask.ACTION.DEPLOY,
        )
        AccessRawDataTask.objects.create(
            task_id=task_id,
            raw_data_id=self.access_param[RAW_DATA_ID],
            created_by=self.access_param[BK_USERNAME],
            updated_by=self.access_param[BK_USERNAME],
            description="",
        )

        task = BaseAccessTask(task_id=task_id)
        resource_list = AccessResourceInfo.objects.filter(raw_data_id=self.access_param[RAW_DATA_ID])
        if not resource_list:
            task.log(
                msg=Bilingual(ugettext_noop(u"不存在部署资源源配置信息")),
                level="ERROR",
                task_log=True,
            )
            task.task.set_status(AccessTask.STATUS.FAILURE)
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE)

        raw_data = AccessRawData.objects.get(id=self.access_param[RAW_DATA_ID])
        # 新版离线文件上传不需要解析文件内容，只需要检查下resource_info配置
        try:
            for resource_obj in resource_list:
                resource_json = json.loads(resource_obj.resource)
                # 第一次接入的数据中不包含hdfs_path等信息，再次编辑不需要重新增加hdfs_path
                if HDFS_PATH not in resource_json.keys():
                    hdfs_config = get_default_storage()
                    hdfs_path = get_hdfs_upload_path(raw_data.bk_biz_id, resource_json[FILE_NAME])
                    hdfs_file_rename = "{}-{}".format(raw_data.id, resource_json[FILE_NAME])
                    hdfs_new_path = get_hdfs_upload_path(raw_data.bk_biz_id, hdfs_file_rename)
                    if hdfs_path.endswith("/"):
                        hdfs_path = hdfs_path[:-1]

                    resource_json[HDFS_PATH] = hdfs_new_path
                    resource_json[HDFS_HOST] = hdfs_config[CONNECTION][HOSTS]
                    resource_json[HDFS_PORT] = hdfs_config[CONNECTION][PORT]
                    resource_json[FILE_NAME] = resource_json[FILE_NAME][15:]
                    resource_obj.resource = json.dumps(resource_json)
                    resource_obj.save()
                    hdfs_host = get_active_namenode(hdfs_config[CONNECTION][HOSTS], hdfs_config[CONNECTION][PORT])
                    rename_file(
                        hdfs_host,
                        hdfs_config[CONNECTION][PORT],
                        hdfs_path,
                        hdfs_new_path,
                    )

        except Exception as e:
            raise CollectorError(
                error_code=CollerctorCode.COLLECTOR_NOT_EXSIT_RESOURCE,
                message="文件系统中没有找到此文件, %s" % str(e),
            )

    def get(self):
        # 过滤hdfs集群信息等敏感信息
        access_param = self.generate_access_param_by_data_id(self.raw_data_id)
        for scope in access_param[ACCESS_CONF_INFO][RESOURCE][SCOPE]:
            for scope_key in scope.keys():
                if scope_key.startswith("hdfs_"):
                    del scope[scope_key]

        return access_param

    def get_by_list(self, raw_data_list):
        resource_data_list = AccessResourceInfo.objects.filter(raw_data_id__in=raw_data_list)
        raw_data_list = AccessRawData.objects.filter(id__in=raw_data_list)
        raw_data_dict = {raw_data.id: raw_data for raw_data in raw_data_list}
        resource_data = [model_to_dict(i) for i in resource_data_list]
        deploy_plan_list = []
        for index, resource in enumerate(resource_data):
            raw_data = raw_data_dict[resource[RAW_DATA_ID]]
            if raw_data.data_scenario != self.data_scenario:
                continue

            access_param = {
                ACCESS_CONF_INFO: {},
                BK_BIZ_ID: raw_data.bk_biz_id,
                DESCRIPTION: raw_data.description,
                UPDATED_AT: raw_data.updated_at,
                CREATED_AT: raw_data.created_at,
                ID: raw_data.id,
                RAW_DATA_NAME: raw_data.raw_data_name,
                RAW_DATA_ALIAS: raw_data.raw_data_alias,
            }
            scope_list = json.loads(resource[RESOURCE])

            if scope_list:
                access_param[ACCESS_CONF_INFO][RESOURCE] = {}
                access_param[ACCESS_CONF_INFO][RESOURCE][SCOPE] = [scope_list]

            # 过滤hdfs集群信息等敏感信息
            for scope in access_param[ACCESS_CONF_INFO][RESOURCE][SCOPE]:
                for scope_key in scope.keys():
                    if scope_key.startswith("hdfs_"):
                        del scope[scope_key]

            deploy_plan_list.append(access_param)

        return deploy_plan_list
