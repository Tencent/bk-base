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

from django.utils.translation import ugettext as _

from dataflow.pizza_settings import COMPUTE_TAG
from dataflow.shared.api.modules.storekit import StorekitApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.meta.tag.tag_helper import TagHelper


class StorekitHelper(object):
    @staticmethod
    def get_physical_table_name(result_table_id, processing_type, **kwargs):
        """
        @param result_table_id:
        @param processing_type:
        @param kwargs: 可选参数
            {
                project_id: 1
                tdw_type: tdw
            }
        @return:
        """
        kwargs.update({"result_table_id": result_table_id, "processing_type": processing_type})
        res = StorekitApi.result_tables.physical_table_name(kwargs)
        res_util.check_response(res)
        return res.data

    # 新增物理表实例
    @staticmethod
    def create_physical_table(result_table_id, cluster_name, cluster_type, expires, storage_config, **kwargs):
        """
        @param result_table_id:
        @param cluster_name:
        @param cluster_type:
        @param expires:
        @param storage_config:
        @param kwargs:  可包含generate_type，可选user|system, 默认user
        @return:
        """
        kwargs.update(
            {
                "result_table_id": result_table_id,
                "cluster_name": cluster_name,
                "cluster_type": cluster_type,
                "expires": expires,
                "storage_config": storage_config,
                "description": "DataFlow增加存储",
            }
        )
        res = StorekitApi.result_table_cluster_types.create(kwargs)
        res_util.check_response(res)
        return res.data

    # 更新物理表实例
    @staticmethod
    def update_physical_table(result_table_id, cluster_name, cluster_type, **kwargs):
        """
        以下为必须参数
        @param result_table_id:
        @param cluster_name:
        @param cluster_type:
        @param kwargs:
        @return:
        """
        kwargs.update(
            {
                "result_table_id": result_table_id,
                "cluster_name": cluster_name,
                "cluster_type": cluster_type,
            }
        )
        res = StorekitApi.result_table_cluster_types.update(kwargs)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_physical_table(result_table_id, cluster_type, **kwargs):
        kwargs.update({"result_table_id": result_table_id, "cluster_type": cluster_type})
        res = StorekitApi.result_table_cluster_types.retrieve(kwargs)
        res_util.check_response(res)
        return res.data

    # 删除存储信息
    @staticmethod
    def delete_physical_table(result_table_id, cluster_type):
        res = StorekitApi.result_table_cluster_types.delete(
            {"result_table_id": result_table_id, "cluster_type": cluster_type}
        )
        res_util.check_response(res)
        return res.data

    @staticmethod
    def delete_physical_table_by_username(result_table_id, cluster_type, bk_username):
        res = StorekitApi.result_table_cluster_types.delete(
            {
                "result_table_id": result_table_id,
                "cluster_type": cluster_type,
                "bk_username": bk_username,
            }
        )
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_hdfs_conf(result_table_id):
        res = StorekitApi.storekit.hdfs_conf({"result_table_id": result_table_id, "cluster_type": "hdfs"})
        res_util.check_response(res)
        return res.data

    # 创建MySQL数据库
    @staticmethod
    def mysql_create_db(result_table_id, db_name):
        res = StorekitApi.mysql.create_db({"result_table_id": result_table_id, "db_name": db_name})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def tsdb_is_table_exist(result_table_id):
        params = {"result_table_id": result_table_id}
        res = StorekitApi.tsdb.is_table_exist(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def tsdb_create_db(result_table_id):
        res = StorekitApi.tsdb.create_db(
            {
                "result_table_id": result_table_id,
            }
        )
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_storage_cluster_configs(cluster_type=None, cluster_group=None):
        params = {}
        if cluster_type:
            params.update({"cluster_type": cluster_type})
        if cluster_group:
            params.update({"cluster_group": cluster_group})
        res = StorekitApi.clusters.list(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def list_cluster_type_configs(cluster_type, tags=[]):
        params = {"cluster_type": cluster_type}
        if tags:
            params.update({"tags": tags})
        res = StorekitApi.clusters.retrieve(params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_storage_cluster_config(cluster_type, cluster_name):
        params = {"cluster_type": cluster_type, "cluster_name": cluster_name}
        res = StorekitApi.cluster_cluster_names.retrieve(params)
        res_util.check_response(res)
        return res.data

    # 判断tspider库是否存在
    @staticmethod
    def tspider_is_db_exist(result_table_id):
        params = {"result_table_id": result_table_id}
        res = StorekitApi.tspider.is_db_exist(params)
        res_util.check_response(res)
        return res.data

    # 创建Tspider库
    @staticmethod
    def tspider_create_db(result_table_id):
        res = StorekitApi.tspider.create_db({"result_table_id": result_table_id})
        res_util.check_response(res)
        return res.data

    # 创建Hermes表
    @staticmethod
    def hermes_create_table(result_table_id):
        res = StorekitApi.hermes.create_table({"result_table_id": result_table_id})
        res_util.check_response(res)
        return res.data

    # 获取存储全局配置信息
    @staticmethod
    def get_common():
        res = StorekitApi.scenarios.common()
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_support_cluster_types():
        res = StorekitApi.scenarios.list()
        res_util.check_response(res)
        return res.data

    # 撤销删除存储配置的操作
    @staticmethod
    def delete_rollback(result_table_id, cluster_type):
        res = StorekitApi.result_table_cluster_types.rollback(
            {"result_table_id": result_table_id, "cluster_type": cluster_type}
        )
        res_util.check_response(res)
        return res.data

    @staticmethod
    def tdw_create_table(result_table_id):
        res = StorekitApi.tdw.create_table({"result_table_id": result_table_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def tdbank_create_table_to_tdw(result_table_id):
        res = StorekitApi.tdbank.create_table_to_tdw({"result_table_id": result_table_id})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def prepare(result_table_id, storage_type):
        timeout = None
        request_params = {
            "cluster_type": storage_type,
            "result_table_id": result_table_id,
        }
        if storage_type == "tspider":
            timeout = 300
        res = StorekitApi.storekit.prepare(request_params, timeout=timeout)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def clear_hdfs_data(result_table_id):
        timeout = None
        request_params = {"cluster_type": "hdfs", "result_table_id": result_table_id}
        res = StorekitApi.storekit.delete(request_params, timeout=timeout)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_default_storage(cluster_type, geog_area_code, allow_empty=False, tag=COMPUTE_TAG):
        tags = [geog_area_code]
        if tag:
            tags.append(tag)
        res = StorekitApi.clusters.retrieve({"cluster_type": cluster_type, "tags": tags})
        res_util.check_response(res)
        if len(res.data) != 1:
            if len(res.data) > 1 or not allow_empty:
                raise Exception(_("获取 %s 默认存储集群列表不唯一，请联系管理员处理.") % cluster_type)
        return res.data[0] if len(res.data) > 0 else {}

    @staticmethod
    def get_jar_file_uploaded_cluster(cluster_group):
        geog_area_code = TagHelper.get_geog_area_code_by_cluster_group(cluster_group)
        return StorekitHelper.get_default_storage("hdfs", geog_area_code)

    @staticmethod
    def check_schema(result_table_id, storage_type):
        res = StorekitApi.storekit.check_schema({"result_table_id": result_table_id, "cluster_type": storage_type})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def sample_records(result_table_id, storage_type):
        res = StorekitApi.storekit.sample_records({"result_table_id": result_table_id, "cluster_type": storage_type})
        res_util.check_response(res)
        return res.data

    @staticmethod
    def get_cluster_type(channel_type):
        if channel_type == "stream":
            return "kafka"
        elif channel_type == "batch":
            return "hdfs"
        elif channel_type == "tdw_batch":
            return "tdw"
        else:
            return None
