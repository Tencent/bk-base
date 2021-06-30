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

from common.base_crypt import BaseCrypt
from django.utils.translation import ugettext as _

from dataflow.shared.log import stream_logger as logger
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.stream.exceptions.comp_execptions import CheckpointInfoError


class CheckpointManager(object):

    cluster_type = "tredis"
    # 缓存
    _connection_info_cache = {}

    def __init__(self, geog_area_code):
        self.geog_area_code = geog_area_code

    @classmethod
    def get_connection_info(cls, geog_area_code):
        if not cls._connection_info_cache.get(geog_area_code):
            data = StorekitHelper.list_cluster_type_configs(cls.cluster_type, [geog_area_code, "compute"])
            if not data:
                raise CheckpointInfoError(_("checkpoint 配置信息为空，请联系管理员"))
            connection_infos = [_info["connection"] for _info in data]
            normal_redis_clusters = [_info for _info in connection_infos if not _info["enable_sentinel"]]
            sentinel_redis_clusters = [_info for _info in connection_infos if _info["enable_sentinel"]]
            if len(normal_redis_clusters) > 1 or len(sentinel_redis_clusters) > 1:
                raise CheckpointInfoError(_("checkpoint 配置信息不唯一，请联系管理员"))
            if sentinel_redis_clusters:
                # 确保万一，再进行值的校验
                host_sentinel = sentinel_redis_clusters[0]["host_sentinel"]
                port_sentinel = sentinel_redis_clusters[0]["port_sentinel"]
                name_sentinel = sentinel_redis_clusters[0]["name_sentinel"]
                if not host_sentinel or not port_sentinel or not name_sentinel:
                    logger.exception("checkpoint 配置信息错误：%s" % sentinel_redis_clusters)
                    raise CheckpointInfoError(_("checkpoint 配置信息错误，请联系管理员"))
                cls._connection_info_cache[geog_area_code] = sentinel_redis_clusters[0]
            else:
                # 确保万一，再进行值的校验
                host = normal_redis_clusters[0]["host"]
                port = normal_redis_clusters[0]["port"]
                password = normal_redis_clusters[0]["password"]
                if not host or not port or not password:
                    logger.exception("checkpoint 配置信息错误：%s" % normal_redis_clusters)
                    raise CheckpointInfoError(_("checkpoint 配置信息错误，请联系管理员"))
                cls._connection_info_cache[geog_area_code] = normal_redis_clusters[0]
        return cls._connection_info_cache.get(geog_area_code)

    @property
    def enable_sentinel(self):
        return CheckpointManager.get_connection_info(self.geog_area_code)["enable_sentinel"]

    @property
    def name_sentinel(self):
        return CheckpointManager.get_connection_info(self.geog_area_code)["name_sentinel"]

    @property
    def host(self):
        return CheckpointManager.get_connection_info(self.geog_area_code)["host"]

    @property
    def port(self):
        return CheckpointManager.get_connection_info(self.geog_area_code)["port"]

    @property
    def host_sentinel(self):
        return CheckpointManager.get_connection_info(self.geog_area_code)["host_sentinel"]

    @property
    def port_sentinel(self):
        return CheckpointManager.get_connection_info(self.geog_area_code)["port_sentinel"]

    @property
    def password(self):
        password = CheckpointManager.get_connection_info(self.geog_area_code)["password"]
        if password:
            password = BaseCrypt.bk_crypt().decrypt(password)
        return password

    @property
    def checkpoint_host(self):
        if not self.enable_sentinel:
            return self.host
        else:
            return self.host_sentinel

    @property
    def checkpoint_port(self):
        if not self.enable_sentinel:
            return self.port
        else:
            return self.port_sentinel
