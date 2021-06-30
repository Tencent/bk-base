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

import logging
import time

import gevent
from huey import Huey
from huey.storage import PriorityRedisExpireStorage
from lazy_import import lazy_module

from metadata.runtime import rt_context, rt_g
from metadata.support import join_service_party
from metadata.util.log import init_logging
from metadata_biz.analyse.similarity import renew_context
from metadata_contents.config.conf import default_configs_collection

# 载入通用配置
rt_g.config_collection = default_configs_collection
normal_conf = rt_g.config_collection.normal_config
db_conf = rt_context.config_collection.db_config
logging_config = rt_g.config_collection.logging_config
logging_config.suffix_postfix = 'metadata_pro_service'
init_logging(logging_config, True)
rt_g.language = normal_conf.LANGUAGE

# 导入资源
rt_g.m_resource = lazy_module(str('metadata.resource'))
rt_g.m_biz_resource = lazy_module(str('metadata_biz.resource'))
biz_types = lazy_module(str('metadata_biz.types'))
rt_g.md_types_registry = biz_types.default_registry

logging.getLogger('kazoo').setLevel(logging.INFO)
logging.getLogger('kafka').setLevel(logging.INFO)


# 配置huey实例
class EnhancedHuey(Huey):
    def create_consumer(self, **options):
        del logging.getLogger('huey').handlers[:]
        return super(EnhancedHuey, self).create_consumer(**options)


huey = EnhancedHuey(
    storage_class=PriorityRedisExpireStorage,
    url='unix://{}'.format(db_conf.redis_lite_socket_path.encode('utf8')),
)


@huey.on_startup()
def init_worker_context():
    from metadata.service.access.service_server import Service

    Service.setup_access_layers()
    join_service_party(normal_conf.SERVICE_HA)


@huey.on_startup()
def init_bow():
    if not getattr(rt_g, 'bow_now', None):
        renew_context()
    gevent.spawn(keep_context_updated)


def keep_context_updated():
    while True:
        time.sleep(1800)
        renew_context()
