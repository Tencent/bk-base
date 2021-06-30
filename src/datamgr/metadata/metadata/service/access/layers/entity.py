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

import gevent
from about_time import about_time
from tinyrpc.dispatch import public

from metadata.backend.interface import BackendType, LineageDirection
from metadata.runtime import rt_context, rt_local, rt_local_manager
from metadata.service.access.layer_interface import check_backend
from metadata.service.access.layers.asset import AssetAccessLayer
from metadata.util.context import inherit_local_ctx


class EntityAccessLayer(AssetAccessLayer):
    """
    实体操作层。
    """

    @public
    @check_backend(BackendType.DGRAPH)
    def query_lineage(
        self,
        entity_type,
        attr_value,
        depth=3,
        direction=LineageDirection.BOTH.value,
        unique_attr_name='qualified_name',
        backend_type='dgraph',
        extra_retrieve=None,
        only_user_entity=False,
    ):
        """
        血缘查询接口

        :param entity_type: 实体类型
        :param attr_value: 实体检索属性值（一般为主键或者资源定位属性值）
        :param depth: 深度
        :param direction: 方向
        :param unique_attr_name: 检索的属性名称。目前不用，自动判断。
        :param backend_type: 后端类型。
        :param extra_retrieve: ERP检索表达式
        :param only_user_entity: 是否只检索用户生成的实体
        :return:
        todo：参数校验，后续需要更为合理的校验框架
        """
        # 目前暂定负数表达无限深度，默认取 200 深度，后续再讨论是否支持无限深度
        with about_time() as t:
            depth = int(depth) if int(depth) >= 0 else 200
            backend = self.backends_group_storage[(BackendType(backend_type))]
            content = backend.query_lineage(
                entity_type,
                attr_value,
                depth=depth,
                raw_direction=LineageDirection(direction),
                extra_retrieve=extra_retrieve,
                only_user_entity=only_user_entity,
            )
        self.logger.info(
            {
                'dispatch_id': rt_context.dispatch_id,
                'lineage_query_type': backend_type,
                'metric_type': 'lineage_compare',
                'elapsed_time': t.duration,
            },
            extra={'output_metric': True},
        )
        return {'lineage': content, 'backend_type': backend_type}

    @public
    def query_genealogy(
        self, entity_type, attr_value, unique_attr_name='qualified_name', depth=3, backend_type='dgraph'
    ):
        raise NotImplementedError()
