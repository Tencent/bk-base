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

from tinyrpc.dispatch import public

from metadata.backend.erp.service import ErpService
from metadata.backend.interface import BackendType, RawBackendType
from metadata.interactor.core import Operation
from metadata.interactor.interact import SingleInteractor
from metadata.service.access.layer_interface import BaseAccessLayer, check_backend


class AssetAccessLayer(BaseAccessLayer):
    """
    Asset操作层。
    """

    @public
    def complex_search(self, statement, backend_type='mysql', orderly=False, **kwargs):
        """
        原生查询接口

        :param statement: 查询语句（sql, graphql等）
        :param backend_type: 后端类型
        :param orderly: 返回结果是否有序
        :param kwargs: 其他参数
        :return:
        """
        backend_type = BackendType(backend_type)
        if not self.normal_conf.BACKEND_HA and backend_type == BackendType.DGRAPH_BACKUP:
            backend_type = BackendType.DGRAPH
        backend = self.backends_group_storage[backend_type]
        ret = None
        if backend_type == BackendType.MYSQL:
            with backend.operate_session() as ops:
                ret = ops.operate(statement, orderly, **kwargs)
        if backend_type.raw == RawBackendType.DGRAPH:
            ret = backend.query(statement, **kwargs)
        return ret

    @public
    def complex_search_with_erp(
        self, statement, backend_type='dgraph', orderly=False, retrieve_args=None, start_rules=None, **kwargs
    ):
        """
        原生查询接口 With ERP support。

        :param start_rules: ERP起始节点获取规则。
        :param retrieve_args: ERP表达式
        :param statement: 查询语句（sql, graphql等）
        :param backend_type: 后端类型
        :param orderly: 返回结果是否有序
        :param kwargs: 其他参数
        :return:
        """
        ret = self.complex_search(statement, backend_type=backend_type, orderly=False, **kwargs)
        backend_type = BackendType(backend_type)
        erp_ret = None
        if retrieve_args and start_rules and list(retrieve_args.keys()) == list(start_rules.keys()):
            all_starts = self._parse_erp_starts(backend_type, start_rules, ret)
            for k, v in retrieve_args.items():
                if 'starts' in v:
                    v['starts'].extend(all_starts[k])
                else:
                    v['starts'] = all_starts[k]
            erp_ret = self.query_via_erp(retrieve_args)
        if erp_ret:
            return {'search_result': ret['data'] if backend_type is BackendType.DGRAPH else ret, 'erp_result': erp_ret}
        else:
            return {'search_result': ret['data']}

    @staticmethod
    def _parse_erp_starts(backend_type, start_rules, complex_search_ret):
        starts = {}
        if backend_type == BackendType.DGRAPH:
            for k, v in start_rules.items():
                starts[k] = [i[v] for i in complex_search_ret['data'][k]]
        if backend_type == BackendType.MYSQL:
            for k, v in start_rules.items():
                starts[k] = [i[v] if isinstance(i, dict) else dict(i)[v] for i in complex_search_ret]
        return starts

    @public
    @check_backend(BackendType.DGRAPH)
    def query_via_erp(self, retrieve_args, version=2, backend_type='dgraph'):
        """
        使用erp协议进行查询。

        :param version: 版本1或2。
        :param retrieve_args: 查询参数。key为元数据类型（如ResultTable），value为另一个dict，包括：起始资源键值（放在starts键中），
        erp表达式（放在expression键中）。
        :type retrieve_args: dict
        :param backend_type: 查询后端类型
        示例::

            {
                 "ResultTable": {"expression": {
                    "*": True,
                    "~DataProcessingRelation.data_set": {
                       "processing_id": True,
                        "data_directing": True,
                       "processing": {
                            "processing_type": True
                        },
                    },
                }, "starts": ["591_durant1115"]}
             }

        :return: ret
        """
        erp_service = ErpService(self.backends_group_storage)
        if version == 1:
            retrieve_args = erp_service.compatible_with_version_one(retrieve_args)
        return erp_service.query(retrieve_args)['data']

    @public
    @check_backend(BackendType.DGRAPH)
    def edit(self, operate_records_lst, batch=False):
        """
        元数据独立更新接口(只应该只存在于Dgraph中的分析元模型开放)

        :param operate_records_lst:
        :param batch:
        :return:
        """
        try:
            b = SingleInteractor(BackendType.DGRAPH)
            b.dispatch([Operation(**item) for item in operate_records_lst], batch)
        except Exception:
            self.logger.exception('Fail to sync.')
            raise
