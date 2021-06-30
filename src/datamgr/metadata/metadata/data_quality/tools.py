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

from metadata.backend.dgraph.backend import DgraphBackend
from metadata.backend.mysql.backend import MySQLBackend
from metadata.runtime import rt_context
from metadata.util.log import init_logging, update_report_logging
from metadata_biz.interactor.mappings import (
    md_db_model_mappings,
    original_replica_db_model_mappings,
)

module_logger = logging.getLogger()


class ToolManager(object):
    """
    管理对各个模块的工具类实例
    """

    def __init__(self):
        self.md_types = []
        self.dgraph_backend = None
        self.mysql_backend = None

        self.errors_statistics = {}

    def setup(self):
        self._setup_md_types()
        self._setup_backend()
        self._setup_logging()

    def _setup_md_types(self):
        """
        加载系统定义的 MD 类型
        """
        self.md_types = list(rt_context.md_types_registry.values())

    def _setup_logging(self):
        logging_config = rt_context.config_collection.logging_config
        logging_config.suffix_postfix = 'data_quality'

        init_logging(logging_config, True)
        update_report_logging(logging_config)

        logging.getLogger().setLevel(logging.INFO)

    def _setup_backend(self):
        """
        :todo 暂时先从 rt_content 中获取，为了加强脚本的独立性，需要移除对 rt_context 的依赖
        :todo 仅保留必要的初始化
        """
        self.dgraph_backend = DgraphBackend(rt_context.m_resource.dgraph_session)
        self.dgraph_backend.gen_types(list(rt_context.md_types_registry.values()))

        self.mysql_backend = MySQLBackend(rt_context.m_resource.mysql_session)
        self.mysql_backend.gen_types(
            rt_context.md_types_registry,
            replica_mappings=original_replica_db_model_mappings,
            mappings={k: v for k, v in md_db_model_mappings},
        )

    def add_statistics_errors(self, err):
        """
        增加 error 的统计数据
        """
        name = err.name

        if name not in self.errors_statistics:
            self.errors_statistics[name] = 0

        self.errors_statistics[name] += 1

    def log_quality_err(self, err):
        """
        :param {QualityErr} err 数据监控异常实例
        """
        metrics = err.to_all_metrics()

        # 统一给指标加上前缀，并加上 report_type 上报日志类型
        metrics = {'quality.{}'.format(k): v for k, v in list(metrics.items())}
        metrics['report_type'] = 'data_quality'

        module_logger.error(metrics, extra={'metric': True})

        self.add_statistics_errors(err)

    @staticmethod
    def log(category, msg, level='error'):
        """
        记录日志，补充必要信息，调整结构体
        """
        getattr(module_logger, level)('[{}] {}'.format(category, msg))


G_TM = ToolManager()
