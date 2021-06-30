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


# 项目所有入口集合。
# todo:类型系统延迟加载。
from metadata.util.early_patch import early_patch

early_patch()

import logging  # noqa

import click  # noqa
from metadata.data_quality.entry import cli as data_quality_cli  # noqa
from metadata.runtime import rt_g  # noqa
from metadata.service.access.service_server import cli as access_rpc_server_cli  # noqa
from metadata.service.cli import cli as tasks_cli  # noqa
from metadata.service.cli.type_system import cli as type_system_util_cli  # noqa
from metadata.util.context import load_common_resource  # noqa
from metadata.util.log import init_logging  # noqa
from metadata_contents.config.conf import default_configs_collection  # noqa

cli = click.CommandCollection(sources=[access_rpc_server_cli, type_system_util_cli, tasks_cli, data_quality_cli])

if __name__ == "__main__":
    load_common_resource(rt_g, default_configs_collection)

    logging_config = rt_g.config_collection.logging_config
    logging_config.suffix_postfix = "entry"
    init_logging(logging_config, True)
    logging.getLogger("kazoo").setLevel(logging.INFO)
    logging.getLogger("kafka").setLevel(logging.INFO)
    normal_conf = rt_g.config_collection.normal_config
    rt_g.language = normal_conf.LANGUAGE

    cli()
