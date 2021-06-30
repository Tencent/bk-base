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


# import inspect
# import logging
# from os import path
# from urllib.parse import quote
#
# from metadata.util.os import dir_create
# from metadata_biz.db_models.bkdata import basic as basic_models
from metadata_contents.config.by_environment.default_conf import DBConfig as DC
from metadata_contents.config.by_environment.default_conf import (
    DgraphBackendConfig as AC,
)
from metadata_contents.config.by_environment.default_conf import InteractorConfig as IC
from metadata_contents.config.by_environment.default_conf import IntroConfig as InC
from metadata_contents.config.by_environment.default_conf import LoggingConfig as LC
from metadata_contents.config.by_environment.default_conf import NormalConfig as NC
from metadata_contents.config.by_environment.default_conf import TaskConfig as TC

RUN_MODE = "prod"
RUN_VERSION = "__BKDATA_VERSION_TYPE__"


class IntroConfig(InC):
    pass


class InteractorConfig(IC):
    pass


class NormalConfig(NC):
    pass


class DBConfig(DC):
    pass


class DgraphBackendConfig(AC):
    pass


class LoggingConfig(LC):
    pass


class TaskConfig(TC):
    pass
