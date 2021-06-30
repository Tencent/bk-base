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

import os
import sys

from metadata.util.conf import basic_configs_collection_gen

RUN_MODE = os.environ.get("RUN_MODE", "test")

if RUN_MODE == "test":
    from metadata_contents.config.by_environment.test_conf import *  # noqa
elif RUN_MODE == "prod":
    from metadata_contents.config.by_environment.templated_conf import *  # noqa
else:
    raise ValueError("Invalid run mode.")

default_configs_collection = basic_configs_collection_gen(
    *[
        {"config_cls": config_cls}
        for config_cls in [
            IntroConfig,
            DBConfig,
            LoggingConfig,
            NormalConfig,
            TaskConfig,
            DgraphBackendConfig,
            InteractorConfig,
        ]
    ]
)

print("Run mode is {}.".format(RUN_MODE), file=sys.stderr)
