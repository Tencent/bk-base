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
from environs import Env

env = Env()
env.read_env("env", recurse=False)

# 从框架目录下，获取 env 变量文件获取当前激活的 APP_NAME
APP_NAME = env.str("APP_NAME")
RUN_MODE = env.str("RUN_MODE")

# 具体应用所在目录，在本地模式下 APP 所在目录不在 upizza 下，而是与 upizza 同级
if RUN_MODE == "LOCAL":
    APP_DIR = os.path.join(os.path.dirname(os.getcwd()), APP_NAME)
else:
    APP_DIR = os.path.join(os.getcwd(), APP_NAME)

# 进一步激活应用目录的 env 变量文件，存在则激活，写入环境变量
env.read_env(f"{APP_DIR}/env", recurse=False)

from pizza.settings_default import *  # noqa

try:
    module = __import__("{}.pizza_settings".format(APP_NAME), globals(), locals(), ["*"])
    for setting in dir(module):
        if setting == setting.upper():
            locals()[setting] = getattr(module, setting)
except ImportError:
    print(f"不存在自定义模块（{APP_NAME}）配置")
