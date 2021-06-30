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

from setuptools import setup

name = "notebook-command"
# VERSION文件来自于bk-base项目根目录，在jupyter Dockerfile文件中做了相应处理
version = open("VERSION").read().strip()
install_requires = ["ipython>=1.0", "requests"]
packages = []
for d, _, _ in os.walk("command"):
    if os.path.exists(os.path.join(d, "__init__.py")):
        packages.append(d.replace(os.path.sep, "."))

setup(
    name=name,
    version=version,
    python_requires=">=3",
    description="",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    keywords="notebook",
    packages=packages,
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
