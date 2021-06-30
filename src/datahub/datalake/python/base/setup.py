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
import shutil
from os import path

from setuptools import setup

PACKAGE_NAME = "tencent_bkbase_datalake"
VERSION = "4.0.0-SNAPSHOT"  # 此版本号需要与datalake/java目录pom.xml文件中版本号一致


def before_setup():
    """
    生成python包前的准备动作：将数据湖的jar包拷贝到lib目录中
    """
    here = path.abspath(path.dirname(__file__))
    jar_file = "datalake-sdk-%s-with-dependencies.jar" % VERSION
    if path.exists(path.join(here, PACKAGE_NAME, "lib", jar_file)):
        return

    # jar文件不存在，需要从datalake的java项目下获取
    datalake_root = path.abspath(path.join(os.getcwd(), path.join("..", "..")))
    jar_path = path.join(datalake_root, "java", "base", "target", jar_file)
    if not path.exists(jar_path):
        # 校验datalake的java sdk已成功编译出datalake-sdk-xxx-with-dependencies.jar包
        raise Exception("file %s not exists, please build it first!" % jar_file)

    # 清理lib目录下的所有文件
    lib_dir = path.join(here, PACKAGE_NAME, "lib")
    if path.exists(lib_dir):
        for dir_path, _, file_list in os.walk(lib_dir):
            for file_name in file_list:
                os.remove(path.join(dir_path, file_name))
    else:
        os.mkdir(lib_dir)

    # 将jar文件拷贝到lib目录下
    shutil.copyfile(jar_path, path.join(lib_dir, jar_file))


before_setup()

setup(
    name=PACKAGE_NAME.replace("_", "-"),
    version=VERSION,
    author="blueking",
    author_email="blueking@tencent.com",
    description="python sdk for datalake",
    long_description="sdk used to read/write data from/to datalake",
    keywords=["bkbase", "datalake", "iceberg"],
    url="",
    include_package_data=True,
    packages=[PACKAGE_NAME],
    zip_safe=False,
    install_requires=["JPype1>=1.0.2"],
    python_requires=">=3.5",
    platforms=["any"],
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: DataLake SDK",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
