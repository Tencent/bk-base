# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from __future__ import absolute_import, print_function, unicode_literals

from setuptools import find_packages, setup

with open("VERSION", "r") as fw:
    version_str = fw.read().strip()

setup(
    name="metadata_sdk",
    version=version_str,
    packages=find_packages(exclude=("tests", "tasks")),
    url="",
    license="",
    author="blueking",
    author_email="blueking",
    description="MetaDataSDK",
    install_requires=[
        "attrs",
        "tblib",
        "jsonext",
        "gevent",
        "kazoo",
        "requests",
        "sqlalchemy",
        "dictdiffer",
        "kombu",
        "setuptools==44.1.0",
        'enum34~=1.1.6;python_version<"3.4"',
        'crypto~=1.4.1;python_version<"3.4"',
        'pycrypto;python_version>="3.6"',
        "concurrent-log-handler",
    ],
)
