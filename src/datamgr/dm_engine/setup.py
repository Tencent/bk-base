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

# The following comment should be removed at some point in the future.
# mypy: disallow-untyped-defs=False
import codecs
import os

from setuptools import find_packages, setup


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, rel_path), "r", encoding="UTF-8") as fp:
        return fp.read()


def get_version(rel_path):
    return read(rel_path).strip()


long_description = read("README.md")


setup(
    name="dm-engine",
    version=get_version("VERSION"),
    description="A job executor with master-worker structure for running datamanager tasks",
    long_description=long_description,
    url="",
    license="MIT",
    author="blueking",
    author_email="blueking@tencent.com",
    packages=find_packages(),
    package_data={"dm_engine": ["config/tasks/*.yaml"]},
    install_requires=[
        "six>=1.14.0",
        "gevent>=20.4.0",
        "redis==2.10.5",
        "redis-py-cluster==1.3.4",
        "httplib2==0.10.3",
        "pymysql==0.9.2",
        "pytz==2017.2",
        "kazoo",
        "numpy==1.18.3",
        "simplejson==3.11.1",
        "attrs>=19.3.0",
        "subprocess32==3.5.4",
        "arrow==0.15.4",
        "enum34==1.1.6",
        "click",
        "raven==6.5.0",
        "PyYAML",
        "prometheus-client",
    ],
    zip_safe=False,
    python_requires=">=3.6",
    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        "console_scripts": ["bk-dm-engine = dm_engine.manager:cli"],
    },
    include_package_data=True,
)
