#!/bin/bash
# Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
#
# Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
#
# BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
#
# License for BK-BASE 蓝鲸基础平台:
# --------------------------------------------------------------------
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial
# portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
# LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
# NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

BKBASE_SPACE=$(dirname $(dirname $(dirname "$PWD")))
WORKSPACE="${BKBASE_SPACE}"/src/dataflow/unified-computing
UDF_SPACE="${BKBASE_SPACE}"/src/api/dataflow/udf

cd "${WORKSPACE}" || exit

# 编译udf依赖包并且打成tar包
mvn clean scala:compile compile package -pl core -am
mv "${WORKSPACE}"/core/target/core-*.jar \
"${WORKSPACE}"/udf/udf-core/src/main/resources/lib
tar -cvf "${UDF_SPACE}"/origin-udf.tar udf pom.xml

# 解压tar包到udf工作目录
cd "${UDF_SPACE}" || exit
rm -rf origin-udf
mkdir origin-udf

tar -xvf origin-udf.tar -C origin-udf

rm origin-udf.tar

if [ ! -d "${UDF_SPACE}/workspace" ]; then
    mkdir -p "${UDF_SPACE}/workspace"
fi

echo "${UDF_SPACE}/workspace"

mv origin-udf "${UDF_SPACE}"/workspace/bkdata_udf_project
