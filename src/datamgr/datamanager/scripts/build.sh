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

WORKSPACE=$(pwd)

RUN_VERSION=$1
BUILD_NO=$2

# 清理旧的构件
rm -rf ./dist

# 提取版本号
DATAMANAGER_VERSION=$(cat ../../../../VERSION)

package_name="datamanager_${RUN_VERSION}_v${DATAMANAGER_VERSION}-${BUILD_NO}.tar.gz"

# 移动对应模块的配置文件至打包目录
mkdir bkdata
mv ./datamanager ./bkdata/

# 生成 project.yml 文件
sed -i 's/__RUN_VERSION__/'$RUN_VERSION'/g' "./bkdata/datamanager/project.yml"
echo $DATAMANAGER_VERSION-${BUILD_NO} > ./bkdata/datamanager/VERSION

# 分别移动 ms/sql/template
cd ../../../..
rsync -r support-files/datamgr/datamanager/ src/datamgr/datamanager/dist/bkdata/support-files
cd ${WORKSPACE}


echo "start build tar..."
tar czf $package_name bkdata
echo "====================PAKCAGE======================"
ls | grep tgz
