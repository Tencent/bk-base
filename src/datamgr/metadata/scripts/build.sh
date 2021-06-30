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


# 设置全局变量
RUN_VERSION=$1
BUILD_NO=$2
SUBMODULE="metadata"
# 设置工作目录变量
WORKSPACE=$(pwd)
PACKAGE_ROOT=${WORKSPACE}/bkdata
METADATA_CODE_ROOT=${PACKAGE_ROOT}/metadata

# 移动对应模块的配置文件至打包目录
cd "${WORKSPACE}" || exit
# 清理工作空间
mkdir -p "${PACKAGE_ROOT}"
mv ./metadata "${PACKAGE_ROOT}"

# 开始流程
echo "start build tar ..."
cd "${WORKSPACE}" || exit

# 组装support-files
if [ -d "${PACKAGE_ROOT}/support-files-default" ]; then
    rm -rf "${PACKAGE_ROOT}/support-files-default"
fi
mkdir -p "${PACKAGE_ROOT}/support-files-default"
cp "${METADATA_CODE_ROOT}/metadata_contents/config/by_environment/default_conf.py" "${PACKAGE_ROOT}/support-files-default/"
cp "${METADATA_CODE_ROOT}/metadata_contents/config/by_environment/__init__.py" "${PACKAGE_ROOT}/support-files-default/"
rm -rf "${METADATA_CODE_ROOT}/metadata_contents/config/by_environment"
mkdir -p "${METADATA_CODE_ROOT}/metadata_contents/config"
mv "${PACKAGE_ROOT}/support-files-default" "${METADATA_CODE_ROOT}/metadata_contents/config/by_environment"
FIXED_CONF_FILE="${METADATA_CODE_ROOT}/metadata_contents/config/by_environment/fixed_conf.json"
touch "${FIXED_CONF_FILE}"
echo "{\"normal_config.AVAILABLE_BACKENDS\": {\"mysql\": true}}" > "${FIXED_CONF_FILE}"
SUPPORT_FILES_ROOT=${PACKAGE_ROOT}/support-files
mkdir -p "${SUPPORT_FILES_ROOT}"
cd ../../../../ && cp -r support-files/datamgr/metadata/* "${SUPPORT_FILES_ROOT}/"
SUBMODULE_VERSION=$(cat ./VERSION)
# 临时拉取gitlab的support-files
cd ../
cp "support-files-gitlab/${RUN_VERSION}/metadata/templates/metadata#metadata_contents#config#by_environment#templated_conf.py" "${SUPPORT_FILES_ROOT}/templates/"
cp "support-files-gitlab/${RUN_VERSION}/metadata/templates/metadata#metadata_contents#config#by_environment#test_conf.py" "${SUPPORT_FILES_ROOT}/templates/"
cp "support-files-gitlab/${RUN_VERSION}/metadata/templates/#etc#supervisor-bkdata-metadata.conf" "${SUPPORT_FILES_ROOT}/templates/"

# 打包
cd "${WORKSPACE}" || exit
echo "${SUBMODULE_VERSION}-${BUILD_NO}" > "${METADATA_CODE_ROOT}/VERSION"
package_name="${SUBMODULE}_${RUN_VERSION}_v${SUBMODULE_VERSION}-${BUILD_NO}.tar.gz"
tar czf "${package_name}" ./bkdata
echo "generate package ${package_name} success!"
