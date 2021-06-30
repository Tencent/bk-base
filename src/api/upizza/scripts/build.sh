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
SUBMODULE=$1
RUN_VERSION=$2
# 添加构建号作为小版本号
BUILD_NO=$3

PIZZA_ROOT=${WORKSPACE}/${SUBMODULE}api

cd "${WORKSPACE}" || exit

# 打包工作目录
PACKAGE_HOME="bkdata"
if [ -d "./$PACKAGE_HOME" ]; then
    rm -rf ./$PACKAGE_HOME
fi
mkdir $PACKAGE_HOME
mkdir -p $PACKAGE_HOME/support-files

# 获取版本号
# 移动子模块 VERSION 文件
if [ -f "$PIZZA_ROOT/${SUBMODULE}/VERSION" ]; then
    cp $PIZZA_ROOT/${SUBMODULE}/VERSION $PIZZA_ROOT/
    echo 'Succeed to move submodule VERSION file'
fi

if [ ! -f "$PIZZA_ROOT/VERSION" ]; then
    project_version="0.0.0"
else
    project_version=$(cat $PIZZA_ROOT/${SUBMODULE}/VERSION)
fi
# 对 VERSION 文件添加小版本号
echo "${project_version}-${BUILD_NO}" > $PIZZA_ROOT/${SUBMODULE}/VERSION
echo "${project_version}-${BUILD_NO}" > $PIZZA_ROOT/VERSION
VERSION="$project_version"

echo "--variables for project.yml"
echo $SUBMODULE, $VERSION, $RUN_VERSION
if [ -f ${PIZZA_ROOT}/project.yml.example ]; then
  mv $PIZZA_ROOT/project.yml.example $PIZZA_ROOT/project.yml
fi

# 生成 project.yml 文件
sed -i 's/__SUBMODULE_API__/'$SUBMODULE'api/g' "${PIZZA_ROOT}/project.yml"
sed -i 's/__RUN_VERSION__/'$RUN_VERSION'/g' "${PIZZA_ROOT}/project.yml"

## 生成临时中间文件
cp $PIZZA_ROOT/requirements.txt $PIZZA_ROOT/tmp_requirements.txt

# 合并子模块的 requirements.txt
if [ -f ${PIZZA_ROOT}/${SUBMODULE}/requirements.txt ]; then
  cat ${PIZZA_ROOT}/${SUBMODULE}/requirements.txt >> ${PIZZA_ROOT}/tmp_requirements.txt
fi

# 合并子模块 extend/requirements.txt
if [ -f ${PIZZA_ROOT}/${SUBMODULE}/extend/requirements.txt ]; then
  cat ${PIZZA_ROOT}/${SUBMODULE}/extend/requirements.txt >> ${PIZZA_ROOT}/tmp_requirements.txt
fi

cat ${PIZZA_ROOT}/tmp_requirements.txt | grep -v "#" | sort | uniq > ${PIZZA_ROOT}/requirements.txt
rm $PIZZA_ROOT/tmp_requirements.txt

# 移动或者生成 project.yml 文件
if [ -f "$PIZZA_ROOT/${SUBMODULE}/project.yml" ]; then
    rm -f $PIZZA_ROOT/project.yml
    cp $PIZZA_ROOT/${SUBMODULE}/project.yml $PIZZA_ROOT/
    echo 'Succeed to move submodule project.yml file'
fi

# 移动子模块 on_migrate 文件
if [ -f "$PIZZA_ROOT/${SUBMODULE}/on_migrate" ]; then
    cp $PIZZA_ROOT/${SUBMODULE}/on_migrate $PIZZA_ROOT/
    chmod a+x $PIZZA_ROOT/on_migrate
    echo 'Succeed to move submodule on_migrate file'
fi

# 移动子模块 pre_start 文件
if [ -f "$PIZZA_ROOT/${SUBMODULE}/pre_start" ]; then
    cp $PIZZA_ROOT/${SUBMODULE}/pre_start $PIZZA_ROOT/
    chmod a+x $PIZZA_ROOT/pre_start
    echo 'Succeed to move submodule pre_start file'
fi

# 移动子模块 post_start 文件
if [ -f "$PIZZA_ROOT/${SUBMODULE}/post_start" ]; then
    cp $PIZZA_ROOT/${SUBMODULE}/post_start $PIZZA_ROOT/
    chmod a+x $PIZZA_ROOT/post_start
    echo 'Succeed to move submodule post_start file'
fi

# 移动子模块 release.md 文件
if [ -f "$PIZZA_ROOT/${SUBMODULE}/release.md" ]; then
    cp $PIZZA_ROOT/${SUBMODULE}/release.md $PIZZA_ROOT/
    echo 'Succeed to move submodule release.md file'
fi

# 移动代码至打包目录
mv -v $PIZZA_ROOT $PACKAGE_HOME/

# 移动对应模块的配置文件至打包目录
mkdir ${PACKAGE_HOME}/support-files/metadata ${PACKAGE_HOME}/support-files/sql ${PACKAGE_HOME}/support-files/templates

# 分别移动 ms/sql/template
ls ../../../../support-files/api/metadata | grep ${SUBMODULE}api | xargs -I {} cp ../../../../support-files/api/metadata/{} ${PACKAGE_HOME}/support-files/metadata/
ls ../../../../support-files/api/sql | grep ${SUBMODULE}api |xargs -I {} cp ../../../../support-files/api/sql/{} ${PACKAGE_HOME}/support-files/sql/
ls ../../../../support-files/api/templates | grep ${SUBMODULE}api | xargs -I {} cp ../../../../support-files/api/templates/{} ${PACKAGE_HOME}/support-files/templates/

# 移动和生成 pizza 依赖的环境文件
cp ../../../../support-files/api/templates/upizza#env ${PACKAGE_HOME}/support-files/templates/${SUBMODULE}api#env
echo "APP_NAME=${SUBMODULE}" >> ${PACKAGE_HOME}/support-files/templates/${SUBMODULE}api#env

# 移动自定义包
if [ -d ../../../../pkgs ]; then
  cp -rf ../../../../pkgs ${PACKAGE_HOME}/support-files/pkgs
fi

package_name="${SUBMODULE}api_${RUN_VERSION}_v${VERSION}-${BUILD_NO}.tgz"
# 打包
tar czf $package_name bkdata/

# 清理中间目录
rm -rf ./bkdata

echo "====================PAKCAGE======================"
ls | grep tgz
