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

JOBNAVI_ROOT=$(cd "$(dirname "$0")";pwd)
PACKAGE_ROOT=$JOBNAVI_ROOT/bkdata

mkdir -p $PACKAGE_ROOT
# jobnavischeduler
cd $PACKAGE_ROOT
mkdir -p jobnavischeduler/share/log-aggregation/lib
cd jobnavischeduler
mkdir bin lib
cd $JOBNAVI_ROOT
cp jobnavi-scheduler/target/jobnavi-scheduler-0.4.0-SNAPSHOT.jar $PACKAGE_ROOT/jobnavischeduler/lib
cp jobnavi-scheduler/target/dependency/* $PACKAGE_ROOT/jobnavischeduler/lib
cp jobnavi-log-aggregation/target/jobnavi-log-aggregation-0.4.0-SNAPSHOT.jar $PACKAGE_ROOT/jobnavischeduler/share/log-aggregation/lib
cp jobnavi-log-aggregation/target/dependency/* $PACKAGE_ROOT/jobnavischeduler/share/log-aggregation/lib
cp jobnavi-scheduler/src/main/bin/* $PACKAGE_ROOT/jobnavischeduler/bin
cp project_scheduler.yml $PACKAGE_ROOT/jobnavischeduler/project.yml
dos2unix $PACKAGE_ROOT/jobnavischeduler/bin/*.sh

# jobnavirunner
cd $PACKAGE_ROOT
mkdir -p jobnavirunner/share/log-aggregation/lib
cd jobnavirunner
mkdir bin lib env adaptor tool
cd $JOBNAVI_ROOT
cp jobnavi-runner/target/jobnavi-runner-0.4.0-SNAPSHOT.jar $PACKAGE_ROOT/jobnavirunner/lib
cp jobnavi-api/target/jobnavi-api-0.4.0-SNAPSHOT.jar $PACKAGE_ROOT/jobnavirunner/lib
# jobnavirunner python api
cp jobnavi-rpc/src/main/python/* jobnavi-api-python/jobnavi -r
mkdir -p $PACKAGE_ROOT/jobnavirunner/lib/jobnavi-api-python
cp jobnavi-api-python/requirements.txt $PACKAGE_ROOT/jobnavirunner/lib/jobnavi-api-python
cp jobnavi-api-python/task_launcher.py $PACKAGE_ROOT/jobnavirunner/lib/jobnavi-api-python
cp jobnavi-api-python/jobnavi $PACKAGE_ROOT/jobnavirunner/lib/jobnavi-api-python -r
# jobnavirunner log aggregation
cp jobnavi-log-aggregation/target/jobnavi-log-aggregation-0.4.0-SNAPSHOT.jar $PACKAGE_ROOT/jobnavirunner/share/log-aggregation/lib
cp jobnavi-log-aggregation/target/dependency/* $PACKAGE_ROOT/jobnavirunner/share/log-aggregation/lib
cp jobnavi-runner/src/main/bin/* $PACKAGE_ROOT/jobnavirunner/bin
cp project_runner.yml $PACKAGE_ROOT/jobnavirunner/project.yml
dos2unix $PACKAGE_ROOT/jobnavirunner/bin/*.sh

# task adaptors
cd $PACKAGE_ROOT
for adaptor in `cat $JOBNAVI_ROOT/adaptors.txt`
do
    read type_id module <<< $(echo $adaptor|awk -F':' '{print $1, $2}')
    mkdir -p jobnavirunner/adaptor/$type_id/stable
    cp $JOBNAVI_ROOT/$module/target/jobnavi*.jar jobnavirunner/adaptor/$type_id/stable/
    # copy adaptor dependencies
    if [ -d $JOBNAVI_ROOT/$module/target/dependency ]
    then
        mkdir jobnavirunner/adaptor/$type_id/stable/lib
        cp $JOBNAVI_ROOT/$module/target/dependency/* jobnavirunner/adaptor/$type_id/stable/lib
    fi
    # copy adaptor scripts
    if [ -d $JOBNAVI_ROOT/$module/src/main/bin ]
    then
        cp $JOBNAVI_ROOT/$module/src/main/bin jobnavirunner/adaptor/$type_id/stable/ -r
    fi
done
dos2unix jobnavirunner/adaptor/*/stable/bin/*.sh
chmod 755 jobnavirunner/adaptor/*/stable/bin/*.sh

# package
cd $JOBNAVI_ROOT
if [ -f jobnavi.tar.gz ]
then
    mv jobnavi.tar.gz jobnavi.tar.gz.bak
fi
tar -czf jobnavi.tar.gz bkdata
rm -rf $PACKAGE_ROOT