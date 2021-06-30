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

current_path=$(cd "$(dirname "$0")";pwd)
jobnavi_dir=$(cd "$(dirname "$0")/../..";pwd)
jobnavi_conf=$jobnavi_dir/conf
exec_id=$1
env=$2
logs=$3/exec_$1
adaptor_lib=$current_path/lib
env_conf=$jobnavi_dir/env/$2/conf
lib=$jobnavi_dir/adaptor/$2

mkdir -p $logs

JOBNAVI_OPTS="-Xms100m -Xss10m -Xloggc:$logs/task-gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=1M"
JOBNAVI_OPTS="$JOBNAVI_OPTS -DJOBNAVI_HOME=$jobnavi_dir -Dlog4j.configuration=file:$jobnavi_conf/log4j-task.properties -Dlog4j.log.dir=$logs"

CLASSPATH=$adaptor_lib/*:$current_path/*
CLASSPATH=$CLASSPATH:$env_conf:$jobnavi_conf

java $JOBNAVI_OPTS -cp $CLASSPATH com.tencent.blueking.dataflow.jobnavi.adaptor.flink.YarnSessionMain "$@" 2>&1
echo $?

