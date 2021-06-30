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
jobnavi_dir=$(cd "$(dirname "$0")/../../..";pwd)
adaptor_dir=$(cd "$(dirname "$0")/../";pwd)
jobnavi_conf=$jobnavi_dir/conf
exec_id=$1
env=$2
logs=$3/exec_$1
job_args=$4

mkdir -p $logs
adaptor_lib="${adaptor_dir}"/*
export HADOOP_CONF_DIR=$jobnavi_dir/env/$env/conf
export YARN_CONF_DIR=$jobnavi_dir/env/$env/conf
export FLINK_LOG_DIR=$logs

###################################################################################
# env bin
bin=$jobnavi_dir/env/$env/bin

_FLINK_HOME_DETERMINED=1
FLINK_HOME=$jobnavi_dir/env/$env
export FLINK_HOME

# get flink config
. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

CC_CLASSPATH=`constructFlinkClassPath`

log_setting=(-Dlog4j.configuration=file:"$jobnavi_conf"/log4j-task.properties -Dlog4j.log.dir="$logs")

# Add HADOOP_CLASSPATH to allow the usage of Hadoop file systems


exec $JAVA_RUN $JVM_ARGS "${log_setting[@]}" -classpath "`manglePathList "$adaptor_lib:$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" com.tencent.blueking.dataflow.jobnavi.adaptor.flink.FlinkCommandMainV2 ${job_args}

