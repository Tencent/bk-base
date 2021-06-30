#Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
#
#Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
#
#BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
#
#License for BK-BASE 蓝鲸基础平台:
#--------------------------------------------------------------------
#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
#documentation files (the "Software"), to deal in the Software without restriction, including without limitation
#the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
#and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all copies or substantial
#portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
#LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
#NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
#WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#! /bin/bash

log_dir=__LOGS_HOME__
mkdir -p ${log_dir}/codecheck
log=${log_dir}/codecheck/codecheck_server.log

BASEDIR=$(dirname $0)

java -Dlog.file="${log}" \
      -cp "${BASEDIR}/../lib/*:${BASEDIR}/../conf/" -Xmx4g -Xms4g -XX:MaxTenuringThreshold=10 \
      -XX:+DisableExplicitGC -XX:SurvivorRatio=8 -XX:-OmitStackTraceInFastThrow -XX:+UseMembar \
      -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSScavengeBeforeRemark \
      -XX:ParallelCMSThreads=4 -XX:+UseCMSCompactAtFullCollection -verbose:gc \
      -XX:+PrintHeapAtGC -Xloggc:../log/gc.log -XX:+PrintGCDetails -XX:+HeapDumpOnOutOfMemoryError \
      -XX:HeapDumpPath=../log/ -XX:+PrintGCDateStamps -XX:+CMSClassUnloadingEnabled \
      -XX:CMSInitiatingOccupancyFraction=80 -XX:CMSFullGCsBeforeCompaction=1 -Dsun.net.inetaddr.ttl=3 \
      -Dsun.net.inetaddr.negative.ttl=1 -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=3 \
      -XX:GCLogFileSize=128M com.tencent.bk.base.codecheck.http.CodeCheckServer >> /dev/null 2>&1

