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

if [[ -f "/etc/profile" ]]; then
    source /etc/profile
fi

workdir=$( cd $(dirname $0) && pwd )
conf_file=$workdir/../etc/databus.properties
log_dir=__LOGS_HOME__

if [ -z "$JAVA_HOME" ]; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi


export DATABUS_OPTS="-Ddatabus.properties.file=$conf_file"
export CLASSPATH=".":"$workdir/../lib/*"
export HEAP_OPTS="-Xmx4096M -Xms1024M -Xss2M"
export JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:+PrintGCDetails -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxDirectMemorySize=1024M -XX:+HeapDumpOnOutOfMemoryError -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=128K -Xloggc:__LOGS_HOME__/connector_manager_gc.log -Djava.awt.headless=true"
export LOG4J_OPTS="-Dlog4j.configuration=file:$workdir/../etc/log4j.properties -Ddatabus.logdir=$log_dir"

### 设置语言、时区等环境变量 ###
export TZ_OPTS="-Ddatabus.display.timezone=__GMT_TIME_ZONE__"
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"

# 记录集群启动参数
echo $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $LOG4J_OPTS $DATABUS_OPTS $TZ_OPTS -cp $CLASSPATH com.tencent.bk.base.datahub.hubmgr.DatabusMgr
exec $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $LOG4J_OPTS $DATABUS_OPTS $TZ_OPTS -cp $CLASSPATH com.tencent.bk.base.datahub.hubmgr.DatabusMgr
