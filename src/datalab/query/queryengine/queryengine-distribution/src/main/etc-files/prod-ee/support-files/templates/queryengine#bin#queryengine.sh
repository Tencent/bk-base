#!/bin/bash -l
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname "$DIR")"
cd ${BASH_SOURCE%/*} 2>/dev/null
WORK_DIR=$(pwd)
EXT_DIR="${PARENT_DIR}/ext/"

cp "${PARENT_DIR}/lib/protobuf-java-3.5.1.jar" "${EXT_DIR}"
export FEDERATION_CONF=${PARENT_DIR}/conf/federation
source $CTRL_DIR/utils.fc

cd $WORK_DIR

exec $JAVA_HOME/bin/java -DQUERYENGINE_HOME=${PARENT_DIR} \
    -DBK_TIMEZONE="__BK_TIMEZONE__" \
    -Xms2048m \
    -Xmx2048m \
    -XX:+UseG1GC \
    -XX:+UseGCOverheadLimit \
    -XX:+ExplicitGCInvokesConcurrent \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=${PARENT_DIR}/logs/`date +%Y%m%d%H%M%S`_dump.log \
    -XX:+ExitOnOutOfMemoryError \
    -XX:+PrintGCTimeStamps \
    -XX:+PrintGCDateStamps \
    -XX:+PrintGCDetails \
    -Djava.awt.headless=true \
    -verbose:gc -Xloggc:${PARENT_DIR}/logs/`date +%Y%m%d%H%M%S`_gc.log \
    -Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.port=8999 \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.rmi.port=8999 \
    -Djava.ext.dirs="${EXT_DIR}:$JAVA_HOME/jre/lib/ext" \
    -Dspring.config.location="${PARENT_DIR}/configure/application.yml" \
    -cp "${WORK_DIR}/../lib/*" com.tencent.bk.base.datalab.queryengine.server.BootApplication \
    -c ${WORK_DIR%/bin}/conf/common
