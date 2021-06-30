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

schedule_url=$1
schedule_hierarchy_amount=$2
schedule_item_for_hierarchy=$3

if [ $# -le 2 ]; then
  echo "Usage: gen_schedule_info.sh <schedule_url> <schedule_hierarchy_amount> <schedule_item_for_hierarchy>"
  exit 1
fi

for (( i=0; i<${schedule_hierarchy_amount}; i++));
do
  name="591_test_offline_performance_"${i}
  for (( j=0; j<${schedule_item_for_hierarchy}; j++));
  do
    parentName=${name}
    name=${name}"_"${j}
    if [[ ${j} -eq 0 ]]; then
      curl -l -H "Content-type: application/json" -X POST -d '{"scheduleId":"'${name}'","extraInfo":"{\"run_mode\":\"DEVELOP\",\"makeup\":false,\"debug\":false,\"type\":\"batch_sql\"}", "period": {"timezone": "Asia/Shanghai","start_time":"1523863440000","frequency":1,"period_unit":"h"},"operate":"add","typeName":"thread","description":"Project submit by XXX","recovery":{"enable":true,"intervalTime":"1M","retry_times": 3}}' ${schedule_url}/schedule
    else
      curl -l -H "Content-type: application/json" -X POST -d '{"scheduleId":"'${name}'","extraInfo":"{\"run_mode\":\"DEVELOP\",\"makeup\":false,\"debug\":false,\"type\":\"batch_sql\"}", "period": {"timezone": "Asia/Shanghai","start_time":"1523863440000","frequency":1,"period_unit": "h"},"parents":[{"value":"1h","type":"fixed","parentId":"'${parentName}'","rule":"all_finished"}],"operate":"add","typeName":"batch","description":"Project submit by XXX"}' ${schedule_url}/schedule
    fi
  done
done
