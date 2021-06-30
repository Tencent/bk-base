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

jobnavi_dir=$(cd "$(dirname "$0")/..";pwd)
exec_id=$1
type=$2
sys_env=$3
env=$4
logs=$5/exec_$1
port=$6
file_path=$jobnavi_dir/adaptor/$type
task_launcher=$jobnavi_dir/lib/jobnavi-api-python/task_launcher.py

mkdir -p "$logs"

export PYENV_ROOT="$jobnavi_dir/env/python-pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
export PYTHONPATH=$file_path
if command -v pyenv 1>/dev/null 2>&1;
then
  eval "$(pyenv init -)"
fi

pyenv activate $env
if [ $? -ne 0 ]; then
   echo "can not find pyenv $env"
   exit 1
fi

export JOBNAVI_HOME=$jobnavi_dir

python $task_launcher $exec_id $port $jobnavi_dir $logs > $logs/$exec_id.out 2>&1 &
echo $?

