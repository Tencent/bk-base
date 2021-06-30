# -- coding: utf-8 --
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import multiprocessing

# 获取当前CPU线程数
CPU_COUNT = multiprocessing.cpu_count()

USER = "user"
# 权限表达式判断条件
OR = "OR"
# 默认单页拉取的数量
PAGE_SIZE = 500
# 线程池大小
POOL_SIZE = CPU_COUNT * 2

# 每次同步的资源数量
RESOURCE_NUMBER = 20

# 拓扑授权  分隔符
SEPARATOR = "&"

# key 分隔符
KEY_SEPARATOR = "$"

# 资源授权类型 ：instance：1, simple_path: project&1, complex_path: project&1#flow&2
SIMPLE_PATH = "simple_path"
COMPLEX_PATH = "complex_path"
INSTANCE = "instance"

# 主线程 心跳日志 的间隔时间, 默认每五秒检查一次
CHECK_POOL_INTERVAL_TIME = 5
