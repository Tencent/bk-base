<!---
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
-->
# Release

## v1.0.0

- 【功能】支持Long(常驻)和Interval(周期)两种任务运行方式，适合不同的业务逻辑。
- 【功能】单 master，多 worker 执行方式，worker 支持自动扩展
- 【功能】支持从 MySQL 中任务表获取任务
- 【功能】支持任务、进程状态上报至 Redis
- 【功能】提供管理指令，查看任务和进程状态
- 【功能】提供标准 PIP 打包方式


## v1.0.1
- 【功能】删除 supervisor 依赖包


## v1.0.2
- 【优化】移除 click、kazoo、six、gevent 的强版本依赖


## v1.0.3
- 【功能】支持 once（一次性）任务
- 【功能】支持通过本地 conf/task/*.yaml 来配置任务
- 【功能】内置 SyncDB 任务，负责将执行记录写入 MySQL
- 【优化】更新任务执行方式为 multiprocess 方式
- 【优化】增添 list_plan_tasks、run_task_local、clear_status、generate_task_sql 管理指令
