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
[TOC]

## 1. 环境准备

### 1.1 初始化 PY 环境

- 安装 Python 3.6.6

- 安装 MySQL，新建数据库 bkdata_basic

- 安装常规的 Python 依赖包
    ```bash
    pip install -r 01_requirements.txt
    ```

- 安装自研的 Python 依赖包，请从 support-file 仓库中获取
    ```
    pip install -r 02_requirements.txt
    ```

- 安装任务执行框架 DMEngine

    从蓝盾流水线下载 DMEngine 安装包，执行安装

### 1.2 增添项目配置

需要添加 conf/settings.py 文件，请从 support-file 仓库中获取

其中与 DMEngine 有关的配置有：

- DM_ENGINE_TIMEZONE     ：框架默认时区
- DM_ENGINE_LOG_LEVEL    ：框架默认日志级别
- DM_ENGINE_LOG_DIR      ：框架日志目录
- DM_ENGINE_DB_CONFIG    ：框架依赖的数据库，存放任务调度配置
- DM_ENGINE_REDIS_CONFIG ：框架依赖的队列存储，存放任务队列和框架各组件的状态


### 1.3 初始化数据库

- 选择数据库 bkdata_basic
- 在工程目录中找到对应的 dm_engine/database/dm_engine.sql 文件，执行 SQL 文件



## 2. 添加自定义任务

各个模块可以维护自己的入口文件，以 auth 为例子，在 auth 目录下创建 tasks 文件

```
import click

from dm_engine import dm_task
from auth.sync_data_managers import transmit_meta_operation_record

click.disable_unicode_literals_warning = True


@click.group()
def entry():
    pass

entry.add_command(click.command('transmit-meta-operation-record')(dm_task(transmit_meta_operation_record)))

if __name__ == '__main__':
    entry()
```

### 2.1 本地简单测试

进入工程目录 datamanager 执行任务 `python -m auth.tasks transmit-meta-operation-record`

可在日志 `logs/None.console.log` 中查看任务日志

### 2.2 框架运行任务

```
bk-dm-engine --settings=conf.settings run-master
bk-dm-engine --settings=conf.settings run-agent
```

在数据库配置任务

```
INSERT INTO dm_engine_task_config
  (`task_code`,`task_name`, `task_category`, `task_loader`, `task_entry`, `task_params`, `task_status`, `work_type`, `work_crontab`, `work_status_interval`, `work_timeout`)
VALUES
  ('auth_transmit_meta_operation_record', '解读元数据事件', 'auth', 'command', 'auth.tasks', 'transmit-meta-operation-record', 'on', 'long', '* * * * *', '60', '60');
```

如果没有主动配置 DM_ENGINE_LOG_DIR，可在日志 `logs/auth_transmit_meta_operation_record.console.log` 中查看任务日志

## 3. 单元测试

- 安装测试依赖包，执行 `pip install mock`

- 初始化模拟数据，执行 `sh tests/bin/reinit_test_db.sh`

- 执行指定模块的单元测试 `pytest -v tests/metadata -s`，任务的日志会被全部重定向至工程目录下的 logs/testcase.log 文件


## 4. 代码规范

所有代码需要见过 PEP8 检查，这里选择 flake8 作为本地检查工具， 提交时执行  `flake8 .` 进行检查，推荐使用 `black --line-length 120` 进行代码修复。

