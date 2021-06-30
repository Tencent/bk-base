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
# DM-Engine项目

## 简介
数据平台的后台引擎，支持各种业务分布式远程执行。支持业务逻辑插件化载入调用。


## 特点
支持Long(常驻)和Interval(周期)两种任务运行方式，适合不同的业务逻辑。


## 功能列表

1. 使用标准Pip包方式封装，方便和其他服务集成。
2. 目前仅支持 Interval 周期任务，使用 crontab 的配置方式
3. 单 master，多 worker 执行方式

## 测试方式

安装 pytest 依赖包
```
pip install pytest
```

测试指令
```
pytest -v tests/test_master.py  -p no:warnings -s
```


## 使用方式

### 安装

安装 PY 环境，切换到项目根目录，使用python 安装方式 `python setup.py install` 进行安装。

如过程中发现安装依赖卡住，请先单独安装requirements.txt,之后再执行`python setup.py install` 命令进行安装。

初始化数据库，或者选择一个已经存在的数据库，比如 bkdata_engine，执行 SQL 文件
`mysql -uroot -p -d bkdata_engine < ./dm_engine/database/dm_engine.sql`


在工程目录 `conf/settings.py` 下添加配置文件，增添以下内容，按需添加，不填写均由默认值

```
# DM_ENGINE settings
DM_ENGINE_TIMEZONE = 'Asia/Shanghai'
DM_ENGINE_LOG_LEVEL = 'INFO'
DM_ENGINE_LOG_DIR = './logs'
DM_ENGINE_DB_CONFIG = {
    "default": {
        "db_name": "bkdata_engine",
        "db_user": "root",
        "db_password": "123",
        "db_host": "127.0.0.1",
        "db_port": 3306,
        "charset": "utf8",
    }
}
DM_ENGINE_REDIS_CONFIG = {
    "default": {
        'port': 6379,
        "host": "127.0.0.1",
        "password": ""
    }
}

```

### 启动服务

启动 master 服务，执行 `bk-dm-engine --settings=conf.settings run-master`
启动 worker 服务，执行 `bk-dm-engine --settings=conf.settings run-agent`
