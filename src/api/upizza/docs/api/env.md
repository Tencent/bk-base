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

如何申明 API 依赖的环境配置

## 1. 创建模板文件
API 模块通过在 support-files 目录中申明模板文件 `support-files/api/templates/authapi#auth#env`，该模板文件表示在最终部署时，会将
该文件进行渲染，然后放置在部署目录 `${BKEE_DIR}` 下的 `authapi/auth/env` 路径。

文件内容包含一堆依赖部署环境的占位符，大体内容如下。

```
DATAFLOW_DB_HOST=__MYSQL_DATAFLOW_IP0__
DATAFLOW_DB_PORT=__MYSQL_DATAFLOW_PORT__
DATAFLOW_DB_USER=__MYSQL_DATAFLOW_USER__
DATAFLOW_DB_PASSWORD=__MYSQL_DATAFLOW_PASS__

DATALAB_DB_HOST=__MYSQL_DATALAB_IP0__
DATALAB_DB_PORT=__MYSQL_DATALAB_PORT__
DATALAB_DB_USER=__MYSQL_DATALAB_USER__
DATALAB_DB_PASSWORD=__MYSQL_DATALAB_PASS__

REDIS_HOST=__REDIS_HOST__
REDIS_NAME=__REDIS_MASTER_NAME__
REDIS_PORT=__REDIS_PORT__
REDIS_PASS=__REDIS_PASS__

RABBITMQ_HOST=__RABBITMQ_HOST__
RABBITMQ_PORT=__RABBITMQ_PORT__

DATAWEB_SAAS_FQDN=__BKDATA_DATAWEB_SAAS_FQDN__

BKIAM_HOST=__BKIAM_HOST__
BKIAM_PORT=__BKIAM_PORT__

DATAHUB_API_HOST=__BKDATA_DATAHUBAPI_HOST__
DATAHUB_API_PORT=__BKDATA_DATAHUBAPI_PORT__
DATAFLOW_API_HOST=__BKDATA_DATAFLOWAPI_HOST__
DATAFLOW_API_PORT=__BKDATA_DATAHUBAPI_PORT__
``` 

形如 __MYSQL_DATAFLOW_IP0__ 的数值为占位符，在实际部署中会被渲染为环境中的数值。

## 2. 通过 env 文件形式来申明环境依赖

[根据微服务12要素原则](https://12factor.net/zh_cn/config) 中对配置的申明方式提出了原则上的建议，将具有环境特性的依赖配置以环境变量的
形式进行申明。

Python 项目推荐使用 `environs` 包加载 env 环境变量文件来引入环境变量。

### 2.1 安装

PIZZA 已经在 requirements.in 申明了对 environs 的依赖，子模块可以不用申明
```
pip install environs
```

### 2.2 在工程目录下创建 env 文件

PIZZA 目前会独立申明自身的 env 文件，以 `support-files/api/templates/upizza#env` 路径存放，打包时放置项目的根目录中。子模块无需关注
PIZZA 所依赖的配置，子模块只需申明自身的 env 配置文件即可，类似步骤1中的 `authapi/auth/env` 文件。

### 2.3 在项目中的使用方式

目前 PIZZA 框架将会自动加载根目录和模块目录下的 env 文件，写入环境变量
 
 - ./env
 - ./${MODULE}/env
 

模块内通过实例化 Env 来读取环境变量，支持类型转换，通过内置库`os.environ.get("BKIAM_HOST")`来读取配置也是可以的
```
from environs import Env

env = Env()

BKIAM_HOST = env.str("BKIAM_HOST")
BKIAM_PORT = env.int("BKIAM_PORT")
```

[更多使用方式可参考官方指南](https://pypi.org/project/environs/)

### 2.4【建议】在 settings 中引用环境变量

建议在 ${MODULE}/pizza_settings.py 中引用环境变量，使得配置更为集中地管理。项目的代码需要读取主要配置都通过 django 框架提供的配置
方式来获取，举个例子

```python
# auth/pizza_settings.py

from environs import Env

env = Env()
DATAHUB_API_HOST = env.str("DATAHUB_API_HOST")
DATAHUB_API_PORT = env.int("DATAHUB_API_PORT")
DATABUS_API_URL = f"http://{DATAHUB_API_HOST}:{DATAHUB_API_PORT}/v3/databus/"


# auth/api/databus.py
from django.conf import settings
from common.api.base import DataAPI

class _DatabusApi:
    def __init__(self):
        self.add_queue_user = DataAPI(
            url=settings.DATABUS_API_URL + "/queue_users/",
            method="POST",
            module="databus",
            description="插入队列服务用户",
        )
```

在 pizza 框架中，`${MODULE}/pizza_settings.py` 会自动加载进 `django.settings` 模块中，所有配置可以统一在此处维护，项目中其他代码
需要使用配置时，通过 `from django.conf import settings`加载配置，
[更多使用方式，请查看 django-settings 文档](https://docs.djangoproject.com/en/2.2/topics/settings/)

### 2.5 PIZZA 框架本身提供的配置信息

`settings.py` 除了 Django 框架要求的配置，还有项目自身特性的配置。以下展示的配置均为项目自身特有的，均有默认值，子模块可直接使用，也可
通过 env 或者 pizza_settings 进行重载覆盖

#### 2.5.1 部署信息
```
- APP_NAME              部署模块，auth/meta/...
- RUN_VERSION           运行版本，tencent/oss/ee/...
- RUN_MODE              运行环境，PRODUCT/DEVELOP/LOCAL/...
```

#### 2.5.2 应用信息
```
- APP_ID                应用代码，基础平台统一代码 bk_bkdata
- APP_TOKEN             应用密钥，基础平台统一密钥，调用 ESB 接口时需要传入
```

#### 2.5.3 平台加解密密钥
```
- CRYPT_INSTANCE_KEY
- CRYPT_ROOT_KEY
- CRYPT_ROOT_IV
```

#### 2.5.4 平台基础库，即 DATAHUB 库
```
- CONFIG_DB_USER
- CONFIG_DB_PASSWORD
- CONFIG_DB_HOST
- CONFIG_DB_PORT
```

#### 2.5.5 日志记录方式

```
- LOGGER_LEVEL         日志记录，默认 INFO
- LOG_MAX_BYTES        单文件最大容量，默认 524288000
- LOG_BACKUP_COUNT     日志备份数，默认 5
```

#### 2.5.6 通用请求地址

```
- AUTH_API_URL       权限模块服务地址路径
- META_API_URL       元数据模块模块服务地址路径
- ESBV2_API_URL      ESBV2服务地址路径
```

#### 2.5.7 TRACING 配置
```
- DO_TRACE                 是否开启 TRACE 记录功能，默认关闭
- TRACING_SAMPLE_RATE      Trace 采样率，默认 1
```