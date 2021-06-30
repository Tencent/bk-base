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


![蓝鲸计算平台.png](./docs/resource/img/logo_zh.png)
---
[![license](https://img.shields.io/badge/license-mit-brightgreen.svg?style=flat)](LICENSE.txt)
[![Release Version](https://img.shields.io/badge/release-2.2.0-brightgreen.svg)](docs/release.md)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

数据管理元数据服务模块（MetaData），提供支撑整个平台的异构元数据采集、存储、分析、查询等功能。

## Overview
* [架构设计](docs/overview/architecture.md)
* [代码目录](docs/overview/code_framework.md)

## Features
* 采集: 提供sync_hook元数据钩子同步，和独立的元数据写入接口，快速收集元数据
* 存储: 多后端存储(Mysql、Dgraph)，适配不同需求、不同类型的元数据存储需求
* 分析: 构建平台图谱，利用类型系统和图查询分析挖掘平台的数据资产、成本、血缘等核心关系
* 查询: 支持ERP和原生存储查询语言，对元数据进行多维度、可下钻的深度关联查询

## Getting started
* [本地开发测试](docs/overview/develop.md)
* [正式环境部署](docs/overview/deploy.md)

## Roadmap
* [版本日志](docs/release.md)

## License
项目基于 MIT 协议， 详细请参考 [LICENSE](LICENSE.txt) 。

## Others
### 翻译生成步骤
1.采集需要支持多语言的文本

`pybabel extract -F babel.cfg -o messages.pot .`

2.增加某种翻译语言（可选）

`pybabel init -i messages.pot -d metadata_contents/translations -l de`

3.更新已有翻译语言文件，并进行翻译

`pybabel update -i messages.pot -d metadata_contents/translations`

4.编译成可用翻译

`pybabel compile -d metadata_contents/translations`