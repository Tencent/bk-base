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
---
[![license](https://img.shields.io/badge/license-mit-brightgreen.svg?style=flat)](LICENSE.txt)
[![Release Version](https://img.shields.io/badge/release-2.2.1-brightgreen.svg)](./docs/release.md)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

AuthAPI 是数据平台权限控制接口，托管全平台对象、对象关系和人员角色关系，并基于角色关系进行对象访问控制。同时支持授权码模式，区分敏感度，对平台数据集使用不同权限控制模型进行访问控制。

## Overview
* [架构设计](docs/overview/architecture.md)
* [代码目录](docs/overview/code_framework.md)


## Features
* 托管全平台对象、对象关系和人员角色关系，并基于角色关系进行对象访问控制
* 支持授权码模式，以授权码作为授权凭证，调用平台的数据访问接口
* 区分敏感度，对平台数据集使用不同权限控制模型进行访问控制

## Getting started
* [本地开发测试](docs/overview/develop.md)
* [正式环境部署](docs/overview/deploy.md)

## Roadmap
* [版本日志](docs/release.md)

## License
项目基于 MIT 协议， 详细请参考 [LICENSE](LICENSE.txt) 。
