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
[![Release Version](https://img.shields.io/badge/release-2.0.0-brightgreen.svg)](./docs/release.md)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

基于 Django+DRF 开发框架进行改造，提供统一的 API 框架，各个子模块服务按照规范进行接口开发，再以 git-submodule 的方式注册进 PIZZA 仓库。

## Features
* 统一用户认证中间件
* 统一请求日志记录中间件
* 提供用户鉴权、元数据同步、第三方请求模块等公共组件
* 规范化 APIDOC、APIURL、APIViewSet

## Develop
* [如何在本地开发 API 模块](docs/api/develop.md)
* [如何生成 API 文档](docs/api/apidoc.md)
* [需要遵循的 API 标准](docs/api/stardard.md)
* [如何添加鉴权逻辑](docs/api/authentication.md)
* [如何申明 API 依赖的环境配置](docs/api/env.md)


## Pizza Develop
* [如何开发 PIZZA 框架](docs/overview/develop.md)


## Build

编译打包对应 API 模块，编译成功后，在工程目录的 dist 会有包文件
```
make build MODULE=auth RUN_VERSION=oss BUILD_NO=111
```

## Roadmap
* [版本日志](docs/release.md)

