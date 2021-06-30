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

## V2.2.4
- 【优化】RT设定字段数量限制

## V2.2.3
- 【优化】升级到Python3.6
- 【优化】请求链路上的一些优化
- 【优化】配置文件的优化

## V2.2.0
- 【功能】数据管理事件上报
  - 增加事件上报接口
  - 增加meta-sdk支持配置拉取接口

## V2.1.0

- 【文档】项目文档补全

## V2.0.5
- 【功能】TDW表访问
  - 访问我的TDW表
  - 访问具体的TDW表

## V2.0.3
- 【功能】token分流功能
  - 主备集群访问token
  - 分场景token参数

## V2.0.0
- 【功能】查询后端切换Dgraph
  
## V1.0.3
- 【功能】Basic API
  - 确保高可用模式启动时有时间同步ZK信息。
  - 增强RPC错误处理和提示。

## V1.0.2
- 【优化】 Advanced API
  - 结果表相关接口支持大写的结果表名称

## V1.0.0 初始版本
- 【功能】 Advanced API
  - 结果表接口
  - 结果表关联信息接口
  - 数据处理接口
  - 数据传输接口
  - 血缘接口
  - 操作事务接口
  - 同步事务接口
- 【功能】Basic API
  - 实体接口
  - atlas后端专用接口
  