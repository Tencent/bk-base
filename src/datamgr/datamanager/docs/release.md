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
## 2.2.2
- 【功能】数据监控任务迁移
- 【功能】监控元数据同步任务

## 2.2.1
- 【功能】bk usermanager 用户信息上报和数据模型
- 【功能】CMDB biz 业务信息上报
- 【更新】cmdb module/set/biz 数据模型支持初始化

## 2.2.0
- 【功能】CMDB set/module 信息上报和对账
- 【更新】cmdb host/module/set 事件订阅支持动态获取所有字段
- 【功能】数据管理后台订阅&处理数据足迹事件&上报es

## 2.1.1
- 【功能】CMDB 主机数据模型支持初始化
- 【功能】升级 dm_engine 依赖，上报 promethues 指标
- 【优化】添加 pre-commit、flake8、单元测试、CI 检查

## 2.1.0
- 【功能】支持 CMDB 主机和主机关系数据合并上报
- 【功能】支持 CDMB 主机数据对账，并上报指标至内部 influxdb

## v2.0.0

- 【功能】支持权限任务
- 【功能】支持审计任务
- 【功能】支持数据质量任务
- 【功能】支持生命周期任务
- 【功能】支持元数据任务
