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
## v2.2.1
- 【修复】request 返回 Response.content 在 PY3 下是 bytes 对象不可 encode

## v2.2.0
- 【更新】升级 Python3 + django2.2
- 【更新】增添 result_table.delete、result_table.update 管理
- 【修复】项目获取有权限的数据源列表维持扁平结构

## v2.1.0
- 【功能】同步项目申请结果数据、原始数据的关系表
- 【功能】第三方单据对接 ITSM

## v2.0.0
- 【功能】对接蓝鲸权限中心

## v1.4.0
- 【功能】区分敏感度，对平台数据集使用不同权限控制模型进行访问控制，包括数据可见范围和授权流程

## v1.3.3
- 【功能】通用单据设计，支持《接入TDM数据审批》单据

## v1.3.2
- 【功能】支持TDW鉴权
- 【优化】鉴权接口支持 display_detail 参数，返回鉴权详情
- 【优化】增添离线管理员，审批离线补算的功能

## v1.3.1
- 【功能】增添 DataAdmin 管理系统的角色管理
- 【优化】队列服务授权改为非阻塞调用方式
- 【优化】UDF函数对象对接新的DB表


## v1.3.0
- 【功能】增添原始数据的数据观察员
- 【功能】支持TDM管理员加入到TDM数据的审批流程
- 【功能】 支持TDM管理员查看TDM数据

## v1.2.1
- 【功能】添加区域标签功能

## v1.2.0
- 【功能】支持 UDF 自定义函数对象的权限控制
- 【修复】表更角色成员接口加上权限控制，取消单据生成逻辑

## v1.1.2
- 【功能】支持离线补算单据

## v1.1.1
- 【修复】修复多单据审批后的授权问题

## v1.1.0
- 【功能】增添任务开发员
- 【功能】增添图表观察员
- 【功能】国际化
- 【功能】队列服务按照项目维度自动授权

## v1.0.6
- 【修复】企业版celery启动问题修复

## v1.0.5
- 【优化】企业版部署 MySQL 文件命名规范调整

## v1.0.4
- 【功能】提供TDW相关API
- 【功能】周期性同步TDW结果表权限
- 【功能】提供原始数据角色获取及更新接口
- 【修复】修复数据源节点业务拉取不全的问题

## v1.0.3
- 【功能】修复授权码用途描述为空

## v1.0.2
- 【功能】修复拉取用户的可查询结果表列表，业务不全的问题
- 【功能】兼容权限角色成员保存时为null的情况

## v1.0.1
-【功能】新增 on_migrate 文件，自动创建 public 目录

## v1.0.0
- 【功能】基于角色的对象访问控制
- 【功能】角色管理，支持超级管理员、平台用户、业务负责人、项目管理员、数据开发员、数据管理员、数据清洗员、数据观察员
- 【功能】功能管理，支持数据接入、数据清洗、项目管理、数据开发、数据查询
- 【功能】用户角色关系管理
- 【功能】授权码模式
- 【功能】单据审批中心
- 【功能】支持数据订阅的权限控制，同步kafka集群权限
