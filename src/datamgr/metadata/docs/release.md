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

## V2.2.1
- 【元数据】元模型变更(AIOps v1.3.2)
    - 更新:
      `sample_result_table`增加`protocol_version`字段
      `experiment_instance`增加`protocol_version`字段
      `dataflow_info`增加`flow_tag`字段 
  【修复】删除元信息时对typed字段进行兼容处理   
      
## V2.2.0
- 【功能】元数据事件系统解耦数据足迹

## V2.1.3
- 【功能】支持BATCH SYNC同步元数据功能
    - 单批次操作节点数目超过50个，自动开启batch_sync模式
    - batch再保持用户提交动作顺序不变的情况下，尽可能聚合同类型节点操作进行batch提交
    - 提交逻辑和upsert保持一致

## V2.1.2
- 【元数据】AIOps V1.2元模型变更
    - 接入:
      `Visualization`,`VisualizationComponent`,`VisualizationComponentRelation`,`TargetVisualizationRelation`
    - 更新: 
      `Algorithm`增加`scene_name`字段,
      `ModelInfo`增加`modeling_type`、`sample_type`字段,
      `SampleSet`增加`modeling_type`字段,
      `AggregationMethod`增加`properties`字段

## V2.1.1
- 【元数据】数据模型元数据更新
- 【元数据】Auth接入项目-数据关联信息元数据

## V2.1.0
- 【功能】DatabusChannelClusterConfig 增加 storage_name 字段
- 【功能】AIOps V1.2 元数据变更

## V2.0.9
- 【功能】上线元数据事件系统
- 【功能】上线元数据事件系统的数据足迹应用

## V2.0.1
- 【优化】Dgraph升级V1.2.3

## V2.0.0
- 【功能】高可用策略
    - 主备集群分流
    - 查询QPS监控
    - 服务异常自动恢复策略
- 【功能】TDW数据表接入平台
- 【接入】元数据接入
    - Flow元数据接入
    - AIOps元数据接入

## V1.0.6
- 【功能】对外功能
    - 数据谱系接口
    - 性能&可靠性增强
    - 国际化支持

## V1.0.5
- 【优化】对外功能
    - 提供完整的MySQL后端错误信息支持

## V1.0.4
- 【功能】类型系统
    - 支持元数据建模
    - 提供自定义元数据校验 
- 【功能】元数据服务
    - 通过Bridge采集同步元数据。
    - 同步流程提供事务性。
    - 对外提供元数据/数据血缘查询&录入功能。
    - 通过MetaRPC/SDK对外提供服务。
