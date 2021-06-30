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
# 数据采集模块

采集模块主要负责采集平台运营数据和第三方数据等公共数据


## 1. 目前采集的数据

### 1.1 cmdb

- host
- host_relation
- module
- set

## 1.2 opdata

- ...


## 2. 上报的数据结构

在 `collection/common/message.py` 已经定义了统一的上报数据结构，所有上报信息都需要按照规范进行上报。如果使用统一的 BKDRawDataCollector 基类，可使用内置的 produce_message 上报消息。

```python
class QueueMessage(object):
    event_content = attr.ib(type=dict)
    collect_time = attr.ib(type=int)
    event_type = attr.ib(type=str, default=EventType.UPDATE.value)
    collect_method = attr.ib(type=str, default=CollectMethod.BATCH.value)
```


## 3. 初始化上报任务

`collection/common/collect` 模块定义了采集基类 BaseCollector，所有采集任务都需要继承基类，重载 report 方法。如果是上报至平台的数据源，可以继承 BKDRawDataCollector 基类进行上报，内部已经封装了必要的上报方法。

```python
from collection.common.collect import BKDRawDataCollector

class CMDBTopoCollector(BKDRawDataCollector):

    def report(self):

        # 从 CMDB 获取到 TOPO 信息
        messages = []
        for m in messages:
            self.produce_message(m)


def collect_cmdb_topo():
    collect_cmdb_topo_config = {
        'raw_data_name': 'dm_cmdb_inst_topo'
    }
    CMDBTopoCollector(collect_cmdb_topo_config).report()


# 注册 dm_engine 任务
entry.add_command(click.command('collect-cmdb-topo')(dm_task(collect_cmdb_topo)))
```

## 4. 初始化处理任务

`collection/common/process` 模块定义了采集基类 BaseProcessor，所有处理任务都需要继承基类，重载 build 方法，这里主要是构建整个数据处理流程。

以数据平台数据流处理流程为例，提供 BKDFlowProcessor 做为实现方式，支持多种处理节点的初始化，包括有

```
- 1. 数据接入节点
- 2. 数据清洗节点
- 3. 数据计算节点
        - 3.1 DataFlow 计算节点
        - 3.2 数据模型计算节点
```

### 4.1 这里需要重点了解几个基本概念

#### 4.1.1 ProcessTemplate

```python
class AccessCustomTemplate(ProcessTemplate):
    template = 'common_custom_access.jinja'

    def get_deploy_plan(self):
        return self.content

# 使用方式
params_template = AccessCustomTemplate({'raw_data_name': 'xxx'})
```

参数模板提供处理节点依赖的参数，通过调用模板接口获取参数，比如 `AccessCustomTemplate(ProcessTemplate)` 提供了 get_bk_biz_id、get_deploy_plan 可获取到业务ID和接入配置。

参数模板的内部实现方式，是通过定义 cls.template jinja 模板路径，初步确定模板内容，在应用时，传入 context 上下文，替换占位符变量，输出最终的的参数内容。


#### 4.1.2 ProcessNode

处理节点实现节点的初始化工作，比如 `AccessNode(ProcessNode)` 实现数据接入配置的初始化，生成数据源，需要结合 self.params_template 来获取参数

```python
class AccessNode(ProcessNode):
    """
    数据接入节点
    """

    def build(self):
        deploy_plan = self.params_template.get_deploy_plan()

        response = access_api.deploy_plans.create(deploy_plan, raise_exception=True)
        return {'raw_data_id': response.data['raw_data_id']}

# 使用方式
params_template = AccessCustomTemplate({'raw_data_name': 'xxx'})
node = AccessNode(params_template)
node.build()
```


#### 4.1.3 pipeline

```python

pipeline = [
    {
        'process_node': 'AccessNode',
        'process_template': 'AccessCustomTemplate',
        'process_context': {
            'bk_biz_id': 591,
            'raw_data_name': 'dm_cmdb_inst_topo',
            'raw_data_alias': 'CMDB 业务拓扑结构'
        }
    },
    {
        'process_node': 'CleanNode',
        'process_template': 'CleanCMDBTopoTemplate',
        'process_context': {
            'bk_biz_id': 591,
            'raw_data_id': '$0.raw_data_id',
            'result_table_name': 'dm_cmdb_inst_topo_raw3',
            'result_table_alias': '[公共] CMDB 业务拓扑事实表'
        }
    }
]
BKDFlowProcessor(pipeline).build()
```

pipeline 是 BKDFlowProcessor 的执行配置，BKDFlowProcessor 会按照定义的流程和配置来初始化处理流程。pipeline 每一步定义一个执行步骤，比如第一条记录，则表示初始化《数据接入节点》，其中

- process_node 定义处理节点类
- process_template 定义参数模板
- process_context 定义节点上下文

pipeline 中定义的 process_node、process_template 需在 BKDFlowProcessor 中进行注册，才可使用。通用的 process_node、process_template 默认注册，如果是需要定制化处理的节点和参数模板需要注册。

处理节点注册方式
```
BKDFlowProcessor.regiter_process_node(AccessNode)
```

参数模板注册方式
```
BKDFlowProcessor.regiter_process_template(AccessCustomTemplate)
```

process_context 如果依赖上一节点的输出，可以通过占位符 ${index} 获取第 {index} 个节点的输出结果，再通过点号进一步读取属性，比如 `$0.raw_data_id` 就是读取第 0 个节点输出内容中的 raw_data_id 属性值。


#### 4.1.4 BKDFlowProcessor

数据平台数据流处理器，重载了 build 提供整个流程的初始化入口


### 4.2 完整处理样例

```python
class CleanCMDBTopoTemplate(CleanTemplate):
    template = 'cmdb_topo_clean.jinja'


BKDFlowProcessor.regiter_process_template(CleanCMDBTopoTemplate)


def process_cmdb_topo():

    process_cmdb_topo_config = {
        'pipeline': [
            {
                'process_node': 'AccessNode',
                'process_template': 'AccessCustomTemplate',
                'process_context': {
                    'bk_biz_id': 591,
                    'raw_data_name': 'dm_cmdb_inst_topo',
                    'raw_data_alias': 'CMDB 业务拓扑结构'
                }
            },
            {
                'process_node': 'CleanNode',
                'process_template': 'CleanCMDBTopoTemplate',
                'process_context': {
                    'bk_biz_id': 591,
                    'raw_data_id': '$0.raw_data_id',
                    'result_table_name': 'dm_cmdb_inst_topo_raw3',
                    'result_table_alias': '[公共] CMDB 业务拓扑事实表'
                }
            }
        ]
    }

    BKDFlowProcessor(process_cmdb_topo_config['pipeline']).build()

# 注册 dm_engine 任务
entry.add_command(click.command('process-cmdb-topo')(dm_task(process_cmdb_topo)))
```

