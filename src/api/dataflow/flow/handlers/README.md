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

#节点开发流程

## 节点定义
请参照已有的节点来写
>路径
dataflow.flow.settings
dataflow.flow.moodels

英文名：
```
XXXNode
```

中文名：
```
新的节点
```

分类：
```
CALC_CATEGORY = [ANode, BNode, CNode, XXXNode]
```

连线规则：

```
LinkRules = {
    XXXNode: {
        "valid_parent_types": [],
        "has_parent": False,
        "has_more_than_one_parent": False
    }
}
```


## 新建节点类
> 路径
dataflow.flow.handlers.nodes
```
class XXXNode(inheritNode):
    # 必须重载的属性
    node_type = 节点类型
    node_form = forms.节点参数表单
    config_attrs = 节点配置参数

    # 必须重载的方法
    def build_output(self, form_data, from_node_ids):
        To be Implemented
```

把类注册到工厂函数中
```
NODE_FACTORY.add_node_class(FlowNode.NodeTypes.RT_SOURCE, XXXNode)
```
## 新节点的任务启动
  （可选）节点任务
> 路径
dataflow.flow.task.deploy_flow
```
class XXXNodeTask(inheritTask):
    To be Implemented
```
注册任务到FlowTaskHandler
```
M_NODE_TASK = {
    XXXNode: XXXNodeTask,
}
```

## 新节点的任务调试
  （可选）节点调试器
> 路径
dataflow.flow.task.debug_flow
```
class XXXNodeDebugger(inheritDebugger):
    To be Implemented
```
注册调试器到FlowDebuggerHandler
```
M_NODE_TASK = {
    XXXNode: XXXNodeDebugger,
}
```

## 新节点打点监控
> 路径
dataflow.flow.handler.monitor

## 提供保存节点可能需要用到的接口
（比如：结果表列表、清洗表列表）
