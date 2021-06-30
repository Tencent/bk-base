## 数据模型代码结构说明


```
- application    # 数据模型应用管理模块
    - instances  # 封装对数据模型和指标应用实例的管理接口
    - jobs       # 管理所有通过数据模型和指标配置生成的 DataFlow 任务

- dmm          # DataModelManager 数据模型配置管理模块
    - manager  # 屏蔽底层存储细节，对外提供有关模型、指标的操作接口

- handlers      # 在业务逻辑上可以服用的模块可以在 handlers 层进行复用，比如值约束校验器，节点配置规则解析
    - verifier  # 计算 SQL 校验器

- models           # ORM 建模
    - datamodel    # 模型构建阶段 ORM 建模
    - application  # 模型引用阶段 ORM 建模

- selializers  # 接口层校验器

- views                            # 接口层
    - application_indicator_views  # 数据模型应用中关于指标接口
    - application_model_views      # 数据模型应用中关于模型接口
    - dmm_indicator_views          # 数据模型管理中关于指标和统计口径的接口
    — dmm_model_views              # 数据模型管理中关于模型接口

- utils  # 模块内工具函数汇总模块
```