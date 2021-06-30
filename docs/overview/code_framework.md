# 蓝鲸基础计算平台(BK-BASE)的代码结构

```
bk-base/
├── docs
├── scripts
├── src
│   ├── api
│   ├── dataflow
│   ├── datahub
│   ├── datalab
│   ├── datamgr
│   └── dataweb
└── support-files
```

## 工程源码(src)

工程是混合了 Java/Python/Javascript/Vue/Scala/Shell 等几种语言，按功能模块分 API 服务、数据开发服务、数据集成服务、数据探索服务、数据管理服务、平台 web 服务等工程

### API 服务(api)

```
|- bk-base/src
  |- api
    |- auth             # 平台权限管理API，包括数据权限，项目权限，数据管理员等
    |- codecheck        # code检查服务API，code节点中代码片段的检查，防止危险代码注入
    |- dataflow         # 数据开发服务API，包括计算任务创建，启动，节点配置等操作，以及节点之间连接规则等
    |- datahub          # 数据集成服务API，包括数据源接入，清洗和入库的操作管理等
    |- datalab          # 数据探索服务API，包括数据查询，分析笔记的创建，删除等维护操作
    |- datamanage       # 数据管理服务API，包括数据质量监控，数据地图和数据字典等
    |- jobnavi          # 任务调度服务API，包括任务提交，启动，编排和依赖关系等
    |- meta             # 元数据服务API，包括数据元数据，数据血缘，数据结果表血缘关系等
    |- pizza            # 平台API统一开发框架，Python2.x版本
    |- resourcecenter   # 资源管理服务API，包括计算资源，存储资源，总线资源等资源的申请，集群管理等
    |- upizza           # 平台API统一开发框架，Python3.x版本
```

API 服务是基于 Django 开发的，Pizza 是 API 服务的统一开发框架，基于 Pizza 可以快速构建平台模块的 API 服务。



### 数据开发服务(dataflow)
```
|- bk-base/src
  |- dataflow
    |- bksql                      # 数据开发计算逻辑SQL配置
      |- bksql-extend             # 提供一些工具类的SQL服务，比如获取SQL中的函数，获取SQL中的维度字段
      |- bksql-plugin-flinksql    # BKSQL适配FlinkSQL
      |- bksql-plugin-mlsql       # BKSQL支持MLSQL（平台自研的用于机器学习SQL语法）
      |- bksql-plugin-sparksql    # BKSQL适配SparkSQL
      |- distribution             # BKSQL打包的依赖lib目录和启动脚本的bin目录等
    |- codecheck           # 数据开发Code节点代码安全检查
    |- jobnavi             # 任务调度编排服务 
       |- jobnavi-api                       # 适配层内部协议API（Java）
       |- jobnavi-api-python                # 适配层内部协议API（Python）
       |- jobnavi-cmd-adaptor               # 命令行任务适配层
       |- jobnavi-common                    # 公共依赖模块
       |- jobnavi-flink-adaptor             # Flink任务适配层
       |- jobnavi-kubernetes-adaptor        # Kubernetes任务适配层
       |- jobnavi-log-aggregation           # 日志归档模块
       |- jobnavi-rpc                       # 内部RPC通信协议模块
       |- jobnavi-runner                    # 任务执行器模块
       |- jobnavi-scheduler                 # 调度器模块
       |- jobnavi-sparksql-adaptor          # SparkSQL离线任务适配层
       |- jobnavi-sparkstreaming-adaptor    # Spark Struct Streaming实时任务适配层
       |- jobnavi-tensorflow-adaptor        # Tensorflow任务适配层 
       |- jobnavi-workflow                  # 工作流任务适配层
    |- ucapi-service                 # Jobnavi适配层，用于多种类型任务轮训启动用
    |- unified-computing             # 统一计算层
       |- batch        # 离线计算相关功能，包含交互式SQL，周期性离线SQL等
       |- core         # 公共依赖功能
       |- metrics      # 数据流埋点指标
       |- one-code     # Code节点父类
       |- one-model    # 基于Spark MLlib实现的MLSQL后台逻辑
       |- python       # 统一计算层Python实现
       |- server       # 统一计算层的统一入口
       |- stream       # 实时计算相关功能，包含FlinkSQL，Flink Code
       |- udf          # 自定义函数功能实现
    |- yarn-service    # 获取Yarn上面所有Stream任务的Application信息，以及获取Yarn集群状态
      
```
DataFlow 一站式拖拽式在线 IDE，屏蔽底层多种计算引擎的复杂度，抽成开发门槛较低的 SQL 语法，同时支持多种不同计算任务的组合编排。


### 数据集成服务(datahub)
```
|- bk-base/src
  |- datahub
    |- cache                # 分布式缓存服务
       |- base              # Cache基本功能，包括创建，删除，读写等
       |- ignite-plugin     # 集群鉴权插件，包含节点鉴权，读写操作等鉴权
       |- spark             # Spark查询Cache数据
    |- databus                        # 数据总线服务
       |- databus-clean               # 总线清洗任务
       |- databus-commons             # 总线的公共功能（比如打点，工具类等等）
       |- databus-connect-common      # 基于Kakfa Connect封装的功能类
       |- databus-pipe-extractor      # 清洗核心逻辑
       |- databus-puller-bkhdfs       # 离线数据接入场景的Puller
       |- databus-puller-datanode     # 分流合流节点任务
       |- databus-puller-http         # HTTP接入场景的Puller
       |- databus-puller-jdbc         # 数据库接入场景的Puller
       |- databus-puller-kafka        # Kafka消息队列接入场景的Puller
       |- databus-shipper-clickhouse  # 分发数据到ClickHouse的总线任务
       |- databus-shipper-datalake    # 分发数据到数据湖的总线任务
       |- databus-shipper-druid       # 分发数据到Druid的总线任务
       |- databus-shipper-es          # 分发数据到ES的总线任务
       |- databus-shipper-hdfs        # 分发数据到HDFS的总线任务
       |- databus-shipper-ignite      # 分发数据到Ignite的总线任务
       |- databus-shipper-jdbc        # 分发数据到数据库的总线任务
       |- databus-shipper-queue       # 分发数据到队列(Kafka)的总线任务
       |- databus-shipper-redis       # 分发数据到redis 的总线任务
       |- distribution                # 项目编译构建成可执行安装包的工具
    |- datalake    # 基于Iceberg的数据湖
    |- hubmgr      # 集成任务管理服务
   
```

DataHub 集成多种数据源类似接入，支持轻量级实时数据清洗，支持多种数据存储分发。


### 数据探索服务(datalab)
```
|- bk-base/src
  |- datalab
    |- bksql                 # 平台统一BKSQL服务
       |- bksql-core         # 核心功能模块
       |- bksql-exec         # 扩展功能模块
       |- bksql-metadata     # BKSQL元数据解析模块
       |- meta-client        # 平台元数据接口工具包
    |- notebook              # 数据探索笔记服务
       |- notebook-k8s       # 笔记镜像相关的构建代码
          |- dockerfiles     # 笔记镜像构建文件
             |- jupyter      # Jupyter Notebook 构建的dockerfile文件
             |- jupyterhub   # Jupyterhub构建的dockerfile文件
          |- notebooks       # 笔记模板(笔记快速入门)
    |- query                             # 数据查询服务
      |- queryengine                     # 查询引擎
          |- queryengine-clickhouse      # ClickHouse查询语句转换优化模块
          |- queryengine-common          # 通用工具库模块
          |- queryengine-datalake        # 数据湖SQL DML操作转换优化模块
          |- queryengine-distribution    # 打包构建模块
          |- queryengine-druid           # Druid查询语句转换优化模块
          |- queryengine-elasticsearch   # Elasticsearch查询转换优化模块
          |- queryengine-federation      # 联邦查询转换优化模块
          |- queryengine-ignite          # Ignite查询语句转换优化模块
          |- queryengine-main            # 核心服务模块
          |- queryengine-onesql          # OneSQL查询语句转换优化模块
          |- queryengine-postgresql      # Postgresql查询语句转换优化模块
          |- queryengine-presto          # Presto查询语句转换优化模块

```
DataLab 交互式探索和分析入口，支持简单快速的 SQL 查询工具，具有简易的数据可视化能力，提供 Notebook 分析挖掘功能。


### 数据管理服务(datamgr)
```
|- bk-base/src
  |- datamgr
    |- datamanager   # 数据管理任务模块，管理元数据、数据安全、数据质量、生命周期、公共数据等任务
    |- dm_engine     # 数据管理任务引擎，支持分布式任务执行
    |- metadata      # 元数据服务，提供异构元数据采集、存储、分析、查询等功能
    |- metasdk       # 元数据操作SDK，提供元数据变更hook抓取、元数据事件订阅等功能

```

Datamgr 具有简易的数据资产管理，基于 Dgraph 构建的元数据血缘管理，提供数据质量监控和数据安全管理等功能。


### 平台 web 服务(dataweb)
```
|- bk-base/src
  |- dataweb
    |- app       # SaaS源码
    |- blueapps  # 蓝鲸SaaS开发框架
    |- web       # 前端源码，包含UI，JavaScript源码，打包配置
      
```
基础平台前端 SaaS 服务


## 配置文件模板(support-files)

```
|- bk-base/support-files
  |- metadata       # 在部署初始化时需要的元数据
  |- sql            # sql初始化脚本，在开发编译时就需要先初始化
  |- template       # 所有需要做替换部署配置/脚本文件
```

## 安装脚本(scripts)

```
|- bk-base
  |- scripts
    |- build     # 编译脚本和工具
    |- intsall   # 安装脚本和工具 
```

存放一些自动化安装替换脚本，待补充。