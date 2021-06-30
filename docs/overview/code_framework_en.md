# Framework of BK-BASE

```
bk-base/
├── docs
├── scripts
├── src
│ ├── api
│ ├── dataflow
│ ├── datahub
│ ├── datalab
│ ├── datamgr
│ └── dataweb
└── support-files
```

## Project source code (src)

The project is a mixture of Java/Python/Javascript/Vue/Scala/Shell and other languages, and is divided into API services, data development services, data integration services, data exploration services, data management services, platform web services and other projects according to functional modules.

### API Service (api)

```
|- bk-base/src
  |- api
    |- auth # Platform authority management API, including data authority, project authority, data administrator, etc.
    |- codecheck # code check service API, check code snippets in code nodes to prevent dangerous code injection
    |- dataflow # Data development service API, including calculation task creation, startup, node configuration and other operations, as well as connection rules between nodes, etc.
    |- datahub # Data integration service API, including data source access, cleaning and warehousing operation management, etc.
    |- datalab # Data exploration service API, including maintenance operations such as data query, creation and deletion of analysis notes
    |- datamanage # Data management service API, including data quality monitoring, data map and data dictionary, etc.
    |- jobnavi # Task scheduling service API, including task submission, startup, orchestration and dependencies, etc.
    |- meta # Metadata service API, including data metadata, data blood relationship, data result table blood relationship, etc.
    |- pizza # Platform API unified development framework, Python2.x version
    |- resourcecenter # Resource management service API, including application for computing resources, storage resources, bus resources and other resources, cluster management, etc.
    |- upizza # Platform API unified development framework, Python3.x version
```

API services are developed based on Django. Pizza is a unified development framework for API services. Based on Pizza, API services for platform modules can be quickly constructed.



### Data Development Service (dataflow)
```
|- bk-base/src
  |- dataflow
    |- bksql # Data development calculation logic SQL configuration
      |- bksql-extend # Provide some tool-like SQL services, such as obtaining functions in SQL and obtaining dimension fields in SQL
      |- bksql-plugin-flinksql # BKSQL adapts to FlinkSQL
      |- bksql-plugin-mlsql # BKSQL supports MLSQL (self-developed platform for machine learning SQL syntax)
      |- bksql-plugin-sparksql # BKSQL adapts to SparkSQL
      |- distribution # BKSQL packaged dependent lib directory and bin directory of startup script, etc.
    |- codecheck # Data development Code node code security check
    |- jobnavi # Task scheduling orchestration service
       |- jobnavi-api # Adaptation layer internal protocol API (Java)
       |- jobnavi-api-python # Adaptation layer internal protocol API (Python)
       |- jobnavi-cmd-adaptor # Command line task adaptation layer
       |- jobnavi-common # Common dependency module
       |- jobnavi-flink-adaptor # Flink task adaptation layer
       |- jobnavi-kubernetes-adaptor # Kubernetes task adaptation layer
       |- jobnavi-log-aggregation # Log archive module
       |- jobnavi-rpc # Internal RPC communication protocol module
       |- jobnavi-runner # Task executor module
       |- jobnavi-scheduler # Scheduler module
       |- jobnavi-sparksql-adaptor # SparkSQL offline task adaptation layer
       |- jobnavi-sparkstreaming-adaptor # Spark Struct Streaming real-time task adaptation layer
       |- jobnavi-tensorflow-adaptor # Tensorflow task adaptation layer
       |- jobnavi-workflow # Workflow task adaptation layer
    |- ucapi-service # Jobnavi adaptation layer, used for the start of multiple types of task rotation training
    |- unified-computing # Unified computing layer
       |- batch # Offline calculation related functions, including interactive SQL, periodic offline SQL, etc.
       |- core # Public dependency function
       |- metrics # Data flow buried point indicators
       |- one-code # Code node parent class
       |- one-model # MLSQL background logic implemented based on Spark MLlib
       |- python # Unified computing layer Python implementation
       |- server # Unified entrance to the unified computing layer
       |- stream # Real-time calculation related functions, including FlinkSQL, Flink Code
       |- udf # Implementation of custom function function
    |- yarn-service # Get the Application information of all Stream tasks on Yarn and get the Yarn cluster status
      
```
DataFlow's one-stop drag-and-drop online IDE shields the complexity of the underlying multiple computing engines, extracts SQL syntax with a lower development threshold, and supports the combination and arrangement of multiple different computing tasks.


### Data Integration Service (datahub)
```
|- bk-base/src
  |- datahub
    |- cache # Distributed cache service
       |- base # Cache basic functions, including creation, deletion, reading and writing, etc.
       |- ignite-plugin # Cluster authentication plug-in, including node authentication, read and write operations and other authentication
       |- spark # Spark query Cache data
    |- databus # Data bus service
       |- databus-clean # Bus cleaning task
       |- databus-commons # Common functions of the bus (such as management, tools, etc.)
       |- databus-connect-common # Function class based on Kakfa Connect package
       |- databus-pipe-extractor # Clean the core logic
       |- databus-puller-bkhdfs # Puller for offline data access scenarios
       |- databus-puller-datanode # Split and merge node task
       |- databus-puller-http # Puller for HTTP access scenarios
       |- databus-puller-jdbc # Puller for database access scenarios
       |- databus-puller-kafka # Puller for Kafka message queue access scenario
       |- databus-shipper-clickhouse # The bus task of distributing data to ClickHouse
       |- databus-shipper-datalake # The bus task of distributing data to the data lake
       |- databus-shipper-druid # The bus task of distributing data to Druid
       |- databus-shipper-es # The bus task of distributing data to ES
       |- databus-shipper-hdfs # The bus task of distributing data to HDFS
       |- databus-shipper-ignite # The bus task of distributing data to Ignite
       |- databus-shipper-jdbc # The bus task of distributing data to the database
       |- databus-shipper-queue # The bus task of distributing data to the queue (Kafka)
       |- databus-shipper-redis # The bus task of distributing data to redis
       |- distribution # A tool to compile the project into an executable installation package
    |- datalake # Data lake based on Iceberg
    |- hubmgr # Integrated task management service
   
```

DataHub integrates similar access to multiple data sources, supports lightweight real-time data cleaning, and supports multiple data storage and distribution.


### Data Exploration Service (datalab)
```
|- bk-base/src
  |- datalab
    |- bksql # Platform unified BKSQL service
       |- bksql-core # Core function module
       |- bksql-exec # Extended function module
       |- bksql-metadata # BKSQL metadata analysis module
       |- meta-client # Platform Metadata Interface Toolkit
    |- notebook # Data exploration note-taking service
       |- notebook-k8s # Note mirror related build code
          |- dockerfiles # Note image build file
             |- jupyter # dockerfile built by Jupyter Notebook
             |- jupyterhub # dockerfile built by Jupyterhub