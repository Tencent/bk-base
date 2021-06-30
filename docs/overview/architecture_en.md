# BK-BASE Architecture

![](../resource/img/system_arch.png)

The overall platform architecture is divided into three layers:

- Open source component layer: supports a variety of open source and Tencent self-developed big data components

> 1. Computing engine: Flink and Spark, etc.
> 2. Storage system: relational MySQL, ElasticSearch for log retrieval, KV storage Redis, distributed cache Ignite and distributed file system HDFS, etc.

- Intermediate service layer:

> 1. Data integration service (Datahub), mainly composed of data bus with Kafka message queue as the core, including data pull, real-time cleaning, data distribution, queue service, data arrangement, integrated management functions, it is connected like a bus Data processing and storage query.
> 2. Data Development Service (DataFlow), which hides the complexity of various computing frameworks to form a unified computing service. Based on SQL and graphical drag-and-drop, real-time stream processing and offline batch processing are combined, and unified computing can be used to perform real-time, offline, and offline data processing. Complex data processing such as aggregation and association.
> 3. Data Exploration Service (DataLab), the query engine supports a variety of storage systems, including relational database MySQL, offline storage HDFS, full-text search Elasticsearch, time series database TSDB and KV storage Redis, etc. The latest version at this stage supports ANSI SQL and There are two types of query methods for text retrieval.
> 4. Data Management (DataMgr), providing services such as metadata management, data quality monitoring, data security, data map, data life cycle, data dictionary, etc.

- API service layer: Provide API services corresponding to the middle service layer, including data query, data integration, data development, metadata services, etc., and provide interface calling services for upper-level applications.

In addition, on the right are the Blueking products that the platform depends on, including [BK-PaaS](https://github.com/Tencent/bk-PaaS), [BK-CMDB](https://github.com/Tencent/bk-cmdb), BK-JOB, [BK-BCS](https://github.com/Tencent/bk-bcs), BK-IAM, in [Blueking official website](https://bk.tencent.com/download/) you can find the introduction of the corresponding product.

