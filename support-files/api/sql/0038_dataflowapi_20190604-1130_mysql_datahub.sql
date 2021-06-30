/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

USE bkdata_basic;

SET NAMES utf8;

INSERT IGNORE INTO `content_language_config` (`content_key`, `language`, `content_value`, `active`, `description`) VALUES
('Source','zh-cn','数据源',1,'dataflow'),
('Processing','zh-cn','数据处理',1,'dataflow'),
('Sink','zh-cn','数据存储',1,'dataflow'),

('Stream Source','zh-cn','实时数据源',1,'dataflow'),
('Batch Source','zh-cn','离线数据源',1,'dataflow'),
('KV Source','zh-cn','关联数据源',1,'dataflow'),
('Stream Processing','zh-cn','实时计算',1,'dataflow'),
('Batch Processing','zh-cn','离线计算',1,'dataflow'),
('Druid','zh-cn','Druid',1,'dataflow'),
('ElasticSearch','zh-cn','ElasticSearch',1,'dataflow'),
('HDFS','zh-cn','HDFS',1,'dataflow'),
('Hermes','zh-cn','Hermes',1,'dataflow'),
('MySQL','zh-cn','MySQL',1,'dataflow'),
('Queue','zh-cn','Queue',1,'dataflow'),
('Merge','zh-cn','合流计算',1,'dataflow'),
('Algorithm Model','zh-cn','算法模型',1,'dataflow'),
('Split','zh-cn','分流计算',1,'dataflow'),
('Tredis','zh-cn','Tredis',1,'dataflow'),
('TSDB','zh-cn','TSDB',1,'dataflow'),
('Tspider','zh-cn','Tspider',1,'dataflow'),
('TDW Source','zh-cn','TDW数据源',1,'dataflow'),
('TDW Storage','zh-cn','TDW存储',1,'dataflow'),
('TDW Batch Processing','zh-cn','TDW离线计算',1,'dataflow'),
('TDW-JAR Batch Processing','zh-cn','TDW-JAR离线计算',1,'dataflow'),
('Anomaly Detection','zh-cn','异常检测',1,'dataflow'),

('Full-text retrieval based on Elasticsearch log','zh-cn','基于Elasticsearch日志全文检索',1,'dataflow'),
('Historical data storage based on HDFS cannot be directly queried and it can only be connected offline','zh-cn','基于HDFS的历史数据存储，不可以直接查询，只能连接离线',1,'dataflow'),
('Relational database storage based on MySQL','zh-cn','基于MySQL的关系型数据库存储',1,'dataflow'),
('Support massive data aggregation analysis','zh-cn','支持海量数据聚合分析',1,'dataflow'),
('Mysql-based distributed relational database storage','zh-cn','基于Mysql分布式关系型数据库存储',1,'dataflow'),
('Kafka-based message queuing service supports real-time subscription of data','zh-cn','基于Kafka的消息队列服务，支持实时订阅数据',1,'dataflow'),
('Support massive data aggregation analysis','zh-cn','支持海量数据聚合分析',1,'dataflow'),
('KV-based database storage','zh-cn','基于KV型的数据库存储',1,'dataflow'),
('InfluxDB based time series database','zh-cn','基于InfluxDB的时间序列数据库',1,'dataflow'),
('TDW storage','zh-cn','TDW存储',1,'dataflow'),

('Result data of cleaning and real-time calculation has low delay, and can be used for real-time statistical calculations in minutes','zh-cn','清洗、实时计算的结果数据，数据延迟低，可用于分钟级别的实时统计计算',1,'dataflow'),
('Data that rolls out to the distributed file system can be used for offline statistical calculations in hours or above','zh-cn','落地到分布式文件系统的数据，可用于小时级以上的离线统计计算',1,'dataflow'),
('Status data stored in K-V can be used as dimension table and real-time data join','zh-cn','在K-V存储的状态数据，可用作维度表与实时数据join',1,'dataflow'),
('TDW source','zh-cn','TDW数据源',1,'dataflow'),

('Real-time calculation based on streaming processing, supporting calculations in seconds and minutes','zh-cn','基于流式处理的实时计算,支持秒级和分钟级的计算',1,'dataflow'),
('Offline calculation based on batch processing, supporting calculations in hours and days','zh-cn','基于批处理的离线计算，支持小时级和天级的计算',1,'dataflow'),
('Support multiple data source nodes with the same ResultTable schema to be merged into one ResultTable','zh-cn','支持表结构相同的多个数据源节点合并成为一个结果数据表',1,'dataflow'),
('Support a data source to be divided into several result tables with the same ResultTable schema according to the business dimension','zh-cn','支持一个数据源根据业务维度切分成若干数据结构相同的结果数据表',1,'dataflow'),
('Apply the published model version on modelflow to dataflow','zh-cn','将modelflow上已发布的模型版本在dataflow上应用',1,'dataflow'),
('TDW offline calculations','zh-cn','TDW离线计算',1,'dataflow'),
('TDW-JAR offline calculations','zh-cn','TDW-JAR离线计算',1,'dataflow'),
('MachineLearning-based model nodes for anomaly detection of time series data','zh-cn','基于机器学习的模型节点，对时间序列类型的数据进行异常检测',1,'dataflow');
