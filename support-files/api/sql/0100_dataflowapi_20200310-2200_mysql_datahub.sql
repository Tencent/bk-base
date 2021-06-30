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
('数据源','en','Source',1,'dataflow'),
('数据处理','en','Processing',1,'dataflow'),
('数据存储','en','Sink',1,'dataflow'),
('机器学习','en','Modeling',1,'dataflow'),

('实时数据源','en','Stream Source',1,'dataflow'),
('离线数据源','en','Batch Source',1,'dataflow'),
('实时关联数据源','en','Stream KV Source',1,'dataflow'),
('实时计算','en','Stream Processing',1,'dataflow'),
('离线计算','en','Batch Processing',1,'dataflow'),
('合流计算','en','Merge',1,'dataflow'),
('分流计算','en','Split',1,'dataflow'),
('单指标异常检测','en','KPI anomaly detection',1,'dataflow'),


('落地到分布式文件系统的数据，可用于小时级以上的离线统计计算', 'en', 'Data that rolls out to the distributed file system can be used for offline statistical calculations in hours or above',1,'dataflow'),
('清洗、实时计算的结果数据，数据延迟低，可用于分钟级别的实时统计计算', 'en', 'Result data of cleaning and real-time calculation has low delay, and can be used for real-time statistical calculations in minutes',1,'dataflow'),
('在K-V存储的状态数据，可用作维度表与实时数据join','en','Status data stored in K-V can be used as dimension table and real-time data join',1,'dataflow'),
('基于流式处理的实时计算,支持秒级和分钟级的计算', 'en', 'Real-time calculation based on streaming processing, supporting calculations in seconds and minutes',1,'dataflow'),
('基于批处理的离线计算，支持小时级和天级的计算', 'en', 'Offline calculation based on batch processing, supporting calculations in hours and days',1,'dataflow'),
('支持表结构相同的多个数据源节点合并成为一个结果数据表','en', 'Support multiple data source nodes with the same ResultTable schema to be merged into one ResultTable',1,'dataflow'),
('支持一个数据源根据业务维度切分成若干数据结构相同的结果数据表', 'en', 'Support a data source to be divided into several result tables with the same ResultTable schema according to the business dimension',1,'dataflow'),
('机器学习模型，用于时间序列数据的单指标异常检测', 'en', 'Machine learning model for anomaly detection of univariate time-series data',1,'dataflow');



INSERT IGNORE INTO `content_language_config` (`content_key`, `language`, `content_value`, `active`, `description`) VALUES
('Druid（分析型、时序型存储）', 'en', 'Druid (analytical, tsdb storage)',1,'dataflow'),
('<b>建议使用场景：</b><br> 时序数据分析<br> <b>建议日数据量[单表]：</b><br> TB/千万级以上 <br><b>查询模式：</b><br> 明细查询/聚合查询','en','<b> Suggested Scenario: </b><br> Time Series Data Analysis <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> TB/10 million or more <br><b> Query Mode: </b><br> Detailed Query/Aggregated Query',1,'dataflow'),

('Elasticsearch（全文检索、分析型存储）', 'en', 'Elastic search (full-text retrieval, analytical storage)',1,'dataflow'),
('<b>建议使用场景：</b><br> 全文检索及数据分析<br> <b>建议日数据量[单表]：</b><br> GB/TB级 <br><b>查询模式：</b><br> 检索查询（支持分词）','en', '<b> Suggested Scenario: </b><br> Full Text Retrieval and Data Analysis <br><b><Suggested Daily Data Volume [Single Table]: </b><br> GB/TB<br><b> Query Mode: </b><br> Retrieval Query (Support Word Segmentation)', 1,'dataflow'),

('HDFS（分布式文件系统）','en','HDFS (Distributed File System)',1,'dataflow'),
('<b>建议使用场景：</b><br> 海量数据离线分析，对查询时延要求不高<br> <b>建议日数据量[单表]：</b><br> TB/PB <br><b>查询模式：</b><br> 可对接离线分析/即席查询/数据分析等方式','en','<b> Suggested Scenario: </b><br> Massive data offline analysis, the request for query latency is not high <br><b> Suggested daily data volume [single table]: </b><br> TB/PB<br><b> query mode: </b><br> dockable offline analysis/ad hoc query/data analysis, etc.',1,'dataflow'),


('Queue（消息队列）','en','Queue (message queue)',1,'dataflow'),
('<b>建议使用场景：</b><br> 数据订阅<br> <b>建议日数据量[单表]：</b><br> GB/TB <br><b>查询模式：</b><br> 客户端连接消费','en', '<b> Suggested Scenario: </b><br> Data Subscription <br> <b> Recommended Daily Data Volume [Single Table]: </b><br> GB/TB<br><b> Query Mode: </b><br> Client Connection Consumption',1,'dataflow'),


('TSpider（关系型数据库）','en','TSpider (Relational Database)',1,'dataflow'),
('<b>建议使用场景：</b><br> 关系型数据库<br> <b>建议日数据量[单表]：</b><br> GB/千万级以内 <br><b>查询模式：</b><br> 点查询/关联查询/聚合查询','en', '<b> Suggested Scenario: </b><br> Relational Database <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> GB/10 Million Level <br><b> Query Mode: </b><br> Point Query/Associated Query/Aggregated Query',1,'dataflow');
