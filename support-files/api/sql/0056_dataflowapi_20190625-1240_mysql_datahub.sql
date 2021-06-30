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
('Druid (analytical, tsdb storage)','zh-cn','Druid（分析型、时序型存储）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Time Series Data Analysis <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> TB/10 million or more <br><b> Query Mode: </b><br> Detailed Query/Aggregated Query','zh-cn','<b>建议使用场景：</b><br> 时序数据分析<br> <b>建议日数据量[单表]：</b><br> TB/千万级以上 <br><b>查询模式：</b><br> 明细查询/聚合查询',1,'dataflow'),

('Elastic search (full-text retrieval, analytical storage)','zh-cn','Elasticsearch（全文检索、分析型存储）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Full Text Retrieval and Data Analysis <br><b><Suggested Daily Data Volume [Single Table]: </b><br> GB/TB<br><b> Query Mode: </b><br> Retrieval Query (Support Word Segmentation)','zh-cn','<b>建议使用场景：</b><br> 全文检索及数据分析<br> <b>建议日数据量[单表]：</b><br> GB/TB级 <br><b>查询模式：</b><br> 检索查询（支持分词）',1,'dataflow'),

('HDFS (Distributed File System)','zh-cn','HDFS（分布式文件系统）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Massive data offline analysis, the request for query latency is not high <br><b> Suggested daily data volume [single table]: </b><br> TB/PB<br><b> query mode: </b><br> dockable offline analysis/ad hoc query/data analysis, etc.','zh-cn','<b>建议使用场景：</b><br> 海量数据离线分析，对查询时延要求不高<br> <b>建议日数据量[单表]：</b><br> TB/PB <br><b>查询模式：</b><br> 可对接离线分析/即席查询/数据分析等方式',1,'dataflow'),

('Hermes (Analytical Database)','zh-cn','Hermes（分析型数据库）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Massive Data Analysis and Retrieval <br><b> Suggested Daily Data Volume [Single Table]: </b><br> TB/10 million or more <br><b> Query Mode: </b><br> Detailed Query/Aggregated Query','zh-cn','<b>建议使用场景：</b><br> 海量数据分析及检索<br> <b>建议日数据量[单表]：</b><br> TB/千万级以上 <br><b>查询模式：</b><br> 明细查询/聚合查询',1,'dataflow'),

('Queue (message queue)','zh-cn','Queue（消息队列）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Data Subscription <br> <b> Recommended Daily Data Volume [Single Table]: </b><br> GB/TB<br><b> Query Mode: </b><br> Client Connection Consumption','zh-cn','<b>建议使用场景：</b><br> 数据订阅<br> <b>建议日数据量[单表]：</b><br> GB/TB <br><b>查询模式：</b><br> 客户端连接消费',1,'dataflow'),

('TDW (Data Warehouse)','zh-cn','TDW（数据仓库）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Massive Data Analysis <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> TB/Ten Million or more <br><b> Query Mode: </b><br> Detailed Query/Aggregated Query','zh-cn','<b>建议使用场景：</b><br> 海量数据分析<br> <b>建议日数据量[单表]：</b><br> TB/千万级以上 <br><b>查询模式：</b><br> 明细查询/聚合查询',1,'dataflow'),

('TRedis (message queue)','zh-cn','TRedis（消息队列）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Static Association; Data Subscription (Use Redis List Data Structure)<br><b> Recommended Daily Data Volume [Single Table]: </b><br> GB<br><b> Query Mode: </b><br> Client Connection Consumption','zh-cn','<b>建议使用场景：</b><br> 静态关联；数据订阅（注，使用Redis List数据结构）<br> <b>建议日数据量[单表]：</b><br> GB <br><b>查询模式：</b><br> 客户端连接消费',1,'dataflow'),

('TSpider (Relational Database)','zh-cn','TSpider（关系型数据库）',1,'dataflow'),
('<b> Suggested Scenario: </b><br> Relational Database <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> GB/10 Million Level <br><b> Query Mode: </b><br> Point Query/Associated Query/Aggregated Query','zh-cn','<b>建议使用场景：</b><br> 关系型数据库<br> <b>建议日数据量[单表]：</b><br> GB/千万级以内 <br><b>查询模式：</b><br> 点查询/关联查询/聚合查询',1,'dataflow');
