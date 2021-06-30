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

USE bkdata_flow;

SET NAMES utf8;

update dataflow_node_config set node_type_alias='Druid (analytical, tsdb storage)', description='<b> Suggested Scenario: </b><br> Time Series Data Analysis <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> TB/10 million or more <br><b> Query Mode: </b><br> Detailed Query/Aggregated Query' where node_type_name = 'druid';
update dataflow_node_config set node_type_alias='Elastic search (full-text retrieval, analytical storage)', description='<b> Suggested Scenario: </b><br> Full Text Retrieval and Data Analysis <br><b><Suggested Daily Data Volume [Single Table]: </b><br> GB/TB<br><b> Query Mode: </b><br> Retrieval Query (Support Word Segmentation)' where node_type_name = 'es';
update dataflow_node_config set node_type_alias='HDFS (Distributed File System)', description='<b> Suggested Scenario: </b><br> Massive data offline analysis, the request for query latency is not high <br><b> Suggested daily data volume [single table]: </b><br> TB/PB<br><b> query mode: </b><br> dockable offline analysis/ad hoc query/data analysis, etc.' where node_type_name = 'hdfs';
update dataflow_node_config set node_type_alias='Hermes (Analytical Database)', description='<b> Suggested Scenario: </b><br> Massive Data Analysis and Retrieval <br><b> Suggested Daily Data Volume [Single Table]: </b><br> TB/10 million or more <br><b> Query Mode: </b><br> Detailed Query/Aggregated Query' where node_type_name = 'hermes';
update dataflow_node_config set node_type_alias='Queue (message queue)', description='<b> Suggested Scenario: </b><br> Data Subscription <br> <b> Recommended Daily Data Volume [Single Table]: </b><br> GB/TB<br><b> Query Mode: </b><br> Client Connection Consumption' where node_type_name = 'queue';
update dataflow_node_config set node_type_alias='TDW (Data Warehouse)', description='<b> Suggested Scenario: </b><br> Massive Data Analysis <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> TB/Ten Million or more <br><b> Query Mode: </b><br> Detailed Query/Aggregated Query' where node_type_name = 'tdw_storage';
update dataflow_node_config set node_type_alias='TRedis (message queue)', description='<b> Suggested Scenario: </b><br> Static Association; Data Subscription (Use Redis List Data Structure)<br><b> Recommended Daily Data Volume [Single Table]: </b><br> GB<br><b> Query Mode: </b><br> Client Connection Consumption' where node_type_name = 'tredis';
update dataflow_node_config set node_type_alias='TSpider (Relational Database)', description='<b> Suggested Scenario: </b><br> Relational Database <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> GB/10 Million Level <br><b> Query Mode: </b><br> Point Query/Associated Query/Aggregated Query' where node_type_name = 'tspider';