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

update dataflow_node_component_config set component_alias='Processing' where component_name = 'calculate';
update dataflow_node_component_config set component_alias='Source' where component_name = 'source';
update dataflow_node_component_config set component_alias='Sink' where component_name = 'storage';

update dataflow_node_config set node_type_alias='Stream Source', description='Result data of cleaning and real-time calculation has low delay, and can be used for real-time statistical calculations in minutes' where node_type_name = 'stream_source';
update dataflow_node_config set node_type_alias='Batch Source', description='Data that rolls out to the distributed file system can be used for offline statistical calculations in hours or above' where node_type_name = 'batch_source';
update dataflow_node_config set node_type_alias='Stream Processing', description='Real-time calculation based on streaming processing, supporting calculations in seconds and minutes' where node_type_name = 'stream';
update dataflow_node_config set node_type_alias='Batch Processing', description='Offline calculation based on batch processing, supporting calculations in hours and days' where node_type_name = 'batch';
update dataflow_node_config set node_type_alias='ElasticSearch', description='Full-text retrieval based on Elasticsearch log' where node_type_name = 'es';
update dataflow_node_config set node_type_alias='HDFS', description='Historical data storage based on HDFS cannot be directly queried and it can only be connected offline' where node_type_name = 'hdfs';
update dataflow_node_config set node_type_alias='MySQL', description='Relational database storage based on MySQL' where node_type_name = 'mysql';
