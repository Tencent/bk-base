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

delete from dataflow_node_instance where node_type_instance_name in ('tdw_storage', 'tdw_source', 'spark_structured_streaming', 'flink_streaming', 'tdbank', 'tpg', 'tdw', 'tdw_batch', 'tdw_jar_batch', 'hermes_storage', 'tsdb_storage', 'tcaplus_storage', 'data_model_app', 'data_model_stream_indicator', 'data_model_batch_indicator');

delete from dataflow_node_instance_link where upstream in ('tdw_storage', 'tdw_source', 'spark_structured_streaming', 'flink_streaming', 'tdbank', 'tpg', 'tdw', 'tdw_batch', 'tdw_jar_batch', 'hermes_storage', 'tsdb_storage', 'tcaplus_storage', 'data_model_app', 'data_model_stream_indicator', 'data_model_batch_indicator') or downstream in ('tdw_storage', 'tdw_source', 'spark_structured_streaming', 'flink_streaming', 'tdbank', 'tpg', 'tdw_batch', 'tdw_jar_batch', 'hermes_storage', 'tsdb_storage', 'tcaplus_storage', 'data_model_app', 'data_model_stream_indicator', 'data_model_batch_indicator');

delete from dataflow_node_instance_link_blacklist where upstream in ('tdw_storage', 'tdw_source', 'spark_structured_streaming', 'flink_streaming', 'tdbank', 'tpg', 'tdw', 'tdw_batch', 'tdw_jar_batch', 'hermes_storage', 'tsdb_storage', 'tcaplus_storage', 'data_model_app', 'data_model_stream_indicator', 'data_model_batch_indicator') or downstream in ('tdw_storage', 'tdw_source', 'spark_structured_streaming', 'flink_streaming', 'tdbank', 'tpg', 'tdw', 'tdw_batch', 'tdw_jar_batch', 'hermes_storage', 'tsdb_storage', 'tcaplus_storage', 'data_model_app', 'data_model_stream_indicator', 'data_model_batch_indicator');

delete from dataflow_node_instance_link_group_v2 where node_instance_name in ('tdw_storage', 'tdw_source', 'spark_structured_streaming', 'flink_streaming', 'tdbank', 'tpg', 'tdw', 'tdw_batch', 'tdw_jar_batch', 'hermes_storage', 'tsdb_storage', 'tcaplus_storage', 'data_model_app', 'data_model_stream_indicator', 'data_model_batch_indicator');

