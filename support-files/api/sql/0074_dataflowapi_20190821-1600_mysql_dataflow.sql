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

INSERT IGNORE INTO `dataflow_node_config` (`node_type_name`, `node_type_alias`, `component_name`, `belong`, `support_debug`, `max_parent_num`, `created_by`, `created_at`, `updated_by`, `updated_at`, `description`, `has_multiple_parent`)
VALUES
('tspider', 'TSpider (Relational Database)', 'storage', 'bkdata', 0, 1, NULL, '2018-12-17 09:52:54', NULL, '2018-12-17 09:58:18', '<b> Suggested Scenario: </b><br> Relational Database <br> <b> Suggested Daily Data Volume [Single Table]: </b><br> GB/10 Million Level <br><b> Query Mode: </b><br> Point Query/Associated Query/Aggregated Query', 0),
('kv_source', 'KV Source', 'source', 'bkdata', 0, 0, NULL, '2018-12-17 09:52:54', NULL, '2018-12-17 09:58:18', 'Status data stored in K-V can be used as dimension table and real-time data join', 0),
('merge', 'Merge', 'calculate', 'bkdata', 0, 2, NULL, '2019-03-25 14:54:48', NULL, '2019-03-25 15:24:56', 'Support multiple data source nodes with the same ResultTable schema to be merged into one ResultTable', 1),
('split', 'Split', 'calculate', 'bkdata', 0, 1, NULL, '2019-03-25 14:54:48', NULL, '2019-03-25 15:28:04', 'Support a data source to be divided into several result tables with the same ResultTable schema according to the business dimension', 0);