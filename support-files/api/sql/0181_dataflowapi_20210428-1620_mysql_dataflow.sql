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

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS `dataflow_node_type_config`;
DROP TABLE IF EXISTS `dataflow_node_type_group`; 
DROP TABLE IF EXISTS `dataflow_node_type_group_rule`; 
DROP TABLE IF EXISTS `dataflow_node_type_instance_config`;
DROP TABLE IF EXISTS `dataflow_node_type_instance_rule`;
DROP TABLE IF EXISTS `dataflow_node_type_rule`;
SET FOREIGN_KEY_CHECKS = 1;

-- ----------------------------
-- Table structure for dataflow_node_instance
-- ----------------------------
DROP TABLE IF EXISTS `dataflow_node_instance`;
CREATE TABLE `dataflow_node_instance` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一标识',
  `node_type_instance_name` varchar(255) NOT NULL COMMENT '节点实例，画布左侧工具栏每一个节点对应表中的一条记录',
  `node_type_instance_alias` varchar(255) NOT NULL COMMENT '节点实例中文名称',
  `group_type_name` varchar(255) NOT NULL COMMENT '属于哪种类型，保留，前端展示',
  `group_type_alias` varchar(255) NOT NULL COMMENT '属于哪种类型，中文描述',
  `order` int(11) NOT NULL COMMENT '画布左侧工具栏的排列，101代表第一层次的第01号节点',
  `support_debug` tinyint(1) NOT NULL COMMENT '是否支持调试',
  `available` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `description` text NOT NULL COMMENT '在画布上的描述信息',
  `created_by` varchar(128) NOT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `node_type_instance_name` (`node_type_instance_name`)
) ENGINE=InnoDB AUTO_INCREMENT=40 DEFAULT CHARSET=utf8 COMMENT='dataflow节点实例配置表';

-- ----------------------------
-- Records of dataflow_node_instance
-- ----------------------------
BEGIN;
INSERT INTO `dataflow_node_instance` VALUES (1, 'stream_source', '实时数据源', 'source', '数据源', 101, 1, 1, '清洗、实时计算的结果数据，数据延迟低，可用于分钟级别的实时统计计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (2, 'kv_source', 'TRedis实时关联数据源', 'source', '数据源', 102, 0, 1, '在K-V存储的状态数据，可用作维度表与实时数据join', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-12-03 15:02:00');
INSERT INTO `dataflow_node_instance` VALUES (3, 'batch_source', '离线数据源', 'source', '数据源', 103, 1, 1, '落地到分布式文件系统的数据，可用于小时级以上的离线统计计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (4, 'batch_kv_source', '离线关联数据源', 'source', '数据源', 104, 0, 0, '在K-V存储的状态数据，可用作维度表与离线数据join', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2021-01-04 14:28:53');
INSERT INTO `dataflow_node_instance` VALUES (5, 'tdw_source', 'TDW数据源', 'source', '数据源', 105, 0, 1, '落地到TDW中的数据，标准化后可用于TDW/TDW-JAR离线计算节点', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2019-09-23 16:10:49');
INSERT INTO `dataflow_node_instance` VALUES (6, 'stream', '实时计算', 'processing', '数据处理', 201, 1, 1, '基于流式处理的实时计算,支持秒级和分钟级的计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (7, 'batch', '离线计算', 'processing', '数据处理', 202, 1, 1, '基于批处理的离线计算，支持小时级和天级的计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (8, 'split', '分流计算', 'processing', '数据处理', 203, 0, 1, '支持一个数据源根据业务维度切分成若干数据结构相同的结果数据表', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (9, 'merge', '合流计算', 'processing', '数据处理', 204, 0, 1, '支持表结构相同的多个数据源节点合并成为一个结果数据表', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (10, 'tdw_batch', 'TDW离线计算', 'processing', '数据处理', 205, 0, 1, 'SQL计算节点，基于洛子SparkScala任务类型实现的批处理任务，支持小时级和天级计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (11, 'tdw_jar_batch', 'TDW-JAR离线计算', 'processing', '数据处理', 206, 0, 1, 'Jar包计算节点，基于洛子SparkScala任务类型实现的批处理任务，支持小时级和天级计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (12, 'spark_structured_streaming', 'Spark Structured Streaming Code', 'processing', '数据处理', 207, 0, 1, '基于Spark Structured Streaming的实时计算，支持秒级和分钟级的计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (13, 'flink_streaming', 'Flink Streaming Code', 'processing', '数据处理', 208, 0, 1, '基于Flink Streaming的实时计算，支持秒级和分钟级的计算', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02');
INSERT INTO `dataflow_node_instance` VALUES (14, 'model_app', 'MLSQL\n模型应用', 'modeling', '机器学习', 304, 0, 1, 'MLSQL模型应用', 'dataflow', '2020-09-10 11:43:02', 'dataflow', '2020-11-30 11:54:20');
INSERT INTO `dataflow_node_instance` VALUES (15, 'model', 'ModelFlow模型', 'modeling', '机器学习', 301, 0, 1, '将Modelflow上已发布的模型版本在Dataflow上应用', 'dataflow', '2019-09-23 16:21:02', 'dataflow', '2020-11-12 14:34:08');
INSERT INTO `dataflow_node_instance` VALUES (16, 'process_model', '单指标异常检测', 'modeling', '机器学习', 302, 0, 0, '机器学习模型，用于时间序列数据的单指标异常检测', 'dataflow', '2019-09-23 16:21:02', 'dataflow', '2021-04-01 16:02:37');
INSERT INTO `dataflow_node_instance` VALUES (17, 'model_ts_custom', '时序模型应用', 'modeling', '机器学习', 303, 0, 1, '时序模型应用', 'dataflow', '2020-09-17 11:38:02', 'dataflow', '2020-10-12 15:57:27');
INSERT INTO `dataflow_node_instance` VALUES (18, 'druid_storage', 'Druid（分析型、时序型存储）', 'storage', '数据存储', 501, 0, 1, '<b>建议使用场景：</b><br> 时序数据分析<br> <b>建议日数据量[单表]：</b><br> TB/千万级以上 <br><b>查询模式：</b><br> 明细查询/聚合查询', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:56:50');
INSERT INTO `dataflow_node_instance` VALUES (19, 'hermes_storage', 'Hermes（分析型数据库）', 'storage', '数据存储', 502, 0, 1, '<b>建议使用场景：</b><br> 海量数据分析及检索<br> <b>建议日数据量[单表]：</b><br> TB/千万级以上 <br><b>查询模式：</b><br> 明细查询/聚合查询', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:56:53');
INSERT INTO `dataflow_node_instance` VALUES (20, 'elastic_storage', 'Elasticsearch（全文检索、分析型存储）', 'storage', '数据存储', 503, 0, 1, '<b>建议使用场景：</b><br> 全文检索及数据分析<br> <b>建议日数据量[单表]：</b><br> GB/TB级 <br><b>查询模式：</b><br> 检索查询（支持分词）', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:56:56');
INSERT INTO `dataflow_node_instance` VALUES (21, 'tpg', 'TPG（关系型数据库）', 'storage', '数据存储', 504, 0, 1, '<b>建议使用场景：</b><br> 关系型数据库<br> <b>建议日数据量[单表]：</b><br> GB/千万级以内 <br><b>查询模式：</b><br> 点查询/关联查询/聚合查询', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:06');
INSERT INTO `dataflow_node_instance` VALUES (22, 'pgsql_storage', 'Postgresql（关系型数据库）', 'storage', '数据存储', 505, 0, 1, '<b>建议使用场景：</b><br> 关系型数据库<br> <b>建议日数据量[单表]：</b><br> GB/千万级以内 <br><b>查询模式：</b><br> 点查询/关联查询/聚合查询', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:11');
INSERT INTO `dataflow_node_instance` VALUES (23, 'tspider_storage', 'TSpider（关系型数据库）', 'storage', '数据存储', 506, 1, 1, '<b>建议使用场景：</b><br> 关系型数据库<br> <b>建议日数据量[单表]：</b><br> GB/千万级以内 <br><b>查询模式：</b><br> 点查询/关联查询/聚合查询', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:16');
INSERT INTO `dataflow_node_instance` VALUES (24, 'queue_pulsar', 'Pulsar（消息队列）', 'storage', '数据存储', 507, 0, 0, '<b>建议使用场景：</b><br> 数据订阅<br> <b>建议日数据量[单表]：</b><br> GB/TB <br><b>查询模式：</b><br> 客户端连接消费', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:19');
INSERT INTO `dataflow_node_instance` VALUES (25, 'tdbank', 'TDBank（消息队列）', 'storage', '数据存储', 508, 0, 1, '<b>建议使用场景：</b><br> 数据订阅<br> <b>建议日数据量[单表]：</b><br> GB/TB <br><b>查询模式：</b><br> 客户端连接消费', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:24');
INSERT INTO `dataflow_node_instance` VALUES (26, 'queue_storage', 'Queue（消息队列）', 'storage', '数据存储', 509, 0, 1, '<b>建议使用场景：</b><br> 数据订阅<br> <b>建议日数据量[单表]：</b><br> GB/TB <br><b>查询模式：</b><br> 客户端连接消费', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:28');
INSERT INTO `dataflow_node_instance` VALUES (27, 'tdw_storage', 'TDW（数据仓库）', 'storage', '数据存储', 510, 0, 1, '<b>建议使用场景：</b><br> 海量数据分析<br> <b>建议日数据量[单表]：</b><br> TB/千万级以上 <br><b>查询模式：</b><br> 明细查询/聚合查询', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:32');
INSERT INTO `dataflow_node_instance` VALUES (28, 'hdfs_storage', 'HDFS（分布式文件系统）', 'storage', '数据存储', 511, 0, 1, '<b>建议使用场景：</b><br> 海量数据离线分析，对查询时延要求不高<br> <b>建议日数据量[单表]：</b><br> TB/PB <br><b>查询模式：</b><br> 可对接离线分析/即席查询/数据分析等方式', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:37');
INSERT INTO `dataflow_node_instance` VALUES (29, 'tredis_storage', 'TRedis（消息队列）', 'storage', '数据存储', 512, 0, 1, '<b>建议使用场景：</b><br> 静态关联；数据订阅（注，使用Redis List数据结构）<br> <b>建议日数据量[单表]：</b><br> GB <br><b>查询模式：</b><br> 客户端连接消费', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:41');
INSERT INTO `dataflow_node_instance` VALUES (30, 'ignite', 'Ignite（分布式缓存）', 'storage', '数据存储', 513, 1, 1, '<b>建议使用场景：</b><br> 静态关联<br> <b>建议数据量[单表]：</b><br> 10w条', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:44');
INSERT INTO `dataflow_node_instance` VALUES (31, 'tsdb_storage', 'TSDB', 'storage', '数据存储', 514, 0, 0, '基于InfluxDB的时间序列数据库', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 10:57:49');
INSERT INTO `dataflow_node_instance` VALUES (32, 'mysql_storage', 'MySQL', 'storage', '数据存储', 515, 0, 1, '基于MySQL的关系型数据库存储', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2020-11-27 11:00:25');
INSERT INTO `dataflow_node_instance` VALUES (33, 'tcaplus_storage', 'Tcaplus（分布式KV）', 'storage', '数据存储', 516, 0, 1, '<b>建议使用场景：</b><br>游戏业务申请数据分析专用 Tcaplus 集群，作为第三方写入存储使用<br>', 'dataflow', '2020-11-13 15:09:49', 'dataflow', '2021-03-11 15:48:49');
INSERT INTO `dataflow_node_instance` VALUES (34, 'data_model_app', '数据模型应用', 'datamodel', '数据模型', 401, 0, 1, '提供描述数据、组织数据和操作数据的抽象框架，应用已发布的数据模型，可标准化对应业务过程的数据处理流程', 'dataflow', '2020-11-27 10:10:03', 'dataflow', '2021-03-16 13:06:29');
INSERT INTO `dataflow_node_instance` VALUES (35, 'unified_kv_source', '关联数据源', 'source', '数据源', 106, 0, 1, '在K-V存储的状态数据，可用作维度表与<b>实时或离线</b>数据join', 'dataflow', '2020-12-02 20:43:09', 'dataflow', '2020-12-18 11:11:47');
INSERT INTO `dataflow_node_instance` VALUES (36, 'clickhouse_storage', 'ClickHouse（分析型数据库）', 'storage', '数据存储', 517, 0, 1, '<b>建议使用场景：</b><br>分析型数据库，实时数仓，适合宽表，大数据量和复杂查询场景，建议申请专用ClickHouse集群<br>', 'dataflow', '2020-12-11 10:20:09', 'dataflow', '2021-01-04 14:36:21');
INSERT INTO `dataflow_node_instance` VALUES (37, 'data_model_stream_indicator', '模型实例指标（实时）', 'datamodel', '数据模型', 402, 0, 1, '创建数据模型相关的实时指标，用于后续业务分析', 'dataflow', '2020-12-11 14:49:31', 'dataflow', '2021-03-16 13:06:50');
INSERT INTO `dataflow_node_instance` VALUES (38, 'data_model_batch_indicator', '模型实例指标（离线）', 'datamodel', '数据模型', 403, 0, 1, '创建数据模型相关的离线指标，用于后续业务分析', 'dataflow', '2020-12-11 14:50:56', 'dataflow', '2021-03-16 13:07:02');
COMMIT;


DROP TABLE IF EXISTS `dataflow_node_instance_link`;
CREATE TABLE `dataflow_node_instance_link` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一标识',
  `upstream` varchar(255) NOT NULL COMMENT '一条连线规则中的上游节点',
  `downstream` varchar(255) NOT NULL COMMENT '一条连线规则中的下游节点',
  `downstream_link_limit` varchar(255) NOT NULL COMMENT '上游节点的下游限制范围',
  `upstream_link_limit` varchar(255) NOT NULL COMMENT '下游节点的上游限制范围',
  `available` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `description` text COMMENT '描述',
  `created_by` varchar(128) NOT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `node_instance_link_uniq1` (`upstream`,`downstream`)
) ENGINE=InnoDB AUTO_INCREMENT=111 DEFAULT CHARSET=utf8 COMMENT='dataflow原子连线规则，实际的实例连线规则由原子规则导出';

-- ----------------------------
-- Records of dataflow_node_instance_link
-- ----------------------------
BEGIN;
INSERT INTO `dataflow_node_instance_link` VALUES (1, 'model_app', 'hdfs', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (4, 'stream', 'kafka', '1,1', '1,1', 1, '如果接kafka不是一个限制，那么跳数后接存储就不止一个', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-11-12 17:34:19');
INSERT INTO `dataflow_node_instance_link` VALUES (5, 'batch_kv_source', 'batch', '1,10', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-26 17:14:09');
INSERT INTO `dataflow_node_instance_link` VALUES (6, 'kafka', 'tdw_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (7, 'kafka', 'model_ts_custom', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:35:05');
INSERT INTO `dataflow_node_instance_link` VALUES (8, 'kafka', 'stream', '1,50', '1,2', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-02-04 11:19:43');
INSERT INTO `dataflow_node_instance_link` VALUES (9, 'kafka', 'split', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:35:31');
INSERT INTO `dataflow_node_instance_link` VALUES (10, 'kafka', 'merge', '1,10', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-03-08 11:17:31');
INSERT INTO `dataflow_node_instance_link` VALUES (11, 'kafka', 'spark_structured_streaming', '1,50', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-11-12 19:40:16');
INSERT INTO `dataflow_node_instance_link` VALUES (12, 'kafka', 'flink_streaming', '1,50', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-11-12 19:40:20');
INSERT INTO `dataflow_node_instance_link` VALUES (13, 'kafka', 'process_model', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:18:45');
INSERT INTO `dataflow_node_instance_link` VALUES (14, 'kafka', 'druid_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (15, 'kafka', 'hermes_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (16, 'kafka', 'elastic_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (17, 'kafka', 'pgsql_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (18, 'kafka', 'tspider_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (19, 'kafka', 'tdbank', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (20, 'kafka', 'queue_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (21, 'kafka', 'hdfs', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (22, 'kafka', 'tredis_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (23, 'kafka', 'ignite', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (24, 'kafka', 'queue_pulsar', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (25, 'kafka', 'tsdb_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (26, 'kafka', 'mysql_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (27, 'model_ts_custom', 'kafka', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (28, 'model_ts_custom', 'hdfs', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (29, 'tdw', 'tdw_batch', '1,10', '1,3', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-04 11:20:05');
INSERT INTO `dataflow_node_instance_link` VALUES (30, 'tdw', 'tdw_jar_batch', '1,10', '1,3', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-04 11:20:08');
INSERT INTO `dataflow_node_instance_link` VALUES (31, 'tdw_storage', 'tdw', '1,1', '1,1', 1, NULL, 'dataflow', '2021-01-04 11:22:27', 'dataflow', '2021-01-04 11:24:26');
INSERT INTO `dataflow_node_instance_link` VALUES (32, 'hdfs_storage', 'model_ts_custom', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 11:21:22');
INSERT INTO `dataflow_node_instance_link` VALUES (33, 'hdfs_storage', 'model_app', '1,10', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 11:21:26');
INSERT INTO `dataflow_node_instance_link` VALUES (34, 'hdfs_storage', 'batch', '1,50', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 11:21:30');
INSERT INTO `dataflow_node_instance_link` VALUES (35, 'hdfs_storage', 'process_model', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 11:21:36');
INSERT INTO `dataflow_node_instance_link` VALUES (36, 'hdfs_storage', 'data_model_batch_indicator', '1,10', '1,1', 1, NULL, 'dataflow', '2020-12-11 15:01:16', 'dataflow', '2021-01-25 11:25:19');
INSERT INTO `dataflow_node_instance_link` VALUES (39, 'stream_source', 'kafka', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (41, 'mysql_storage', 'model', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:45:17');
INSERT INTO `dataflow_node_instance_link` VALUES (42, 'kv_source', 'stream', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-12-11 18:15:57');
INSERT INTO `dataflow_node_instance_link` VALUES (48, 'hdfs', 'model_ts_custom', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (49, 'hdfs', 'model_app', '1,10', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (50, 'hdfs', 'batch', '1,50', '1,10', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (51, 'hdfs', 'process_model', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (52, 'hdfs', 'queue_pulsar', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (53, 'hdfs', 'ignite', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (54, 'hdfs', 'elastic_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (55, 'hdfs', 'tdbank', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (56, 'hdfs', 'queue_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (57, 'hdfs', 'pgsql_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (58, 'hdfs', 'tsdb_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (59, 'hdfs', 'tspider_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (60, 'hdfs', 'druid_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (61, 'hdfs', 'hermes_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (62, 'hdfs', 'tredis_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (63, 'hdfs', 'mysql_storage', '1,1', '1,1', 1, '黑名单', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (64, 'hdfs', 'hdfs_storage', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 11:26:55');
INSERT INTO `dataflow_node_instance_link` VALUES (67, 'spark_structured_streaming', 'kafka', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:57:11');
INSERT INTO `dataflow_node_instance_link` VALUES (69, 'flink_streaming', 'kafka', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:52:44');
INSERT INTO `dataflow_node_instance_link` VALUES (70, 'batch', 'hdfs', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (71, 'tspider_storage', 'model', '1,10', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:56:41');
INSERT INTO `dataflow_node_instance_link` VALUES (72, 'tdw_batch', 'tdw', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-04 16:29:14');
INSERT INTO `dataflow_node_instance_link` VALUES (73, 'tdw_jar_batch', 'tdw', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-04 16:30:04');
INSERT INTO `dataflow_node_instance_link` VALUES (74, 'tdw', 'tpg', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-04 16:30:43');
INSERT INTO `dataflow_node_instance_link` VALUES (75, 'merge', 'kafka', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:57:20');
INSERT INTO `dataflow_node_instance_link` VALUES (76, 'tdw_source', 'tdw_batch', '1,10', '1,3', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:55:55');
INSERT INTO `dataflow_node_instance_link` VALUES (77, 'tdw_source', 'tdw_jar_batch', '1,10', '1,3', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-28 15:55:57');
INSERT INTO `dataflow_node_instance_link` VALUES (78, 'model', 'hdfs', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (79, 'process_model', 'kafka', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2020-10-12 20:43:55');
INSERT INTO `dataflow_node_instance_link` VALUES (81, 'process_model', 'hdfs', '1,1', '1,1', 1, '', 'dataflow', '2020-10-12 20:43:37', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (82, 'stream', 'memory', '1,1', '1,1', 1, '特殊内存', 'dataflow', '2020-11-12 15:51:27', 'dataflow', '2020-11-12 15:52:00');
INSERT INTO `dataflow_node_instance_link` VALUES (83, 'memory', 'stream', '1,50', '1,2', 1, '特殊内存', 'dataflow', '2020-11-12 15:54:20', 'dataflow', '2021-02-04 11:19:40');
INSERT INTO `dataflow_node_instance_link` VALUES (84, 'kafka', 'tcaplus_storage', '1,1', '1,1', 1, NULL, 'dataflow', '2020-11-13 15:08:01', 'dataflow', '2020-11-13 15:11:38');
INSERT INTO `dataflow_node_instance_link` VALUES (85, 'batch_source', 'hdfs', '1,1', '1,1', 1, NULL, 'dataflow', '2020-11-16 15:45:06', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (86, 'model', 'kafka', '1,1', '1,1', 1, NULL, 'dataflow', '2020-11-16 15:49:42', 'dataflow', '2020-11-16 15:49:54');
INSERT INTO `dataflow_node_instance_link` VALUES (87, 'model_app', 'kafka', '1,1', '1,1', 0, NULL, 'dataflow', '2020-11-16 15:50:15', 'dataflow', '2020-11-18 10:26:26');
INSERT INTO `dataflow_node_instance_link` VALUES (88, 'hdfs', 'model', '1,10', '1,1', 1, NULL, 'dataflow', '2020-11-16 15:56:44', 'dataflow', '2021-01-25 15:37:33');
INSERT INTO `dataflow_node_instance_link` VALUES (89, 'kafka', 'model', '1,10', '1,1', 1, NULL, 'dataflow', '2020-11-16 15:59:28', 'dataflow', '2020-11-16 16:02:23');
INSERT INTO `dataflow_node_instance_link` VALUES (90, 'kafka', 'model_app', '1,10', '1,10', 0, NULL, 'dataflow', '2020-11-16 15:59:46', 'dataflow', '2020-11-18 10:26:30');
INSERT INTO `dataflow_node_instance_link` VALUES (94, 'data_model_app', 'kafka', '1,1', '1,1', 1, NULL, 'dataflow', '2020-11-27 10:12:45', 'dataflow', '2021-02-01 17:55:42');
INSERT INTO `dataflow_node_instance_link` VALUES (95, 'stream_source', 'data_model_app', '1,10', '1,1', 1, NULL, 'dataflow', '2020-11-27 11:01:59', 'dataflow', '2020-12-17 20:06:29');
INSERT INTO `dataflow_node_instance_link` VALUES (96, 'kv_source', 'data_model_app', '1,10', '1,1', 0, NULL, 'dataflow', '2020-11-27 13:14:26', 'dataflow', '2021-01-26 18:14:19');
INSERT INTO `dataflow_node_instance_link` VALUES (97, 'unified_kv_source', 'stream', '1,10', '1,1', 1, NULL, 'dataflow', '2020-12-03 15:04:44', 'dataflow', '2020-12-11 18:18:04');
INSERT INTO `dataflow_node_instance_link` VALUES (98, 'unified_kv_source', 'batch', '1,10', '1,10', 1, NULL, 'dataflow', '2020-12-03 15:04:55', 'dataflow', '2020-12-07 10:03:31');
INSERT INTO `dataflow_node_instance_link` VALUES (99, 'kafka', 'clickhouse_storage', '1,1', '1,1', 1, NULL, 'dataflow', '2020-12-11 10:28:50', 'dataflow', '2020-12-11 10:29:00');
INSERT INTO `dataflow_node_instance_link` VALUES (100, 'data_model_stream_indicator', 'kafka', '1,1', '1,1', 1, NULL, 'dataflow', '2020-12-11 14:58:36', 'dataflow', '2020-12-11 14:59:04');
INSERT INTO `dataflow_node_instance_link` VALUES (101, 'data_model_batch_indicator', 'hdfs', '1,1', '1,1', 1, NULL, 'dataflow', '2020-12-11 14:59:38', 'dataflow', '2021-01-25 15:42:42');
INSERT INTO `dataflow_node_instance_link` VALUES (102, 'kafka', 'data_model_stream_indicator', '1,10', '1,1', 1, NULL, 'dataflow', '2020-12-11 15:00:11', 'dataflow', '2021-02-02 14:33:12');
INSERT INTO `dataflow_node_instance_link` VALUES (103, 'hdfs', 'data_model_batch_indicator', '1,10', '1,1', 1, NULL, 'dataflow', '2020-12-11 15:01:16', 'dataflow', '2021-02-02 14:39:27');
INSERT INTO `dataflow_node_instance_link` VALUES (104, 'unified_kv_source', 'data_model_app', '1,10', '1,1', 1, NULL, 'dataflow', '2020-12-11 18:01:27', 'dataflow', '2020-12-11 18:02:23');
INSERT INTO `dataflow_node_instance_link` VALUES (105, 'hdfs', 'clickhouse_storage', '1,1', '1,1', 1, NULL, 'dataflow', '2020-12-21 14:42:38', 'dataflow', '2021-01-25 15:37:33');
COMMIT;

DROP TABLE IF EXISTS `dataflow_node_instance_link_group_v2`;
CREATE TABLE `dataflow_node_instance_link_group_v2` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一标识',
  `node_instance_name` varchar(255) NOT NULL COMMENT '下游节点实例名称',
  `upstream_max_link_limit` varchar(255) NOT NULL COMMENT '被组合的节点上游连接限制',
  `description` text COMMENT '描述',
  `created_by` varchar(128) NOT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COMMENT='定义组合连接的情况';

-- ----------------------------
-- Records of dataflow_node_instance_link_group_v2
-- ----------------------------
BEGIN;
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (1, 'stream', '{\"kv_source\": 10, \"stream_source\": 1}', NULL, 'dataflow', '2020-11-27 11:41:54', 'dataflow', '2020-12-08 11:58:06');
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (2, 'stream', '{\"kv_source\": 10, \"stream\": 1}', NULL, 'dataflow', '2020-11-27 11:42:54', 'dataflow', '2020-12-08 11:58:11');
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (3, 'stream', '{\"unified_kv_source\": 10, \"merge\": 1}', '', 'dataflow', '2021-01-15 09:59:27', 'dataflow', '2021-01-15 09:59:49');
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (4, 'data_model_app', '{\"kv_source\": 10, \"stream_source\": 1}', NULL, 'dataflow', '2020-11-27 14:43:42', 'dataflow', '2020-12-08 11:58:17');
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (5, 'stream', '{\"unified_kv_source\": 10, \"stream\": 1}', NULL, 'dataflow', '2020-12-03 15:06:55', 'dataflow', '2020-12-08 11:58:21');
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (6, 'stream', '{\"unified_kv_source\": 10, \"stream_source\": 1}', NULL, 'dataflow', '2020-12-03 15:09:14', 'dataflow', '2020-12-08 12:03:08');
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (7, 'data_model_app', '{\"unified_kv_source\": 10, \"stream_source\": 1}', NULL, 'dataflow', '2020-12-11 18:02:57', 'dataflow', '2020-12-11 18:03:51');
INSERT INTO `dataflow_node_instance_link_group_v2` VALUES (8, 'stream', '{\"kv_source\": 10, \"merge\": 1}', NULL, 'dataflow', '2021-01-15 10:10:09', 'dataflow', '2021-01-15 10:11:39');
COMMIT;

DROP TABLE IF EXISTS `dataflow_node_instance_link_blacklist`;
CREATE TABLE `dataflow_node_instance_link_blacklist` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一标识',
  `upstream` varchar(255) NOT NULL COMMENT '一条连线规则中的上游节点',
  `downstream` varchar(255) NOT NULL COMMENT '一条连线规则中的下游节点',
  `description` text COMMENT '描述',
  `created_by` varchar(128) NOT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `node_instance_link_blacklist_uniq1` (`upstream`,`downstream`)
) ENGINE=InnoDB AUTO_INCREMENT=159 DEFAULT CHARSET=utf8 COMMENT='连接黑名单，由原子连接推导后的连线规则，再用黑名单过滤';

-- ----------------------------
-- Records of dataflow_node_instance_link_blacklist
-- ----------------------------
BEGIN;
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (1, 'model_app', 'model_ts_custom', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (2, 'model_app', 'batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (3, 'model_app', 'process_model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (4, 'model_app', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (5, 'model_app', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (6, 'model_app', 'model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (7, 'stream', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (8, 'stream', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (9, 'stream', 'model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (10, 'model_ts_custom', 'stream', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (11, 'model_ts_custom', 'split', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (12, 'model_ts_custom', 'merge', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (13, 'model_ts_custom', 'spark_structured_streaming', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (14, 'model_ts_custom', 'flink_streaming', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (15, 'model_ts_custom', 'process_model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (16, 'model_ts_custom', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (17, 'model_ts_custom', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (18, 'model_ts_custom', 'model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (19, 'model_ts_custom', 'model_app', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (20, 'model_ts_custom', 'batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (21, 'stream_source', 'tdw_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (22, 'stream_source', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (23, 'stream_source', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (24, 'tdw_storage', 'tpg', '', 'dataflow', '2021-01-04 16:40:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (25, 'stream', 'model_app', '', 'dataflow', '2021-01-04 16:40:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (26, 'spark_structured_streaming', 'model_app', '', 'dataflow', '2021-01-04 16:40:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (27, 'flink_streaming', 'model_app', '', 'dataflow', '2021-01-04 16:40:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (28, 'batch_source', 'clickhouse_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (38, 'spark_structured_streaming', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (39, 'spark_structured_streaming', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (40, 'spark_structured_streaming', 'model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (41, 'flink_streaming', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (42, 'flink_streaming', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (43, 'flink_streaming', 'model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (44, 'batch', 'model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (45, 'merge', 'model_ts_custom', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (46, 'merge', 'split', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (48, 'merge', 'process_model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (49, 'merge', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (50, 'merge', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (51, 'merge', 'model', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (52, 'merge', 'model_app', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (53, 'merge', 'batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (54, 'model', 'model_app', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (55, 'model', 'batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (56, 'model', 'ignite', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (57, 'model', 'elastic_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (58, 'model', 'tdbank', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (59, 'model', 'tsdb_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (60, 'model', 'druid_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (61, 'model', 'hermes_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (62, 'model', 'tredis_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (63, 'process_model', 'model_ts_custom', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (64, 'process_model', 'stream', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (65, 'process_model', 'split', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (66, 'process_model', 'merge', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (67, 'process_model', 'spark_structured_streaming', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (68, 'process_model', 'flink_streaming', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (69, 'process_model', 'model_app', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (70, 'process_model', 'batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (71, 'process_model', 'tdw_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (72, 'process_model', 'tdw_jar_batch', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (73, 'stream_source', 'model_app', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (74, 'stream_source', 'data_model_batch_indicator', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (92, 'model', 'split', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (93, 'batch_source', 'mysql_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (94, 'batch_source', 'elastic_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (95, 'batch_source', 'tredis_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (96, 'batch_source', 'tspider_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (97, 'model', 'spark_structured_streaming', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (98, 'batch_source', 'hermes_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (99, 'batch_source', 'pgsql_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (100, 'batch_source', 'hdfs_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (101, 'batch_source', 'queue_pulsar', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (102, 'batch_source', 'queue_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (103, 'batch_source', 'tdbank', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (104, 'model', 'merge', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (105, 'model', 'tdw_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (106, 'model', 'flink_streaming', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (107, 'batch_source', 'ignite', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (108, 'batch_source', 'druid_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (109, 'batch_source', 'tsdb_storage', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (110, 'model', 'stream', '', 'dataflow', '2020-10-13 10:13:37');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (112, 'stream_source', 'batch', '新版本不支持连接', 'dataflow', '2020-12-28 17:50:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (113, 'model_app', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (114, 'stream', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (115, 'stream', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (116, 'model_ts_custom', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (117, 'model_ts_custom', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (118, 'data_model_batch_indicator', 'batch', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (119, 'data_model_batch_indicator', 'model_app', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (120, 'data_model_batch_indicator', 'model', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (121, 'data_model_batch_indicator', 'process_model', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (122, 'data_model_batch_indicator', 'model_ts_custom', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (123, 'data_model_stream_indicator', 'model_ts_custom', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (124, 'data_model_stream_indicator', 'stream', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (125, 'data_model_stream_indicator', 'model_app', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (126, 'data_model_stream_indicator', 'tdw_storage', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (128, 'data_model_stream_indicator', 'split', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (129, 'data_model_stream_indicator', 'spark_structured_streaming', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (130, 'data_model_stream_indicator', 'flink_streaming', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (131, 'data_model_stream_indicator', 'batch', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (132, 'data_model_stream_indicator', 'merge', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (133, 'data_model_stream_indicator', 'model', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (134, 'data_model_stream_indicator', 'process_model', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (135, 'stream_source', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (136, 'data_model_app', 'model_app', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (137, 'data_model_app', 'stream', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (138, 'data_model_app', 'model_ts_custom', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (139, 'data_model_app', 'tdw_storage', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (140, 'data_model_app', 'split', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (141, 'data_model_app', 'spark_structured_streaming', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (142, 'data_model_app', 'flink_streaming', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (143, 'data_model_app', 'batch', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (144, 'data_model_app', 'merge', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (145, 'data_model_app', 'model', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (146, 'data_model_app', 'process_model', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (147, 'batch_source', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (148, 'spark_structured_streaming', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (149, 'spark_structured_streaming', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (150, 'flink_streaming', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (151, 'flink_streaming', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (152, 'batch', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (153, 'merge', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (154, 'merge', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (155, 'model', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (156, 'model', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (157, 'process_model', 'data_model_batch_indicator', '', 'dataflow', '2021-02-03 16:30:49');
INSERT INTO `dataflow_node_instance_link_blacklist` VALUES (158, 'process_model', 'data_model_stream_indicator', '', 'dataflow', '2021-02-03 16:30:49');
COMMIT;

DROP TABLE IF EXISTS `dataflow_node_instance_link_channel`;
CREATE TABLE `dataflow_node_instance_link_channel` (
  `result_table_id` varchar(255) NOT NULL COMMENT '结果表id',
  `channel_storage` varchar(255) NOT NULL COMMENT 'rt_id的channel，kafka/hdfs/tdw',
  `downstream_node_id` int(11) NOT NULL COMMENT '下游节点id',
  `channel_mode` varchar(255) DEFAULT NULL COMMENT '删存储需要记channel_mode',
  `created_by` varchar(128) NOT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`result_table_id`,`channel_storage`,`downstream_node_id`),
  KEY `key_rt_id_channel` (`result_table_id`,`channel_storage`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='连线规则链路表';

