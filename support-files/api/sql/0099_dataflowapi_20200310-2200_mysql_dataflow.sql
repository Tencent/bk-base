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

drop table if exists dataflow_node_type_config;
drop table if exists dataflow_rule_type;
drop table if exists dataflow_node_type_rule;
drop table if exists dataflow_node_type_group;
drop table if exists dataflow_node_type_group_rule;
drop table if exists dataflow_node_type_instance_config;
drop table if exists dataflow_node_type_instance_rule;

SET FOREIGN_KEY_CHECKS = 1;

-- 节点类型配置表

CREATE TABLE IF not EXISTS `dataflow_node_type_config` (
  `node_type_name` varchar(255) NOT NULL COMMENT '节点类型',
  `node_type_alias` varchar(255) NOT NULL COMMENT '节点类型名称',
  `parent_node_type_name` varchar(255) DEFAULT NULL COMMENT '上层节点类型,只允许一个',
  `available` tinyint(1) DEFAULT '1' COMMENT '是否启用该节点类型',
  `belongs_to` varchar(128) DEFAULT 'bkdata' COMMENT '这个字段标记是系统的(bkdata)还是用户的(other)',
  `scope` enum('private','public') NOT NULL DEFAULT 'public' COMMENT '标记节点类型的权限范围，可选有public|private',
  `order` int(11) NOT NULL COMMENT '同一层级的节点类型排序',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`node_type_name`),
  UNIQUE KEY `node_type_config_uniq1` (`node_type_name`,`parent_node_type_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点类型配置表';


INSERT INTO `dataflow_node_type_config` (`node_type_name`, `node_type_alias`, `parent_node_type_name`, `available`, `belongs_to`, `scope`, `order`, `description`)
VALUES
  ('source', '数据源', NULL, 1, 'bkdata', 'public', 1, '数据源描述'),
  ('storage', '数据存储', NULL, 1, 'bkdata', 'public', 4, '存储描述'),
  ('modeling', '机器学习', NULL, 1, 'bkdata', 'public', 3, '机器学习描述'),
  ('processing', '数据处理', NULL, 1, 'bkdata', 'public', 2, '数据处理描述'),
  ('model', 'ModelFlow模型', 'modeling', 1, 'bkdata', 'public', 1, 'ModelFlow模型'),
  ('process_model', '单指标异常检测', 'modeling', 1, 'bkdata', 'public', 2, '单指标异常检测'),
  ('batch', '离线处理节点', 'processing', 1, 'bkdata', 'public', 2, '离线计算'),
  ('stream', '实时处理节点', 'processing', 1, 'bkdata', 'public', 1, '实时计算'),
  ('tdw_batch', '离线处理节点', 'processing', 1, 'bkdata', 'public', 4, 'TDW离线计算'),
  ('transform', '数据传输节点', 'processing', 1, 'bkdata', 'public', 3, '转换计算'),
  ('kv_source', '静态关联数据源', 'source', 1, 'bkdata', 'public', 3, '关联数据源'),
  ('tdw_source', 'TDW数据源', 'source', 1, 'bkdata', 'public', 4, 'TDW数据源'),
  ('batch_source', '离线数据源', 'source', 1, 'bkdata', 'public', 2, '离线数据源'),
  ('stream_source', '实时数据源', 'source', 1, 'bkdata', 'public', 1, '实时数据源'),
  ('mq', '队列存储', 'storage', 1, 'bkdata', 'public', 1, '队列存储'),
  ('tdw', 'TDW存储', 'storage', 1, 'bkdata', 'public', 8, 'TDW存储'),
  ('olap', '在线分析处理', 'storage', 1, 'bkdata', 'public', 4, '在线分析处理'),
  ('oltp', '在线事务处理', 'storage', 1, 'bkdata', 'public', 3, '在线事务处理'),
  ('tsdb', '时序数据库', 'storage', 1, 'bkdata', 'public', 7, '时序数据库'),
  ('search', '搜索型存储', 'storage', 1, 'bkdata', 'public', 5, '搜索型存储'),
  ('key_value', '键值对存储', 'storage', 1, 'bkdata', 'public', 2, '键值对存储'),
  ('filesystem', '文件系统', 'storage', 1, 'bkdata', 'public', 6, '文件系统'),
  ('stream_sql', '实时SQL节点', 'stream', 1, 'bkdata', 'public', 1, '实时SQL节点'),
  ('stream_sdk', '实时SDK节点', 'stream', 1, 'bkdata', 'public', 1, '实时SDK节点');


-- 规则类型命名表

CREATE TABLE IF NOT EXISTS `dataflow_rule_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '规则组标识',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow规则类型命名表';

/*!40000 ALTER TABLE `dataflow_rule_type` DISABLE KEYS */;

INSERT IGNORE INTO `dataflow_rule_type` (`id`, `description`)
VALUES
  (1,'数据流连线规则');

-- 节点类型连线规则表

CREATE TABLE IF NOT EXISTS `dataflow_node_type_rule` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一标识',
  `upstream_node_type_name` varchar(255) DEFAULT NULL COMMENT '上游节点类型',
  `downstream_node_type_name` varchar(255) NOT NULL COMMENT '有效下游节点类型',
  `downstream_support_alone` tinyint(1) NOT NULL COMMENT '下游规则是否支持独立存在，0表示需要结合下游组合规则',
  `upstream_support_alone` tinyint(1) NOT NULL COMMENT '上游规则是否支持独立存在，0表示需要结合上游组合规则',
  `channel_type` varchar(255) DEFAULT NULL COMMENT '需要的channel存储，可选NULL|stream|batch，支持多个，用逗号分隔',
  `max_downstream_instance_num` int(11) NOT NULL COMMENT '当前上游节点类型下，每个rt最大下游节点数量',
  `min_downstream_instance_num` int(11) NOT NULL COMMENT '当前上游节点类型下，每个rt最小下游节点数量',
  `max_upstream_instance_num` int(11) NOT NULL COMMENT '当前下游节点类型下，每个rt最大上游节点数量',
  `min_upstream_instance_num` int(11) NOT NULL COMMENT '当前下游节点类型下，每个rt最小上游节点数量',
  `rule_type` int(11) NOT NULL COMMENT '定义规则的类型，上下游节点类型、规则类型形成一条连线规则',
  `available` tinyint(1) DEFAULT '1' COMMENT '是否启用该规则',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `node_type_rule_uniq1` (`upstream_node_type_name`,`downstream_node_type_name`,`rule_type`),
  KEY `rule_type_fk_rule_type_id` (`rule_type`),
  KEY `downstream_node_type_name_fk_node_type_config_node_type_name` (`downstream_node_type_name`),
  CONSTRAINT `downstream_node_type_name_fk_node_type_config_node_type_name` FOREIGN KEY (`downstream_node_type_name`) REFERENCES `dataflow_node_type_config` (`node_type_name`),
  CONSTRAINT `rule_type_fk_rule_type_id` FOREIGN KEY (`rule_type`) REFERENCES `dataflow_rule_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点类型连线规则表';

-- 通过取交集的方式获取该节点类型连线规则下的规则配置，若available 为1，每条规则的实例规则都是正确的(除非在实例中设置)
-- 1.
-- available 不可有交叉关系，假如 AA 的 父为 A
-- 不可设置这样的规则：B 可连 A，B不可连 AA
-- 2.
-- 首先一般配置 * + 下游节点类型(配置无视下游节点类型的最大/小上游实例数量，不配置默认都为1)，再配置具体上游节点类型
INSERT INTO `dataflow_node_type_rule` (`id`, `upstream_node_type_name`, `downstream_node_type_name`, `downstream_support_alone`, `upstream_support_alone`, `channel_type`, `max_downstream_instance_num`, `min_downstream_instance_num`, `max_upstream_instance_num`, `min_upstream_instance_num`, `rule_type`, `available`, `description`)
VALUES
  (1, 'kv_source', 'stream_sql', 1, 0, NULL, 10, 1, 1, 1, 1, 1, 'xxx'),
  -- 不要求 channel_type，类比于若下游是存储，才需要 kafka
  (2, 'stream_source', 'stream', 1, 1, NULL, 50, 1, 2, 1, 1, 1, 'xxx'),
  (3, 'stream_source', 'stream_sql', 1, 1, NULL, 50, 1, 2, 1, 1, 1, 'xxx'),
  (4, 'stream_source', 'model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (5, 'batch_source', 'batch', 1, 1, NULL, 50, 1, 10, 1, 1, 1, 'xxx'),
  (6, 'batch_source', 'model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  -- 虽然44已包含，但为了作为组合规则的一部分
  (7, 'stream_sql', 'stream_sql', 1, 1, NULL, 50, 1, 2, 1, 1, 1, 'xxx'),
  (8, 'stream', 'batch', 1, 1, 'stream,batch', 50, 1, 10, 1, 1, 1, 'xxx'),
  (9, 'transform', 'stream', 1, 1, 'stream', 50, 1, 2, 1, 1, 1, 'xxx'),
  (10, 'stream', 'transform', 1, 1, 'stream', 50, 1, 1, 1, 1, 1, 'xxx'),
  (11, 'stream', 'storage', 1, 1, 'stream', 1, 1, 1, 1, 1, 1, 'xxx'),
  (12, 'batch', 'batch', 1, 1, 'batch', 50, 1, 10, 1, 1, 1, 'xxx'),
  (13, 'stream_source', 'batch', 1, 1, NULL, 10, 1, 10, 1, 1, 1, '实时数据源之前可以连接离线计算，如今禁止'),
  (14, 'batch', 'storage', 1, 1, 'stream,batch', 1, 1, 1, 1, 1, 1, 'xxx'),
  (15, 'batch', 'filesystem', 1, 1, NULL, 1, 1, 1, 1, 1, 1, 'xxx'),
  (16, 'transform', 'storage', 1, 1, NULL, 1, 1, 1, 1, 1, 1, 'xxx'),
  (17, 'filesystem', 'batch', 1, 1, NULL, 50, 1, 10, 1, 1, 1, 'xxx'),
  (18, '*', 'source', 1, 1, NULL, -1, -1, 0, 0, 1, 1, '配置无视下游节点类型的最大/小上游实例数量'),
  (19, '*', 'stream', 1, 1, NULL, -1, -1, 2, 1, 1, 1, '配置无视下游节点类型的最大/小上游实例数量'),
  (20, '*', 'batch', 1, 1, NULL, -1, -1, 10, 1, 1, 1, '配置无视下游节点类型的最大/小上游实例数量'),
  (21, '*', 'model', 1, 1, NULL, -1, -1, 1, 1, 1, 1, '配置无视下游节点类型的最大/小上游实例数量'),
  (22, '*', 'transform', 1, 1, NULL, -1, -1, 10, 1, 1, 1, '配置无视下游节点类型的最大/小上游实例数量'),
  (23, '*', 'storage', 1, 1, NULL, -1, -1, 1, 1, 1, 1, '配置无视下游节点类型的最大/小上游实例数量'),
  (24, NULL, 'source', 1, 1, NULL, -1, -1, -1, -1, 1, 1, '数据源允许作为初始节点'),
  (25, 'model', 'model', 1, 1, NULL, 10, 1, 1, 1, 1, 1, 'xxx'),
  (26, 'stream_source', 'process_model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (27, 'batch_source', 'process_model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (28, 'stream', 'process_model', 1, 1, 'stream', 50, 1, 1, 1, 1, 1, 'xxx'),
  (29, 'batch', 'process_model', 1, 1, 'batch', 50, 1, 1, 1, 1, 1, 'xxx'),
  (30, '*', 'process_model', 1, 1, NULL, -1, -1, 1, 1, 1, 1, 'xxx'),
  (31, 'process_model', 'process_model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (32, 'tdw_batch', 'tdw_batch', 1, 1, 'tdw_batch', 50, 1, 3, 1, 1, 1, 'xxx'),
  (33, 'tdw_source', 'tdw_batch', 1, 1, NULL, 50, 1, 3, 1, 1, 1, 'xxx'),
  (34, 'process_model', 'model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (35, 'tdw', 'tdw_batch', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (36, 'stream', 'tdw', 1, 1, 'stream', 50, 1, 1, 1, 1, 1, 'xxx'),
  (37, 'filesystem', 'process_model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (38, 'oltp', 'model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (39, '*', 'tdw', 1, 1, NULL, -1, -1, 1, 1, 1, 1, 'xxx'),
  (40, '*', 'tdw_batch', 1, 1, NULL, -1, -1, 10, 1, 1, 1, 'xxx'),
  (41, 'tdw_batch', 'tdw', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (42, '*', 'stream_sdk', 1, 1, 'stream', -1, -1, 1, 1, 1, 1, 'xxx'),
  (43, 'stream', 'stream_sdk', 1, 1, 'stream', 50, 1, 2, 1, 1, 1, 'xxx'),
  (44, 'stream', 'stream_sql', 1, 1, NULL, 50, 1, 2, 1, 1, 1, 'xxx'),
  (45, 'stream_source', 'storage', 1, 1, 'stream', 1, 1, 1, 1, 1, 1, 'xxx'),
  (46, 'stream_source', 'transform', 1, 1, 'stream', 50, 1, 1, 1, 1, 1, 'xxx'),
  (47, 'kv_source', 'batch', 1, 0, NULL, 10, 1, 1, 1, 1, 1, 'xxx'),
  (48, 'model', 'oltp', 1, 1, NULL, 1, 1, 1, 1, 1, 1, 'xxx'),
  (49, 'model', 'mq', 1, 1, NULL, 1, 1, 1, 1, 1, 1, 'xxx'),
  (50, 'model', 'process_model', 1, 1, NULL, 50, 1, 1, 1, 1, 1, 'xxx'),
  (51, 'model', 'filesystem', 1, 1, NULL, 1, 1, 1, 1, 1, 1, 'xxx'),
  (52, 'process_model', 'storage', 1, 1, NULL, 1, 1, 1, 1, 1, 1, 'xxx');


-- 规则组命名表

CREATE TABLE IF NOT EXISTS `dataflow_node_type_group` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一组标识',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow规则组命名表';

/*!40000 ALTER TABLE `dataflow_node_type_group` DISABLE KEYS */;

INSERT IGNORE INTO `dataflow_node_type_group` (`id`, `description`)
VALUES
  (1,'kv_source规则组合1'),
  (2,'kv_source规则组合2'),
  (3,'batch_kv_source规则组合1'),
  (4,'batch_kv_source规则组合2');

-- 规则组表

CREATE TABLE IF NOT EXISTS `dataflow_node_type_group_rule` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一标识',
  `rule_id` int(11) NOT NULL COMMENT '规则ID',
  `group_id` int(11) NOT NULL COMMENT '规则组合ID',
  `rule_direction` enum('upstream','downstream') NOT NULL COMMENT '规则的组合可以是一种类型的规则组合，也可以是向上和向下的规则组合',
  `max_instance_num` int(11) NOT NULL COMMENT '指定rule_direction规则下最大节点数量',
  `min_instance_num` int(11) NOT NULL COMMENT '指定rule_direction规则下最小节点数量',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `node_type_group_rule_uniq1` (`rule_id`,`group_id`,`rule_direction`),
  KEY `group_id_fk_node_type_group_id` (`group_id`),
  CONSTRAINT `group_id_fk_node_type_group_id` FOREIGN KEY (`group_id`) REFERENCES `dataflow_node_type_group` (`id`),
  CONSTRAINT `rule_id_fk_node_type_rule_id` FOREIGN KEY (`rule_id`) REFERENCES `dataflow_node_type_rule` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow规则组表';


INSERT IGNORE INTO `dataflow_node_type_group_rule` (`rule_id`, `group_id`, `rule_direction`, `max_instance_num`, `min_instance_num`, `description`)
VALUES
  (1,1,'upstream',1,1,'kv, stream'),
  (7,1,'upstream',1,1,'stream, stream'),
  (1,2,'upstream',1,1,'kv, stream'),
  (3,2,'upstream',1,1,'stream_source, stream'),

  (47,3,'upstream',1,1,'batch_kv, batch'),
  (12,3,'upstream',10,1,'batch, batch'),
  (47,4,'upstream',1,1,'batch_kv, batch'),
  (5,4,'upstream',10,1,'batch_source, batch'); 


-- 节点实例表

CREATE TABLE IF NOT EXISTS `dataflow_node_type_instance_config` (
  `node_type_instance_name` varchar(255) NOT NULL COMMENT '节点类型实例，如：stream',
  `node_type_instance_alias` varchar(255) NOT NULL COMMENT '节点类型实例名称，如：实时节点',
  `node_type_name` varchar(255) NOT NULL COMMENT '节点类型',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `available` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否启用该节点类型实例',
  `belong_to` varchar(128) DEFAULT 'bkdata' COMMENT '这个字段标记是系统的(bkdata)还是用户的(other)',
  `scope` enum('private','public') NOT NULL DEFAULT 'public' COMMENT '标记节点类型的权限范围，可选有public|private',
  `order` int(11) NOT NULL COMMENT '节点实例全局排序',
  `support_debug` tinyint(1) NOT NULL COMMENT '是否支持调试',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`node_type_instance_name`),
  KEY `node_type_name_fk_node_type_config_node_type_name` (`node_type_name`),
  CONSTRAINT `node_type_name_fk_node_type_config_node_type_name` FOREIGN KEY (`node_type_name`) REFERENCES `dataflow_node_type_config` (`node_type_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点类型实例配置表';



INSERT INTO `dataflow_node_type_instance_config` (`node_type_instance_name`, `node_type_instance_alias`, `node_type_name`, `created_by`, `created_at`, `updated_by`, `updated_at`, `available`, `belong_to`, `scope`, `order`, `support_debug`, `description`)
VALUES
  ('merge', '合流计算', 'transform', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02', 1, 'bkdata', 'public', 204, 0, '支持表结构相同的多个数据源节点合并成为一个结果数据表'),
  ('split', '分流计算', 'transform', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02', 1, 'bkdata', 'public', 203, 0, '支持一个数据源根据业务维度切分成若干数据结构相同的结果数据表'),
  ('stream_source', '实时数据源', 'stream_source', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02', 1, 'bkdata', 'public', 101, 1, '清洗、实时计算的结果数据，数据延迟低，可用于分钟级别的实时统计计算'),
  ('stream', '实时计算', 'stream_sql', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02', 1, 'bkdata', 'public', 201, 1, '基于流式处理的实时计算,支持秒级和分钟级的计算'),
  ('elastic_storage', 'Elasticsearch（全文检索、分析型存储）', 'search', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2019-09-23 16:15:00', 1, 'bkdata', 'public', 403, 0, '<b>建议使用场景：</b><br> 全文检索及数据分析<br> <b>建议日数据量[单表]：</b><br> GB/TB级 <br><b>查询模式：</b><br> 检索查询（支持分词）'),
  ('process_model', '单指标异常检测', 'process_model', 'dataflow', '2019-09-23 16:21:02', 'dataflow', '2019-09-23 22:03:27', 1, 'bkdata', 'public', 302, 0, '机器学习模型，用于时间序列数据的单指标异常检测'),
  ('tspider_storage', 'TSpider（关系型数据库）', 'oltp', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2019-09-23 16:15:30', 1, 'bkdata', 'public', 406, 1, '<b>建议使用场景：</b><br> 关系型数据库<br> <b>建议日数据量[单表]：</b><br> GB/千万级以内 <br><b>查询模式：</b><br> 点查询/关联查询/聚合查询'),
  ('queue_storage', 'Queue（消息队列）', 'mq', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2019-09-23 16:14:46', 1, 'bkdata', 'public', 409, 0, '<b>建议使用场景：</b><br> 数据订阅<br> <b>建议日数据量[单表]：</b><br> GB/TB <br><b>查询模式：</b><br> 客户端连接消费'),
  ('kv_source', '实时关联数据源', 'kv_source', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02', 1, 'bkdata', 'public', 102, 0, '在K-V存储的状态数据，可用作维度表与实时数据join'),
  ('hdfs_storage', 'HDFS（分布式文件系统）', 'filesystem', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2019-09-23 16:14:49', 1, 'bkdata', 'public', 411, 0, '<b>建议使用场景：</b><br> 海量数据离线分析，对查询时延要求不高<br> <b>建议日数据量[单表]：</b><br> TB/PB <br><b>查询模式：</b><br> 可对接离线分析/即席查询/数据分析等方式'),
  ('batch_source', '离线数据源', 'batch_source', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02', 1, 'bkdata', 'public', 103, 1, '落地到分布式文件系统的数据，可用于小时级以上的离线统计计算'),
  ('batch', '离线计算', 'batch', 'dataflow', '2018-12-25 19:15:02', 'dataflow', '2018-12-25 19:15:02', 1, 'bkdata', 'public', 202, 1, '基于批处理的离线计算，支持小时级和天级的计算');


-- 节点实例自己的连线规则表，为实例增加其它连线规则
-- 并非每个节点实例都需要配置，目前这个表为空

CREATE TABLE IF NOT EXISTS `dataflow_node_type_instance_rule` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一标识',
  `upstream_node_type_instance_name` varchar(255) DEFAULT NULL COMMENT '节点类型实例，如：stream',
  `downstream_node_type_instance_name` varchar(255) NOT NULL COMMENT '节点类型实例允许的下游节点类型实例，如：stream',
  `max_downstream_instance_num` int(11) NOT NULL COMMENT '当前上游节点类型下，每个rt最大下游节点数量',
  `min_downstream_instance_num` int(11) NOT NULL COMMENT '当前上游节点类型下，每个rt最小下游节点数量',
  `max_upstream_instance_num` int(11) NOT NULL COMMENT '当前下游节点类型下，每个rt最大上游节点数量',
  `min_upstream_instance_num` int(11) NOT NULL COMMENT '当前下游节点类型下，每个rt最小上游节点数量',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `rule_type` int(11) NOT NULL COMMENT '定义规则的类型，上下游节点类型、规则类型形成一条连线规则',
  `available` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否启用该规则',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE KEY `node_type_instance_rule_uniq1` (`upstream_node_type_instance_name`,`downstream_node_type_instance_name`,`rule_type`),
  KEY `node_type_instance_rule_fk_rule_type_id` (`rule_type`),
  KEY `downstream_node_instance_fk_instance_config_node_type_instance` (`downstream_node_type_instance_name`),
  CONSTRAINT `node_type_instance_rule_fk_rule_type_id` FOREIGN KEY (`rule_type`) REFERENCES `dataflow_rule_type` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点实例连线规则表';

-- 最小上游节点数量先基于通配配置的判断(min_upstream_num1)，再基于具体规则判断(若有)(min_upstream_num2)，且 min_upstream_num1 >= min_upstream_num2，如下述的 merge
INSERT INTO `dataflow_node_type_instance_rule` (`id`, `upstream_node_type_instance_name`, `downstream_node_type_instance_name`, `max_downstream_instance_num`, `min_downstream_instance_num`, `max_upstream_instance_num`, `min_upstream_instance_num`, `created_by`, `created_at`, `updated_by`, `updated_at`, `rule_type`, `available`, `description`)
VALUES
  (1, 'stream', 'merge', 50, 1, 10, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2018-12-25 19:36:17', 1, 1, 'xxx'),
  (2, 'pgsql_storage', 'model', 50, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, 'xxx'),
  (3, 'tdw_batch', 'tdw_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (4, 'tdw_jar_batch', 'tdw_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (5, 'tpg', 'tdw_batch', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (6, 'tpg', 'tdw_jar_batch', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (7, 'stream', 'tpg', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (8, 'spark_structured_streaming', 'tpg', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (9, 'flink_streaming', 'tpg', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (10, 'batch', 'tpg', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (11, 'batch', 'tdw_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (12, 'merge', 'tpg', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (13, 'split', 'tpg', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (14, 'split', 'elastic_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (16, 'split', 'pgsql_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (17, 'split', 'tsdb_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (18, 'split', 'queue_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (19, 'split', 'tspider_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (20, 'split', 'druid_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (21, 'split', 'tdbank', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (22, 'split', 'tredis_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (23, 'split', 'hdfs_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (24, 'split', 'tdw_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (25, 'split', 'hermes_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (26, 'stream_source', 'tdw_storage', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (27, 'stream_source', 'tpg', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (28, 'split', 'stream', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (29, 'split', 'flink_streaming', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (30, 'split', 'spark_structured_streaming', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (31, 'flink_streaming', 'merge', 50, 1, 10, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2018-12-25 19:36:17', 1, 1, 'xxx'),
  (32, 'spark_structured_streaming', 'merge', 50, 1, 10, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2018-12-25 19:36:17', 1, 1, 'xxx'),
  (33, 'stream_source', 'merge', 50, 1, 10, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2018-12-25 19:36:17', 1, 1, 'xxx'),
  (34, '*', 'merge', -1, -1, 10, 2, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2018-12-25 19:36:17', 1, 1, 'xxx'),
  (35, 'kv_source', 'batch', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (36, 'model', 'tdbank', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (37, 'batch_kv_source', 'stream', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则'),
  (38, 'split', 'queue_pulsar', 10, 1, 1, 1, 'dataflow', '2018-12-25 19:35:02', 'dataflow', '2019-09-23 17:27:02', 1, 0, '禁止这条规则');  

update `dataflow_node_type_rule` set `upstream_support_alone` = 1, `max_upstream_instance_num` = 10 where `upstream_node_type_name`='kv_source' and `downstream_node_type_name`='batch';