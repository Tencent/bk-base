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

SET NAMES utf8;

USE bkdata_meta;

CREATE TABLE IF NOT EXISTS  `tag_type_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `name` varchar(32) NOT NULL COMMENT '标签类型名称',
  `alias` varchar(32) NOT NULL COMMENT '标签类型中文',
  `seq_index` int(11) NOT NULL COMMENT '位置',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `description` text COMMENT '描述信息',
  `created_by` varchar(64) NOT NULL COMMENT '创建者',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(64) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COMMENT ='标签类型配置';


CREATE TABLE IF NOT EXISTS  `datamap_layout` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `category` varchar(64) NOT NULL COMMENT '分类的名称',
  `loc` tinyint(1) NOT NULL COMMENT '页面展示位置: 0-left,1-right;',
  `seq_index` int(11) NOT NULL,
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `config` text COMMENT '显示配置信息（JSON格式）比如显示颜色，显示大小等信息',
  `description` text COMMENT '描述信息',
  `created_by` varchar(64) DEFAULT NULL COMMENT '创建者',
  `created_at` timestamp NULL DEFAULT NULL COMMENT '创建时间',
  `updated_by` varchar(64) DEFAULT NULL COMMENT '更新者',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8 COMMENT='数据地图-页面显示相关内容的配置，比如节点左右位置、失效信息等等';


CREATE TABLE IF NOT EXISTS  `dm_standard_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '标准id',
  `standard_name` varchar(128) NOT NULL COMMENT '标准名称',
  `description` text COMMENT '标准描述',
  `category_id` int(11) NOT NULL COMMENT '所属分类',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_standard_name_category_id` (`standard_name`,`category_id`)
) ENGINE=InnoDB AUTO_INCREMENT=222 DEFAULT CHARSET=utf8 COMMENT='数据标准总表';

CREATE TABLE IF NOT EXISTS  `dm_standard_version_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '版本id',
  `standard_id` int(11) NOT NULL COMMENT '关联的dm_standard_config表id',
  `standard_version` varchar(128) NOT NULL COMMENT '标准版本号,例子:v1.0,v2.0...',
  `description` text COMMENT '版本描述',
  `standard_version_status` varchar(32) NOT NULL COMMENT '版本状态:developing/tobeonline/online/tobeoffline/offline',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_standard_id_standard_version` (`standard_id`,`standard_version`)
) ENGINE=InnoDB AUTO_INCREMENT=488 DEFAULT CHARSET=utf8 COMMENT='数据标准版本表';

CREATE TABLE IF NOT EXISTS  `dm_standard_content_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '标准内容id',
  `standard_version_id` int(11) NOT NULL COMMENT '关联的dm_standard_version_config表id',
  `standard_content_name` varchar(128) NOT NULL COMMENT '标准内容名称',
  `parent_id` varchar(256) NOT NULL COMMENT '父表id,格式例子:[1,2],明细数据标准为[]',
  `source_record_id` int(11) NOT NULL COMMENT '来源记录id',
  `standard_content_sql` text,
  `category_id` int(11) NOT NULL COMMENT '所属分类',
  `standard_content_type` varchar(128) NOT NULL COMMENT '标准内容类型[detaildata/indicator]',
  `description` text COMMENT '标准内容描述',
  `window_period` text COMMENT '窗口类型,以秒为单位,json表达定义',
  `filter_cond` text COMMENT '过滤条件',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_standard_content_name_standard_version_id` (`standard_content_name`,`standard_version_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1275 DEFAULT CHARSET=utf8 COMMENT='数据标准内容表';

CREATE TABLE IF NOT EXISTS  `dm_detaildata_field_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `standard_content_id` int(11) NOT NULL COMMENT '关联的dm_standard_content_config的id',
  `source_record_id` int(11) NOT NULL COMMENT '来源记录id',
  `field_name` varchar(128) NOT NULL COMMENT '字段英文名',
  `field_alias` varchar(128) DEFAULT NULL COMMENT '字段中文名',
  `field_type` varchar(128) NOT NULL COMMENT '数据类型',
  `field_index` int(11) NOT NULL COMMENT '字段在数据集中的顺序',
  `unit` varchar(128) DEFAULT NULL COMMENT '单位',
  `description` text COMMENT '备注',
  `constraint_id` int(11) DEFAULT NULL COMMENT '关联的值约束配置表id',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4387 DEFAULT CHARSET=utf8 COMMENT='明细数据标准字段详情表';

CREATE TABLE IF NOT EXISTS  `dm_indicator_field_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `standard_content_id` int(11) NOT NULL COMMENT '关联的dm_standard_content_config的id',
  `source_record_id` int(11) NOT NULL COMMENT '来源记录id',
  `field_name` varchar(128) NOT NULL COMMENT '字段英文名',
  `field_alias` varchar(128) DEFAULT NULL COMMENT '字段中文名',
  `field_type` varchar(128) NOT NULL COMMENT '数据类型',
  `is_dimension` tinyint(1) DEFAULT '1' COMMENT '是否维度:0:否;1:是;',
  `add_type` varchar(128) DEFAULT NULL COMMENT '可加性:yes完全可加;half:部分可加;no:不可加;',
  `unit` varchar(128) DEFAULT NULL COMMENT '单位',
  `field_index` int(11) NOT NULL COMMENT '字段在数据集中的顺序',
  `constraint_id` int(11) DEFAULT NULL COMMENT '关联的值约束配置表id',
  `compute_model_id` int(11) NOT NULL DEFAULT '0' COMMENT '计算方式id',
  `description` text COMMENT '备注',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4665 DEFAULT CHARSET=utf8 COMMENT='原子指标字段详情表';

CREATE TABLE IF NOT EXISTS  `dm_constraint_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `constraint_name` varchar(128) NOT NULL COMMENT '约束名称',
  `rule` text NOT NULL COMMENT '约束条件',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=501 DEFAULT CHARSET=utf8 COMMENT='支持的值约束配置表';

CREATE TABLE IF NOT EXISTS  `dm_unit_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `name` varchar(64) NOT NULL COMMENT '单位英文名',
  `alias` varchar(64) NOT NULL COMMENT '单位中文名',
  `category_name` varchar(64) NOT NULL COMMENT '单位类目英文名',
  `category_alias` varchar(64) NOT NULL COMMENT '单位类目中文名',
  `description` text COMMENT '描述',
  `created_by` varchar(64) NOT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(64) DEFAULT NULL COMMENT '更新人',
  `updated_at` timestamp NULL DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS  `dm_schema_tmp_config` (
  `bk_table_id` varchar(128) NOT NULL COMMENT '随机生成的表id,生成规则:bk_table_YYYYMMDDHHMMSS_随机数',
  `schema` text NOT NULL COMMENT 'schema信息[json串形式]',
  `bk_sql` text NOT NULL COMMENT 'bksql',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`bk_table_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='原子指标值临时表';

CREATE TABLE IF NOT EXISTS  `dm_task_detail` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `task_id` int(11) NOT NULL COMMENT '关联的dm_task_config表id',
  `task_content_id` int(11) NOT NULL COMMENT '关联dm_task_content_config表id',
  `standard_version_id` int(11) NOT NULL COMMENT '关联的dm_standard_version_config的id',
  `bk_biz_id` int(11) DEFAULT NULL,
  `project_id` int(11) DEFAULT NULL,
  `data_set_type` varchar(128) NOT NULL COMMENT '输入数据集类型:raw_data/result_table',
  `data_set_id` varchar(128) DEFAULT NULL COMMENT '标准化后的数据集id:result_table_id/data_id',
  `task_type` varchar(128) NOT NULL COMMENT '详情类型[detaildata/indicator]',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否有效',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `standard_version_id` (`standard_version_id`,`data_set_id`,`data_set_type`),
  KEY `data_set_type` (`data_set_type`)
) ENGINE=InnoDB AUTO_INCREMENT=1213 DEFAULT CHARSET=utf8 COMMENT='标准化工具内容详情统计表';

CREATE TABLE IF NOT EXISTS  `dm_task_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '任务id',
  `task_name` varchar(128) NOT NULL COMMENT '任务名称',
  `project_id` int(11) NOT NULL COMMENT '所属项目id',
  `standard_version_id` int(11) NOT NULL COMMENT '关联的dm_standard_version_config的id',
  `description` text COMMENT '任务描述',
  `data_set_type` varchar(128) NOT NULL COMMENT '输入数据集类型:raw_data/result_table',
  `data_set_id` varchar(128) DEFAULT NULL COMMENT '输入数据集id:result_table_id, data_id',
  `standardization_type` int(11) NOT NULL COMMENT '标准化类型:0:部分;1:完全;',
  `flow_id` int(11) DEFAULT NULL COMMENT 'flow id',
  `task_status` varchar(128) NOT NULL COMMENT '状态标识：ready/preparing/running/stopping/stopped/succeeded/queued/failed/pending',
  `edit_status` varchar(50) NOT NULL COMMENT '编辑状态:editting/published/reeditting',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=583 DEFAULT CHARSET=utf8 COMMENT='标准化任务总表';

CREATE TABLE IF NOT EXISTS  `dm_task_content_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '标准任务内容id',
  `task_id` int(11) NOT NULL COMMENT '关联的dm_task_config表id',
  `parent_id` varchar(256) NOT NULL COMMENT '父表id,格式例子:[1,2,3...]',
  `standard_version_id` int(11) NOT NULL COMMENT '关联的dm_standard_version_config的id',
  `source_type` varchar(128) NOT NULL COMMENT '标识来源[standard:标准模板sql;user:配置字段]',
  `result_table_id` varchar(128) NOT NULL COMMENT '标准化结果表id',
  `result_table_name` varchar(128) NOT NULL COMMENT '标准化结果表中文名',
  `task_content_name` varchar(128) NOT NULL COMMENT '子流程名称',
  `task_type` varchar(128) NOT NULL COMMENT '任务类型[detaildata/indicator]',
  `task_content_sql` text COMMENT '标准化sql',
  `node_config` text COMMENT '配置详情,json表达,参考dataflow的配置定义',
  `created_by` varchar(50) NOT NULL COMMENT 'created by',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `updated_by` varchar(50) DEFAULT NULL COMMENT 'updated by',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  `request_body` text COMMENT '记录完整的请求信息',
  `flow_body` text COMMENT '记录请求dataflow创建接口的完整请求信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=583 DEFAULT CHARSET=utf8 COMMENT='标准化任务内容定义表';

