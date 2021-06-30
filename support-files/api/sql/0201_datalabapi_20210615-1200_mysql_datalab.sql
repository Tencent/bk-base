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
 
USE bkdata_lab;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `datalab_notebook_output` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `notebook_id` int(11) NOT NULL COMMENT '笔记任务Id',
  `cell_id` varchar(64) NOT NULL COMMENT 'cell Id',
  `output_type` varchar(64) NOT NULL COMMENT '产出物类型，model或result_table',
  `output_foreign_id` varchar(64) NOT NULL COMMENT '产出物外键关联Id，output_type和output_foreign_id对应关系为：model/model_id，result_table/result_table_id',
  `sql` text NOT NULL COMMENT '生成产出物的sql文本',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`),
  UNIQUE (`output_type`, `output_foreign_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='笔记产出物表';

CREATE TABLE IF NOT EXISTS `datalab_notebook_execute_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `notebook_id` int(11) NOT NULL COMMENT '笔记任务id',
  `cell_id` int(11) NOT NULL COMMENT 'cell序号',
  `code_text` text NOT NULL COMMENT '代码文本',
  `stage_start_time` timestamp(3) NOT NULL default '0000-00-00 00:00:00.000' COMMENT '提交时间',
  `stage_end_time` timestamp(3) NOT NULL default '0000-00-00 00:00:00.000' COMMENT '结束时间',
  `stage_status` varchar(32)  NOT NULL default 'created' COMMENT 'stage状态 success：成功，failed：失败',
  `error_message` varchar(1000) NOT NULL default '' COMMENT '异常信息',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='笔记执行信息表';

CREATE TABLE IF NOT EXISTS `datalab_notebook_mlsql_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `notebook_id` int(11) NOT NULL COMMENT '笔记任务Id',
  `kernel_id` varchar(128) NOT NULL COMMENT 'kernel Id',
  `execute_time` timestamp(3) NOT NULL default '0000-00-00 00:00:00.000' COMMENT '提交时间',
  `sql` text NOT NULL COMMENT 'sql文本',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='MLSql执行信息表';

CREATE TABLE IF NOT EXISTS `datalab_bksql_function_config` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `sql_type` varchar(255) NOT NULL COMMENT 'mysql, druid',
  `func_name` varchar(255) NOT NULL,
  `func_alias` varchar(255) NOT NULL,
  `func_group` varchar(255) NOT NULL COMMENT 'string-functions,',
  `usage` varchar(255) NOT NULL COMMENT 'sqrt(DOUBLE num)',
  `params` varchar(255) COMMENT 'ARG1,ARG2,...',
  `explain` text,
  `version` varchar(255) DEFAULT NULL,
  `support_framework` varchar(255) DEFAULT 'query' COMMENT 'stream,batch,query',
  `support_storage` varchar(255) DEFAULT 'HDFS' COMMENT 'HDFS Druid..',
  `active` tinyint(1) DEFAULT '1',
  `example` text,
  `example_return_value` text,
  `created_by` varchar(50) NOT NULL DEFAULT '',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `description` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数表';

CREATE TABLE IF NOT EXISTS `datalab_notebook_report_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `notebook_id` int(11) NOT NULL COMMENT '笔记任务Id',
  `report_secret` varchar(128) NOT NULL COMMENT '报告秘钥',
  `report_config` text NOT NULL COMMENT '报告配置',
  `content_name` varchar(255) NOT NULL COMMENT '报告关联的笔记名称',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`),
  Foreign KEY(notebook_id) references datalab_notebook_task_info(notebook_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='笔记报告信息表';


-- 流水日志
CREATE DATABASE IF NOT EXISTS bkdata_log;

USE bkdata_log;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `datalab_es_query_history` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `result_table_id` varchar(255) NOT NULL COMMENT '查询结果表Id',
  `search_range_start_time` timestamp(3) NOT NULL default '0000-00-00 00:00:00.000' COMMENT '查询开始时间',
  `search_range_end_time` timestamp(3) NOT NULL default '0000-00-00 00:00:00.000' COMMENT '查询结束时间',
  `keyword` text NOT NULL COMMENT '搜索关键字',
  `time_taken` varchar(32) NOT NULL default 0 COMMENT '查询耗时',
  `total` bigint(19) NOT NULL default 0 COMMENT '查询结果条数',
  `result` boolean NOT NULL default true COMMENT '查询结果',
  `err_msg` text COMMENT '异常信息',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='es查询流水表';
