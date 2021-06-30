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
 
-- 数据探索相关表

CREATE DATABASE IF NOT EXISTS bkdata_lab;

USE bkdata_lab;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS `datalab_default_project_info` (
  `project_id` int(7) NOT NULL AUTO_INCREMENT COMMENT '项目Id',
  `project_name` varchar(255) NOT NULL COMMENT '项目名称',
  `project_type` varchar(128) NOT NULL default 'personal' COMMENT '项目类型, personal或者common',
  `bind_to` varchar(128) NOT NULL COMMENT '绑定的用户名或者公共项目Id',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`project_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据探索临时项目表';

CREATE TABLE IF NOT EXISTS `datalab_query_task_info` (
  `query_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '查询任务Id',
  `query_name` varchar(255) NOT NULL COMMENT '查询任务名称',
  `project_id` int(7) NOT NULL  COMMENT '所属项目Id',
  `project_type` varchar(128) NOT NULL default 'personal' COMMENT '项目类型, personal或者common',
  `sql_text` text NOT NULL COMMENT 'sql脚本',
  `query_task_id` varchar(64) NOT NULL COMMENT 'sql查询作业id',
  `lock_user` varchar(128) NOT NULL default '' COMMENT '占用查询任务的用户名',
  `lock_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '查询任务被占用的时刻',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`query_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询任务表';

CREATE TABLE IF NOT EXISTS `datalab_notebook_task_info` (
  `notebook_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '笔记任务Id',
  `notebook_name` varchar(255) NOT NULL COMMENT '笔记任务名称',
  `project_id` int(7) NOT NULL  COMMENT '所属项目Id',
  `project_type` varchar(128) NOT NULL default 'personal' COMMENT '项目类型, personal或者common',
  `notebook_url` varchar(1000) NOT NULL COMMENT 'notebook地址',
  `content_name` varchar(255) NOT NULL COMMENT 'notebook名称',
  `kernel_type` varchar(128) NOT NULL DEFAULT 'Python3' COMMENT 'notebook内核服务类型',
  `lock_user` varchar(128) NOT NULL default '' COMMENT '占用笔记任务的用户名',
  `lock_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '笔记任务被占用的时刻',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`notebook_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='笔记任务表';

CREATE TABLE IF NOT EXISTS  `datalab_favorite_result_table` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `project_id` int(7) NOT NULL COMMENT '所属项目Id',
  `project_type` varchar(128) NOT NULL default 'personal' COMMENT '项目类型, personal或者common',
  `result_table_id` varchar(255) NOT NULL COMMENT '置顶结果表id',
  `active` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否是有效记录 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据探索用户置顶结果表';

CREATE TABLE IF NOT EXISTS `datalab_history_task_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
  `query_id` int(11) NOT NULL COMMENT '历史查询任务Id',
  `query_task_id` varchar(64) NOT NULL COMMENT 'sql查询作业id',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询任务历史表';

CREATE TABLE IF NOT EXISTS `datalab_query_task_chart` (
  `chart_id` int(11) NOT NULL AUTO_INCREMENT COMMENT '图表Id',
  `chart_name`  varchar(255) NOT NULL COMMENT '图表名称',
  `chart_type`  varchar(32) NOT NULL COMMENT '图表类型',
  `chart_config` text NOT NULL COMMENT '图表配置',
  `task_id` int(11) NOT NULL COMMENT '关联的查询任务Id',
  `active` tinyint(1)  NOT NULL DEFAULT 1 COMMENT '记录是否有效 0：失效，1：有效',
  `created_at` timestamp  NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '创建时间',
  `created_by` varchar(128) NOT NULL default '' COMMENT '创建人',
  `updated_at` timestamp NOT NULL ON UPDATE CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `updated_by` varchar(128) NOT NULL default ''  COMMENT '修改人',
  `description` text NOT NULL  COMMENT '备注信息',
  PRIMARY KEY (`chart_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='查询任务可视化配置表';
