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

-- 数据计算相关表

CREATE DATABASE IF NOT EXISTS bkdata_flow;

USE bkdata_flow;

SET NAMES utf8;

CREATE TABLE `dataflow_control_list` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `case` varchar(255) NOT NULL COMMENT '当前功能控制场景',
  `value` varchar(255) NOT NULL COMMENT '列入当前功能控制名单的值',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow功能控制名单';


CREATE TABLE `processing_cluster_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `cluster_domain` varchar(255) DEFAULT 'default' COMMENT '对应组件主节点域名',
  `cluster_group` varchar(255) NOT NULL COMMENT '集群组',
  `cluster_name` varchar(128) NOT NULL COMMENT '集群名称',
  `cluster_label` varchar(128) NOT NULL DEFAULT 'standard' COMMENT '集群标签',
  `priority` int(11) NOT NULL DEFAULT 1 COMMENT '优先级',
  `version` varchar(128) NOT NULL COMMENT '集群版本',
  `belong` varchar(128) DEFAULT 'bkdata' COMMENT '标记这个字段是系统的sys还是用户可见的other',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink spark spark-streaming', 
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据计算集群';


CREATE TABLE `processing_stream_info` (
  `processing_id` varchar(255) NOT NULL COMMENT '数据处理节点ID',
  `stream_id` varchar(255) DEFAULT NULL COMMENT '流处理作业ID',
  `concurrency` int DEFAULT NULL COMMENT '计算并发度',
  `checkpoint_type` varchar(255) DEFAULT NULL COMMENT 'timestamp, offset',
  `window` text COMMENT '窗口信息',
  `processor_type` varchar(255) DEFAULT NULL COMMENT 'common aggragate join static_join',
  `processor_logic` longtext COMMENT '统计方法参数json,{"avg":{"avgField":"xxx"}}',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink spark spark-streaming', 
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`processing_id`),
  KEY(`stream_id`,`processing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='流处理计算任务配置信息';

CREATE TABLE `processing_batch_info` (
  `processing_id` varchar(255) NOT NULL COMMENT '处理节点ID',
  `batch_id` varchar(255) DEFAULT NULL COMMENT '批处理作业ID',
  `processor_type` varchar(255) DEFAULT NULL COMMENT 'sql',
  `processor_logic` text COMMENT '具体的逻辑，如sql语句',
  `schedule_period` varchar(4) NOT NULL COMMENT '调度周期',
  `count_freq` int(11) NOT NULL COMMENT '统计频率',
  `delay` int(11) NOT NULL DEFAULT 0  COMMENT '延迟时间',
  `submit_args` text NOT NULL COMMENT '提交任务时用到的参数',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink spark spark-streaming mr', 
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`processing_id`),
  KEY(`batch_id`,`processing_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='批处理计算任务配置信息';

CREATE TABLE `processing_stream_job` (
  `stream_id` varchar(255) NOT NULL COMMENT '流处理作业ID',
  `running_version` varchar(64) DEFAULT NULL COMMENT '正在运行版本',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink storm-flow storm-yaml spark spark-streaming',
  `jobserver_config` text DEFAULT NULL COMMENT '作业服务配置', 
  `cluster_group` varchar(255) NOT NULL COMMENT '集群组',
  `cluster_name` varchar(255) DEFAULT NULL COMMENT '细分集群名称',
  `heads` text NOT NULL COMMENT 'topo头部',
  `tails` text NOT NULL COMMENT 'topo尾部',
  `status` varchar(255) NOT NULL COMMENT 'starting failed_starting running stop failed_stopping stopping',
  `concurrency` int DEFAULT NULL COMMENT '并发度',
  `deploy_mode` varchar(255) DEFAULT NULL COMMENT 'yarn-cluster yarn-session',
  `deploy_config` text DEFAULT NULL COMMENT '{task_manager_mem_mb:1024}',
  `offset` tinyint(1) DEFAULT 0 COMMENT '0:尾部，1:头部',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`stream_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='流处理计算任务运行表';

CREATE TABLE `processing_batch_job` (
  `batch_id` varchar(255) NOT NULL COMMENT '批处理作业ID',
  `processor_type` varchar(255) DEFAULT NULL COMMENT 'sql',
  `processor_logic` text COMMENT '具体的逻辑，如sql语句',
  `schedule_time` bigint(20) NOT NULL COMMENT '调度时间',
  `schedule_period` varchar(4) NOT NULL COMMENT '调度周期',
  `count_freq` int(11) NOT NULL COMMENT '统计频率',
  `delay` int(11) NOT NULL DEFAULT 0  COMMENT '延迟时间',
  `submit_args` text NOT NULL COMMENT '提交任务时用到的参数',
  `storage_args` text NOT NULL COMMENT '任务需要写入的存储信息',
  `running_version` varchar(64) DEFAULT NULL COMMENT '正在运行版本',
  `jobserver_config` text DEFAULT NULL COMMENT '作业服务配置', 
  `cluster_group` varchar(255) NOT NULL COMMENT '集群组',
  `cluster_name` varchar(255) NOT NULL COMMENT '细分集群名称',
  `deploy_mode` varchar(255) NOT NULL COMMENT 'yarn',
  `deploy_config` text NOT NULL COMMENT '{executer_mem_mb:1024}',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`batch_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='批处理计算任务配置信息';

CREATE TABLE `processing_job_info` (
  `job_id` varchar(255) NOT NULL COMMENT '作业ID',
  `processing_type`  varchar(255) NOT NULL COMMENT 'batch stream algrithon databus adhoc',
  `code_version` varchar(64) DEFAULT NULL COMMENT '代码版本号',
  `component_type` varchar(128) NOT NULL COMMENT 'yarn flink spark spark-streaming', 
  `jobserver_config` text DEFAULT NULL COMMENT '作业服务配置', 
  `cluster_group` varchar(255) NOT NULL COMMENT '集群组',
  `cluster_name` varchar(255) DEFAULT NULL COMMENT '细分集群名称',
  `job_config` text DEFAULT NULL COMMENT '作业配置', 
  `deploy_mode` varchar(255) DEFAULT NULL COMMENT 'yarn yarn-cluster yarn-session',
  `deploy_config` text DEFAULT NULL COMMENT '{task_manager_mem_mb:1024}',
  `locked` tinyint(1) DEFAULT 0 COMMENT '0:没锁，1:有锁',
  `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='流/批计算任务配置表';


CREATE TABLE `dataflow_jobnavi_cluster_config` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `cluster_name` varchar(32) NOT NULL COMMENT '集群名称',
    `cluster_domain` text NOT NULL COMMENT '集群域名地址',
    `version` varchar(32) NOT NULL COMMENT '版本',
    `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
    `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `description` text NOT NULL COMMENT '备注信息',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `processing_version_config` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `component_type` varchar(32) NOT NULL COMMENT '组件，如flink',
    `branch` varchar(32) NOT NULL COMMENT '代码分支，如master',
    `version` varchar(32) NOT NULL COMMENT '对应的分支版本，如0.0.1',
    `description` longtext NULL COMMENT '描述',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 初始化 version
DELETE FROM processing_version_config;
INSERT INTO `processing_version_config` (component_type, branch, version) VALUES ('flink', 'master', '0.0.4');

CREATE TABLE `debug_result_data_log` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT,
    `debug_id` varchar(255) NOT NULL COMMENT '调试唯一标识,debug_flow_id_uuid',
    `job_id` varchar(255) NOT NULL COMMENT 'job id',
    `job_type` varchar(64) NOT NULL COMMENT '调试作业类型，realtime or offline',
    `processing_id` varchar(255) NOT NULL COMMENT 'result_table_id',
    `result_data` text COMMENT '调试结果数据',
    `debug_date` bigint(20) DEFAULT NULL COMMENT '调试时间',
    `thedate` int(7) NOT NULL COMMENT '调试时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `debug_error_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `debug_id` varchar(255) NOT NULL COMMENT '调试唯一标识',
  `job_id` varchar(255) NOT NULL COMMENT '任务job id',
  `job_type` varchar(64) NOT NULL COMMENT '调试作业类型，realtime or offline',
  `processing_id` varchar(255) NOT NULL COMMENT 'result_table_id',
  `error_code` varchar(64) DEFAULT NULL COMMENT '错误码',
  `error_message` text COMMENT '错误信息',
  `debug_date` bigint(20) DEFAULT NULL COMMENT '调试时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `debug_metric_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `debug_id` varchar(255) NOT NULL COMMENT '调试唯一标识',
  `job_id` varchar(255) NOT NULL COMMENT '任务job id',
  `operator` varchar(126) NOT NULL COMMENT '调试人',
  `job_type` varchar(64) NOT NULL COMMENT '调试作业类型，realtime or offline',
  `processing_id` varchar(255) NOT NULL DEFAULT '' COMMENT 'result_table_id',
  `input_total_count` bigint(20) DEFAULT NULL COMMENT '数据输入量',
  `output_total_count` bigint(20) DEFAULT NULL COMMENT '数据输出量',
  `filter_discard_count` bigint(20) DEFAULT NULL COMMENT '数据转换丢弃量',
  `transformer_discard_count` bigint(20) DEFAULT NULL COMMENT '数据转换丢弃量',
  `aggregator_discard_count` bigint(20) DEFAULT NULL COMMENT '数据聚合丢弃量',
  `debug_start_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '调试开始时间',
  `debug_end_at` datetime DEFAULT NULL COMMENT '调试结束时间',
  `debug_heads` varchar(255) DEFAULT NULL COMMENT '调试heads，多个逗号分隔',
  `debug_tails` varchar(255) DEFAULT NULL COMMENT '调试tails，多个逗号分隔',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8; 

-- BKSQL相关表

DROP TABLE IF EXISTS bksql_function_config;
DROP TABLE IF EXISTS bksql_function_mapper_config;
DROP TABLE IF EXISTS bksql_function_permission_config;
DROP TABLE IF EXISTS bksql_function_type_config;
DROP TABLE IF EXISTS bksql_function_group_config;
DROP TABLE IF EXISTS bksql_function_example_config;

CREATE TABLE `bksql_keywords_config` (
  `keyword` varchar(255) NOT NULL PRIMARY KEY COMMENT '关键词',
  `description` TEXT DEFAULT NULL COMMENT '备注信息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL关键字配置表';

CREATE TABLE `bksql_data_type_config` (
 `data_type` varchar(255) NOT NULL PRIMARY KEY COMMENT '数据类型',
 `value_range` TEXT DEFAULT NULL COMMENT '值域',
 `description` TEXT DEFAULT NULL COMMENT '描述'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL数据类型配置表';

CREATE TABLE `bksql_logic_operation_config` (
 `operation` varchar(255) NOT NULL PRIMARY KEY COMMENT '操作符',
 `grammar` varchar(255) NOT NULL COMMENT '语法',
 `description` TEXT DEFAULT NULL COMMENT '备注信息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL运算逻辑配置表';

CREATE TABLE `bksql_function_config` (
 `func_name` varchar(255) NOT NULL COMMENT '函数名称',
 `func_group_id` int NOT NULL COMMENT '函数类型id，数学函数、字符串函数、条件函数和聚合函数等',
 `explain_en` TEXT DEFAULT NULL COMMENT '英文说明',             
 `args_unify` tinyint(1) DEFAULT '1' COMMENT '参数是否统一',
 `permission` int DEFAULT '0' COMMENT '权限，如公开等',
 `func_type` int DEFAULT '0' COMMENT '函数类型，如内置函数、系统自定义函数、用户自定义函数',
 `example` TEXT DEFAULT NULL COMMENT '示例',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`func_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数配置表';

CREATE TABLE `bksql_function_mapper_config` (
 `id` INT(10) NOT NULL AUTO_INCREMENT,
 `func_name` varchar(255) NOT NULL COMMENT '函数名称',
 `framework_name` varchar(255) NOT NULL COMMENT '框架名称',
 `framework_func_name` varchar(255) NOT NULL COMMENT '框架对应函数名称',
 `assign_usage` varchar(255) NOT NULL COMMENT '用法',
 `args_en` varchar(255) DEFAULT NULL COMMENT '英文参数',             
 `return_type` varchar(255) NOT NULL COMMENT '函数返回类型', 
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数映射配置表';

CREATE TABLE `bksql_function_auth_config` (
 `id` int NOT NULL COMMENT '权限id',
 `extent_permission`  varchar(255) NOT NULL COMMENT '权限范围',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数权限配置表';

CREATE TABLE `bksql_function_type_config` (
 `type_id` int NOT NULL COMMENT '函数类型id',
 `type_name_en`  varchar(255) NOT NULL COMMENT '英文版-函数类型，如内置函数、系统自定义函数、用户自定义函数',
 `display_en` varchar(255) DEFAULT NULL COMMENT '英文展示',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数种类配置表';

CREATE TABLE IF NOT EXISTS `bksql_function_group_config` (
 `group_id` int NOT NULL COMMENT '函数类型id',
 `func_group_en` varchar(255) NOT NULL COMMENT '英文-数学函数、字符串函数、条件函数和聚合函数等',
 `display_en` varchar(255) NOT NULL COMMENT '英文展示', 
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`group_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `bksql_function_example_config` (
  `func_name` varchar(255) NOT NULL COMMENT '函数名称',
  `example` text COMMENT '示例',
  `return_field` varchar(255) NOT NULL COMMENT '实例返回字段名称',
  `return_value` varchar(255) NOT NULL COMMENT '实例返回字段值',
  `description` text COMMENT '描述',
  PRIMARY KEY (`func_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数示例表';

INSERT INTO bksql_function_type_config (type_id,type_name_en,display_en) values
(0,'Internal Function','Predefined function'),
(1, 'Custom Function','To be open');

INSERT INTO bksql_function_group_config (`group_id`,`func_group_en`,display_en) values
(1,  'String Function', 'String processing functions'),
(2,  'Mathematical Function', 'Commonly used mathematical functions'),
(3,  'Conditional Function', 'Commonly used conditional judgment functions'),
(4, 'Aggregate Function', 'Calculate a set of values and return a single value');

INSERT INTO bksql_function_example_config(`func_name`,`example`,`return_field`,`return_value`) values
('replace',"SELECT replace('JACKandJUE','J','BL') as result FROM TABLE",'result','BLACKandBLUE'),
('substring',"SELECT substring('abcefg123',2,3) as result FROM TABLE",'result','cef'),
('substring_index',"SELECT substring_index('1.2.3.4','.','3') as result FROM TABLE",'result','1.2.3'),
('lower',"SELECT lower('DATAFLOW') as result FROM TABLE",'result','dataflow'),
('upper',"SELECT upper('dataflow') as result FROM TABLE",'result','DATAFLOW'),
('trim',"SELECT trim('  dataflow   ') as result FROM TABLE",'result','dataflow'),
('rtrim',"SELECT rtrim('dataflow   ') as result FROM TABLE",'result','dataflow'),
('ltrim',"SELECT ltrim('  dataflow') as result FROM TABLE",'result','dataflow'),
('concat',"SELECT concat('data','flow') as result FROM TABLE",'result','dataflow'),
('concat_ws',"SELECT concat_ws('|','data','flow') as result FROM TABLE",'result','data|flow'),
('char_length',"SELECT char_length('dataflow') as result FROM TABLE",'result','8'),
('split',"SELECT split(0.1.2.3.4,'.',3) as result FROM TABLE",'result','3'),
('truncate',"SELECT truncate(12.365,2) as result FROM TABLE",'result','12.36'),
('ceil',"SELECT ceil(1.2) as result FROM TABLE",'result','2'),
('floor',"SELECT floor(1.9) as result FROM TABLE",'result','1'),
('abs',"SELECT abs(-5) as result FROM TABLE",'result','5'),
('power',"SELECT power(2,3) as result FROM TABLE",'result','8'),
('round',"SELECT round(6.59) as result FROM TABLE",'result','7'),
('mod',"SELECT mod(5,2) as result FROM TABLE",'result','1'),
('sqrt',"SELECT sqrt(4) as result FROM TABLE",'result','2'),
('case when',"SELECT case when 1<>1 then 'data' else 'flow' end as result FROM TABLE",'result','flow'),
('if',"SELECT if(1=1,'data','flow') as result FROM TABLE",'result','data'),
('count',"SELECT count(*) as result FROM TABLE",'result','100'),
('sum',"SELECT sum(field) as result FROM TABLE",'result','100'),
('max',"SELECT max(field) as result FROM TABLE",'result','50'),
('min',"SELECT min(field) as result FROM TABLE",'result','1'),
('avg',"SELECT avg(field) as result FROM TABLE",'result','10.00'),
('last',"SELECT last(field) as result FROM TABLE",'result','50');

INSERT INTO bksql_function_mapper_config (`id`,`func_name`,`framework_name`,`framework_func_name`,`assign_usage`,`description`,`args_en`,`return_type`) values
(1, 'replace', 'storm', 'replace', 'replace(STRING str, STRING parttern, STRING replacement)', 'str STRING，指定字符串；parttern STRING，被替换字符串；replacement STRING，用于替换的字符串。', 'str STRING, the specified string; parttern STRING, the replaced string; replacement STRING, the string to replace.', 'STRING'),
(2, 'replace', 'spark sql', 'regexp_replace', 'replace(STRING str, STRING parttern, STRING replacement)', 'str STRING，指定字符串；parttern STRING，被替换字符串；replacement STRING，用于替换的字符串。', 'str STRING, the specified string; parttern STRING, the replaced string; replacement STRING, the string to replace.', 'STRING'),
(3, 'substring', 'storm', 'substring', 'substring(STRING a,INT start),substring(STRING a, INT start, INT len)', 'a STRING，指定字符串；start INT，截取从字符串a开始的位置；len INT，截取的长度。', 'a STRING, the specified string; start INT, intercepting the position from the string a; len INT, the length of the interception.', 'STRING'),
(4, 'substring', 'spark sql', 'substring', 'substring(STRING a,INT start),substring(STRING a, INT start, INT len)', 'a STRING，指定字符串；start INT，截取从字符串a开始的位置；len INT，截取的长度。', 'a STRING, the specified string; start INT, intercepting the position from the string a; len INT, the length of the interception.', 'STRING'),
(5, 'substring_index', 'storm', 'substring_index', 'substring_index(STRING str, STRING delim, int n)', 'str STRING，指定字符串；delim STRING，分隔字符；n INT 字符串str的第n个分割字符delim。', 'str STRING, the specified string; delim STRING, separator character; n INT The nth split character delim of the string str.', 'STRING'),
(6, 'substring_index', 'spark sql', 'substring_index', 'substring_index(STRING str, STRING delim, int n)', 'str STRING，指定字符串；delim STRING，分隔字符；n INT 字符串str的第n个分割字符delim。', 'str STRING, the specified string; delim STRING, separator character; n INT The nth split character delim of the string str.', 'STRING'),
(7, 'lower', 'storm', 'lower', 'lower(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(8, 'lower', 'spark sql', 'lower', 'lower(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(9, 'upper', 'storm', 'upper', 'upper(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(10, 'upper', 'spark sql', 'upper', 'upper(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(11, 'trim', 'storm', 'trim', 'trim(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(12, 'trim', 'spark sql', 'trim', 'trim(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(13, 'rtrim', 'storm', 'rtrim', 'rtrim(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(14, 'rtrim', 'spark sql', 'rtrim', 'rtrim(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(15, 'ltrim', 'storm', 'ltrim', 'ltrim(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(16, 'ltrim', 'spark sql', 'ltrim', 'ltrim(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(17, 'concat', 'storm', 'concat', 'concat(STRING var1, STRING var2, ...)', 'var1 STRING，拼接字符串；var2 STRING，拼接字符串。', 'Var1 STRING, splicing string; var2 STRING, splicing string.', 'STRING'),
(18, 'concat', 'spark sql', 'concat', 'concat(STRING var1, STRING var2, ...)', 'var1 STRING，拼接字符串；var2 STRING，拼接字符串。', 'Var1 STRING, splicing string; var2 STRING, splicing string.', 'STRING'),
(19, 'concat_ws', 'storm', 'concat', 'concat_ws(STRING separator, STRING var1, STRING var2, ...)', 'separator STRING，拼接指定字符；var1 STRING，拼接字符串；var2 STRING，拼接字符串。', 'separator STRING, splicing the specified characters; var1 STRING, splicing strings; var2 STRING, splicing strings.', 'STRING'),
(20, 'concat_ws', 'spark sql', 'concat', 'concat_ws(STRING separator, STRING var1, STRING var2, ...)', 'separator STRING，拼接指定字符；var1 STRING，拼接字符串；var2 STRING，拼接字符串。', 'separator STRING, splicing the specified characters; var1 STRING, splicing strings; var2 STRING, splicing strings.', 'STRING'),
(21, 'char_length', 'storm', 'char_length', 'char_length(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(22, 'char_length', 'spark sql', 'char_length', 'char_length(STRING a)', 'a STRING，指定字符串。', 'a STRING, the specified string.', 'STRING'),
(23, 'split', 'storm', 'split_index', 'split(STRING str, STRING sep, INT index)', 'str STRING，被分割的字符串；sep STRING，以什么为分割符的字符串；index STRING，获取的第几段参数值。', 'str STRING, the string to be split; sep STRING, what is the delimiter character string; index STRING, the number of parameters to retrieve.', 'STRING'),
(24, 'split', 'spark sql', 'split', 'split(STRING str, STRING sep, INT index)', 'str STRING，被分割的字符串；sep STRING，以什么为分割符的字符串；index STRING，获取的第几段参数值。', 'str STRING, the string to be split; sep STRING, what is the delimiter character string; index STRING, the number of parameters to retrieve.', 'STRING'),
(25, 'truncate', 'storm', 'truncate', 'truncate(DOUBLE/FLOAT a, INT n)', 'a DOUBLE/FLOAT，指定数值字段；n INT，保留小数点后几位数字。', 'a DOUBLE/FLOAT, specify a numeric field; n INT, retain a few digits after the decimal point.', 'DOUBLE/FLOAT'),
(26, 'truncate', 'spark sql', 'trunc', 'truncate(DOUBLE/FLOAT a, INT n)', 'a DOUBLE/FLOAT，指定数值字段；n INT，保留小数点后几位数字。', 'a DOUBLE/FLOAT, specify a numeric field; n INT, retain a few digits after the decimal point.', 'DOUBLE/FLOAT'),
(27, 'ceil', 'storm', 'ceil', 'ceil(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field.', 'DOUBLE'),
(28, 'ceil', 'spark sql', 'ceil', 'ceil(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field.', 'DOUBLE'),
(29, 'floor', 'storm', 'floor', 'floor(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field.', 'DOUBLE'),
(30, 'floor', 'spark sql', 'floor', 'floor(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field.', 'DOUBLE'),
(31, 'abs', 'storm', 'abs', 'abs(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field.', 'INT/DOUBLE/LONG/FLOAT'),
(32, 'abs', 'spark sql', 'abs', 'abs(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field.', 'INT/DOUBLE/LONG/FLOAT'),
(33, 'power', 'storm', 'power', 'power(INT/DOUBLE/LONG/FLOAT a, INT n)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段；n INT，指定数值的几次方。', 'a INT/DOUBLE/LONG/FLOAT, specifies a numeric field; n INT, specifies the number of times the value is to be used.', 'DOUBLE'),
(34, 'power', 'spark sql', 'power', 'power(INT/DOUBLE/LONG/FLOAT a, INT n)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段；n INT，指定数值的几次方。', 'a INT/DOUBLE/LONG/FLOAT, specifies a numeric field; n INT, specifies the number of times the value is to be used.', 'DOUBLE'),
(35, 'round', 'storm', 'round', 'round(INT/DOUBLE/LONG/FLOAT a, INT n)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段；n INT，保留小数点后几位数字。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field; n INT, retain a few digits after the decimal point.', 'DOUBLE'),
(36, 'round', 'spark sql', 'round', 'round(INT/DOUBLE/LONG/FLOAT a, INT n)', 'a INT/DOUBLE/LONG/FLOAT，指定数值字段；n INT，保留小数点后几位数字。', 'a INT/DOUBLE/LONG/FLOAT, specify a numeric field; n INT, retain a few digits after the decimal point.', 'DOUBLE'),
(37, 'mod', 'storm', 'mod', 'mod(LONG a,LONG b)', 'a LONG，被除数；b LONG，除数。', 'a LONG, dividend; b LONG, divisor.', 'INT'),
(38, 'mod', 'spark sql', 'pmod', 'mod(LONG a,LONG b)', 'a LONG，被除数；b LONG，除数。', 'a LONG, dividend; b LONG, divisor.', 'INT'),
(39, 'sqrt', 'storm', 'sqrt', 'sqrt(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值。', 'a INT/DOUBLE/LONG/FLOAT, specify the value.', 'DOUBLE'),
(40, 'sqrt', 'spark sql', 'sqrt', 'sqrt(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT，指定数值。', 'a INT/DOUBLE/LONG/FLOAT, specify the value.', 'DOUBLE'),
(41, 'case when', 'storm', 'case when', 'CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END', '', '', ''),
(42, 'case when', 'spark sql', 'case when', 'CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END', '', '', ''),
(43, 'if', 'storm', 'if', 'if(BOOLEAN testCondition, T valueTrue, T valueFalseOrNull)', '', '', ''),
(44, 'if', 'spark sql', 'if', 'if(BOOLEAN testCondition, T valueTrue, T valueFalseOrNull)', '', '', ''),
(45, 'count', 'storm', 'count', 'count(INT/DOUBLE/LONG/FLOAT/STRING a)', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'LONG'),
(46, 'count', 'spark sql', 'count', 'count(INT/DOUBLE/LONG/FLOAT/STRING a)', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'LONG'),
(47, 'min', 'storm', 'min', 'min(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT', 'a INT/DOUBLE/LONG/FLOAT', 'INT/DOUBLE/LONG/FLOAT'),
(48, 'min', 'spark sql', 'min', 'min(INT/DOUBLE/LONG/FLOAT/STRING a)', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'INT/DOUBLE/LONG/FLOAT/STRING'),
(49, 'max', 'storm', 'max', 'max(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT', 'a INT/DOUBLE/LONG/FLOAT', 'INT/DOUBLE/LONG/FLOAT'),
(50, 'max', 'spark sql', 'max', 'max(INT/DOUBLE/LONG/FLOAT/STRING a)', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'INT/DOUBLE/LONG/FLOAT/STRING'),
(51, 'sum', 'storm', 'sum', 'sum(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT', 'a INT/DOUBLE/LONG/FLOAT', 'DOUBLE'),
(52, 'sum', 'spark sql', 'sum', 'sum(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT', 'a INT/DOUBLE/LONG/FLOAT', 'DOUBLE'),
(53, 'avg', 'storm', 'avg', 'avg(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT', 'a INT/DOUBLE/LONG/FLOAT', 'DOUBLE'),
(54, 'avg', 'spark sql', 'avg', 'avg(INT/DOUBLE/LONG/FLOAT a)', 'a INT/DOUBLE/LONG/FLOAT', 'a INT/DOUBLE/LONG/FLOAT', 'DOUBLE'),
(55, 'last', 'storm', 'last', 'last(INT/DOUBLE/LONG/FLOAT/STRING a)', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'INT/DOUBLE/LONG/FLOAT/STRING'),
(56, 'last', 'spark sql', 'last', 'last(INT/DOUBLE/LONG/FLOAT/STRING a)', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'a INT/DOUBLE/LONG/FLOAT/STRING', 'INT/DOUBLE/LONG/FLOAT/STRING');

INSERT INTO bksql_function_config (`func_name`,`func_group_id`,`explain_en`,`args_unify`) values
('replace', 1, 'Replace the substring of the string str with the regular pattern pattern with the string replacement and return the new string. Matching is being replaced, the argument is null or it is legally invalid.', 1),
('substring', 1, 'Gets a string substring, intercepts a substring of length len from position start, and if len is not specified it intercepts the end of the string. start starts from 1, and starts with zero as 1 when viewed.', 1),
('substring_index', 1, 'Returns the string before the nth delimiter delim in the string str. If n is a positive number, return from the last (counting from the left) delimiter to all characters on the left. If count is negative, return from the last (counting from the right) to all characters on the right.', 1),
('lower', 1, 'Returns a string converted to a lowercase character.', 1),
('upper', 1, 'Returns a string converted to uppercase characters.', 1),
('trim', 1, 'Remove the white space on both sides.', 1),
('rtrim', 1, 'Remove the white space character on the right.', 1),
('ltrim', 1,'Remove the left blank character.', 1),
('concat', 1,  'Splicing two or more string values to form a new string.', 1),
('concat_ws', 1,  'Connect each parameter value and the delimiter specified by the first parameter separator into a new string.', 1),
('char_length', 1, 'Returns the number of characters in the string.', 1),
('split', 1, 'Sep as a delimiter, the string str is divided into several segments, which take the index segment, can not return NULL, index from 0.', 1),
('truncate', 2, 'Preserve the number of decimal places and specify the number of digits.', 1),
('ceil', 2, 'Returns the smallest integer greater than or equal to a number.', 1),
('floor', 2, 'Returns the largest integer less than or equal to a number.', 1),
('abs', 2,'Take the absolute value.', 1),
('power', 2,'Returns the (n)th power of field a.', 1),
('round', 2,'Round off the value, round off the field a and retain n decimal places.', 1),
('mod', 2, 'Find the remainder and return the remainder of dividing field a by b.', 1),
('sqrt', 2, 'Returns the square root of the number.', 1),
('case when', 3,'If a is TRUE, b is returned; if c is TRUE, d is returned; otherwise, e is returned.', 1),
('if', 3,'The Boolean value of the first parameter is the judgment criterion. If it is true, the second parameter is returned; if it is false, the third parameter is returned.', 1),
('count', 4,  'Count the number of input numbers.', 1),
('min', 4,  'Returns the minimum value of the input value.', 0),
('max', 4, 'Returns the maximum value of the input value.', 0),
('sum', 4,  'Returns the sum of the values between all input values.', 1),
('avg', 4, 'Returns the average number.', 1),
('last', 4,  'Return the last data.', 1);

-- bksql end

CREATE TABLE `adhoc_query_info` (
  `query_id` varchar(255) NOT NULL COMMENT '即席查询ID',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`query_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='即席查询任务配置信息';

CREATE TABLE `adhoc_query_job` (
  `query_id` varchar(255) NOT NULL COMMENT '即席查询ID',
  `sql` text COMMENT 'SQL',
  `args` text NOT NULL  COMMENT '即席查询参数',
  `status` varchar(128) NOT NULL COMMENT '即席查询状态',
  `jobserver_config` text DEFAULT NULL COMMENT '作业服务配置', 
  `exec_id` varchar(255) NOT NULL COMMENT '执行ID',
  `error_message` text NOT NULL COMMENT '错误信息',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(128) DEFAULT NULL COMMENT '修改人',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`query_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='即席查询任务运行信息';

CREATE TABLE `storage_hdfs_path_status` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `result_table_id` varchar(255) NOT NULL COMMENT 'result_table_id',
  `path_time` varchar(11) NOT NULL COMMENT 'HDFS路径时间',
  `data_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据时间',
  `write_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '写入时间',
  `is_rerun` int(11) NOT NULL DEFAULT '0' COMMENT '是否重跑',
  `path` text NOT NULL COMMENT 'HDFS路径',
  `partition_num` int(11) NOT NULL COMMENT 'kafka partition号',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',        
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hdfs数据路径状态';

CREATE TABLE `storage_hdfs_export_retry` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `result_table_id` varchar(255) NOT NULL COMMENT 'result_table_id',
  `schedule_time` bigint(20) NOT NULL COMMENT '调度时间',
  `data_dir` text COMMENT 'hdfs数据目录',
  `retry_times` int(11) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY (`result_table_id`,`schedule_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='hdfs读取重试设置';

-- 数据开发相关表

DROP TABLE IF EXISTS dataflow_node_component_config;
DROP TABLE IF EXISTS dataflow_node_config;

CREATE TABLE `dataflow_node_component_config` (
  `component_name` varchar(255) NOT NULL COMMENT '节点所属组件ID，如：storage',
  `component_alias` varchar(255) NOT NULL COMMENT '节点所属组件名称，如：数据存储',
  `order` int(11) NOT NULL COMMENT '组件在界面的位置顺序',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`component_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow中组件库配置表';

CREATE TABLE `dataflow_node_config` (
  `node_type_name` varchar(255) NOT NULL COMMENT '类型ID，如：raw_source',
  `node_type_alias` varchar(255) NOT NULL COMMENT '类型名称，如：原始数据',
  `component_name` varchar(255) NOT NULL COMMENT '节点所属组件ID，如：storage',
  `belong` varchar(128) DEFAULT 'bkdata' COMMENT '标记这个字段是系统的bkdata还是用户的other',
  `has_multiple_parent` bool NOT NULL COMMENT '是否有多个父亲节点',
  `support_debug` bool NOT NULL COMMENT '是否支持调试',
  `max_parent_num` integer NOT NULL COMMENT '最大父节点数量',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '更新人 ',                                          
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间', 
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`node_type_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow中组件库中节点配置表';

INSERT INTO `dataflow_node_component_config` (`component_name`, `component_alias`, `description`, `order`)
VALUES
('calculate', '数据处理', '计算描述', 2),
('source', '数据源', '数据源描述', 1),
('storage', '数据存储', '存储描述', 3);


INSERT INTO `dataflow_node_config` (`node_type_name`, `node_type_alias`, `component_name`, `belong`, `support_debug`, `max_parent_num`, `created_by`, `created_at`, `updated_by`, `updated_at`, `description`, `has_multiple_parent`)
VALUES
('stream_source', '实时数据源', 'source', 'bkdata', 0, 0, NULL, '2018-12-17 09:53:08', NULL, '2018-12-17 09:57:57', '清洗、实时计算的结果数据，数据延迟低，可用于分钟级别的实时统计计算', 0),
('batch_source', '离线数据源', 'source', 'bkdata', 0, 0, NULL, '2018-12-17 09:53:16', NULL, '2018-12-17 09:57:53', '落地到分布式文件系统的数据，可用于小时级以上的离线统计计算', 0),
('stream', '实时计算', 'calculate', 'bkdata', 1, 2, NULL, '2018-11-12 16:46:13', NULL, '2019-01-23 20:13:24', '基于流式处理的实时计算_支持秒级和分钟级的计算', 1),
('batch', '离线计算', 'calculate', 'bkdata', 0, 2, NULL, '2018-11-12 16:46:13', NULL, '2018-11-26 19:34:24', '基于批处理的离线计算，支持小时级和天级的计算。', 1),
('es', 'ElasticSearch', 'storage', 'bkdata', 0, 1, NULL, '2018-11-12 16:46:13', NULL, '2018-11-26 19:35:54', '基于Elasticsearch日志全文检索', 0),
('hdfs', 'HDFS', 'storage', 'bkdata', 0, 1, NULL, '2018-11-12 16:46:13', NULL, '2018-11-26 19:35:55', '基于HDFS的历史数据存储，不可以直接查询，只能连接离线。', 0),
('mysql', 'MySQL', 'storage', 'bkdata', 0, 1, NULL, '2018-11-12 16:46:13', NULL, '2018-11-26 19:35:56', '基于MySQL的关系型数据库存储。', 0);


-- Create model FlowNodeLinkRules

CREATE TABLE `dataflow_node_link_rules_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `node_type_name` varchar(255) NOT NULL COMMENT '节点类型',
  `valid_child_node_type` varchar(64) DEFAULT NULL COMMENT '允许的子节点类型',
  `storage` varchar(64) DEFAULT NULL COMMENT '当前节点类型接连接子节点所必须带的存储',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '开始人 ',
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '开始时间',
  `description` text NOT NULL COMMENT '备注信息',
  `max_child_num` int(11) DEFAULT NULL COMMENT '当前节点类型的一个rt在支持的子节点存储类型数量',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=32 DEFAULT CHARSET=utf8 COMMENT='【连线规则】节点连线规则配置';

CREATE TABLE `dataflow_info` (
  `flow_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'flow的id',
  `flow_name` varchar(255) NOT NULL COMMENT 'flow的名名称',
  `project_id` int(11) NOT NULL COMMENT 'flow所属项目id',
  `status` varchar(32) COMMENT 'flow运行状态',
  `is_locked` int(11) NOT NULL COMMENT '是否被锁住',
  `latest_version` varchar(255) DEFAULT NULL COMMENT '最新版本号',
  `bk_app_code` varchar(255) NOT NULL COMMENT '哪些有APP在使用？',                                 
  `active` tinyint(1) NOT NULL DEFAULT 0 COMMENT '0:无效，1:有效',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `locked_by` varchar(255) DEFAULT NULL,                                                        
  `locked_at` datetime(6) DEFAULT NULL,                                                         
  `updated_by` varchar(50) DEFAULT NULL COMMENT '开始人 ',                                      
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '开始时间',      
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`flow_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow配置信息';

CREATE TABLE `dataflow_node_info` (
  `node_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'node的id',                                                        
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `node_name` varchar(255) DEFAULT NULL COMMENT '节点名称', 
  `node_config` longtext NOT NULL COMMENT '节点配置',
  `node_type` varchar(15) NOT NULL COMMENT '节点类型',
  `frontend_info` longtext COMMENT '记录节点的坐标信息',
  `status` varchar(9) NOT NULL COMMENT '节点状态',
  `latest_version` varchar(255) NOT NULL COMMENT '最新版本',                                     
  `running_version` varchar(255) COMMENT '正在运行的版本',                             
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` varchar(50) DEFAULT NULL COMMENT '开始人 ',                                          
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '开始时间',           
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点配置信息';

CREATE TABLE `dataflow_link_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `from_node_id` int(11) NOT NULL COMMENT '连线的头节点',
  `to_node_id` int(11) NOT NULL COMMENT '连线的尾节点',
  `frontend_info` longtext COMMENT '前端配置',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dataflow_link_info_from_node_id_to_node_id_uniq` (`from_node_id`,`to_node_id`)                        
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点连线信息';

CREATE TABLE IF NOT EXISTS `dataflow_node_relation` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `bk_biz_id` int(11) DEFAULT NULL COMMENT '业务id',
  `project_id` int(11) NOT NULL COMMENT '项目id',
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `node_id` int(11) NOT NULL COMMENT 'flow中节点id',
  `result_table_id` varchar(255) NOT NULL comment 'result_table_id, data_id节点的输入数据',
  `node_type` varchar(64) NOT NULL COMMENT '节点类型',
  `generate_type` varchar(32) DEFAULT 'user' COMMENT '结果表生成类型 user/system',
  `is_head` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否是节点的头部RT',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dataflow_node_relation_node_id_result_table_id_uniq` (`node_id`,`result_table_id`)              
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点与对象之间关系信息';

CREATE TABLE `dataflow_processing` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `node_id` int(11) COMMENT 'node的id',
  `processing_id` varchar(255) NOT NULL COMMENT 'processing的id',
  `processing_type` varchar(255) NOT NULL COMMENT '包括实时作业stream、离线作业batch',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点任务配置信息';

CREATE TABLE `dataflow_job` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `node_id` int(11) COMMENT 'node的id',
  `job_id` varchar(255) NOT NULL COMMENT 'job的id',
  `job_type` varchar(255) NOT NULL COMMENT '取值来源于processing_type_config',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow节点任务运行信息';

CREATE TABLE `dataflow_instance_version` (
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `running_version` varchar(255) NOT NULL COMMENT '运行中的版本',
  PRIMARY KEY (`flow_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow任务运行实例版本信息';

CREATE TABLE `dataflow_execute_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `action` varchar(32) NOT NULL comment 'start, stop, restart',
  `status` varchar(32) NOT NULL comment 'preparing, running, succeeded, failed',
  `logs_zh` longtext COMMENT '日志中文内容',
  `logs_en` longtext COMMENT '日志英文内容',
  `end_time` datetime(6) COMMENT '结束时间',
  `start_time` datetime(6) COMMENT '开始时间',
  `context` longtext COMMENT '运行时上下文',
  `version` varchar(255) COMMENT 'flow的版本',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow任务运行实例版本信息';

CREATE TABLE `dataflow_version_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `flow_version` varchar(32) NOT NULL COMMENT 'flow的版本',
  `version_ids` longtext NOT NULL COMMENT '关联的节点&连线 RevisionID',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow任务实例版本流水表';

CREATE TABLE `dataflow_debug_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `flow_id` int(11) NOT NULL COMMENT 'flow的id',
  `version` varchar(32) NOT NULL COMMENT 'flow的版本',
  `nodes_info` longtext COMMENT '节点信息',
  `start_time` datetime(6) DEFAULT NULL COMMENT '开始时间',
  `end_time` datetime(6) DEFAULT NULL COMMENT '结束时间',
  `status` varchar(32) NOT NULL COMMENT '调式状态',
  `logs_zh` longtext COMMENT '日志中文内容',
  `logs_en` longtext COMMENT '日志英文内容',
  `context` longtext COMMENT '？？？？？？',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow调式流水表';

-- django-reversion依赖表

CREATE TABLE `django_content_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_label` varchar(100) NOT NULL,
  `model` varchar(100) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `django_content_type_app_label_3ec8c61c_uniq` (`app_label`,`model`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

INSERT IGNORE INTO `django_content_type` (`app_label`, `model`) VALUES
('dataflow.flow', 'flownodeinfo'),
('dataflow.flow', 'flowlinkinfo');

CREATE TABLE `flow_user` (
  `id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, 
  `username` varchar(100) NOT NULL
);

CREATE TABLE `reversion_revision` (
  `id` int(11) NOT NULL AUTO_INCREMENT, 
  `date_created` datetime NOT NULL, 
  `comment` longtext NOT NULL, 
  `user_id` integer NULL,
  PRIMARY KEY (`id`),
  KEY `reversion_revision_user_id_5b2ec55e_fk_auth_user_id` (`user_id`),
  KEY `reversion_revision_c69e55a4` (`date_created`),
  CONSTRAINT `reversion_revision_user_id_5b2ec55e_fk_auth_user_id` FOREIGN KEY (`user_id`) REFERENCES `flow_user` (`id`)  
);

CREATE TABLE `reversion_version` (
  `id` int(11) NOT NULL AUTO_INCREMENT, 
  `object_id` varchar(191) NOT NULL, 
  `format` varchar(255) NOT NULL, 
  `serialized_data` longtext NOT NULL, 
  `object_repr` longtext NOT NULL, 
  `content_type_id` integer NOT NULL, 
  `revision_id` integer NOT NULL, 
  `db` varchar(191) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `reversion_version_db_52f6853c_uniq` (`db`,`content_type_id`,`object_id`,`revision_id`),
  KEY `reversion_ver_content_type_id_26d4c4a9_fk_django_content_type_id` (`content_type_id`),
  KEY `reversion_version_revision_id_4916a950_fk_reversion_revision_id` (`revision_id`),
  CONSTRAINT `reversion_version_revision_id_4916a950_fk_reversion_revision_id` FOREIGN KEY (`revision_id`) REFERENCES `reversion_revision` (`id`),
  CONSTRAINT `reversion_ver_content_type_id_26d4c4a9_fk_django_content_type_id` FOREIGN KEY (`content_type_id`) REFERENCES `django_content_type` (`id`)
);
