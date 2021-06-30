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


CREATE TABLE IF not EXISTS `bksql_function_config_v3` (
 `func_name` varchar(255) NOT NULL COMMENT '函数名称',
 `func_alias` varchar(255) NOT NULL COMMENT '函数中文名',
 `func_group` ENUM('Mathematical Function','String Function','Conditional Function','Aggregate Function','Other Function') COMMENT '英文版-数学函数、字符串函数、条件函数和聚合函数等',
 `explain` TEXT DEFAULT NULL COMMENT '英文函数说明',
 `func_type` ENUM('Internal Function', 'User-defined Function') COMMENT '英文版-函数类型，如内置函数、用户自定义函数',
 `func_udf_type` varchar(255) DEFAULT NULL COMMENT '函数开发类型udf、udtf、udaf',
 `func_language` varchar(255) DEFAULT NULL COMMENT '函数开发语言java，python',
 `example` TEXT DEFAULT NULL COMMENT '示例',
 `example_return_value` TEXT DEFAULT NULL COMMENT '示例返回值',
 `version` varchar(255) DEFAULT NULL COMMENT '最新版本',
 `support_framework` varchar(255) NOT NULL COMMENT '支持计算类型，stream,batch',
 `active` tinyint(1) DEFAULT '1' COMMENT '记录是否有效',
 `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
 `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`func_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数表';

CREATE TABLE IF not EXISTS `bksql_function_dev_config` (
 `id` INT(10) NOT NULL AUTO_INCREMENT,
 `func_name` varchar(255) NOT NULL COMMENT '函数名称',
 `version` varchar(255) NOT NULL COMMENT '记录版本 v1,v2,dev',
 `func_alias` varchar(255) DEFAULT NULL COMMENT '函数中文名',
 `func_language` varchar(255) DEFAULT NULL COMMENT '函数开发语言java，python',
 `func_udf_type` varchar(255) DEFAULT NULL COMMENT '函数开发类型udf、udtf、udaf',
 `input_type` TEXT DEFAULT NULL COMMENT '输入参数',
 `return_type` TEXT DEFAULT NULL COMMENT '返回参数',
 `explain` TEXT DEFAULT NULL COMMENT '函数说明',
 `example` TEXT DEFAULT NULL COMMENT '使用样例',
 `example_return_value` TEXT DEFAULT NULL COMMENT '样例返回',
 `code_config` MEDIUMTEXT DEFAULT NULL COMMENT '代码信息',
 `sandbox_name` varchar(255) DEFAULT 'default' COMMENT '沙箱名称',
 `released` tinyint(1) DEFAULT 0 COMMENT '函数是否发布，0未发布，1已发布',
 `locked` tinyint(1) DEFAULT 0 COMMENT '判断是否在开发中，1开发中，0不在开发中',
 `locked_by` varchar(50) NOT NULL DEFAULT '' COMMENT '上锁人',
 `locked_at` timestamp NULL DEFAULT NULL COMMENT '上锁时间',
 `created_by` varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
 `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `updated_by` varchar(50) DEFAULT NULL COMMENT '修改人 ',
 `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
 `debug_id` varchar(255) DEFAULT NULL COMMENT '调试id',
 `support_framework` varchar(255) DEFAULT NULL COMMENT '支持计算类型，stream,batch',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`id`),
 KEY `multi_key` (`func_name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数开发记录表';

CREATE TABLE IF not EXISTS `bksql_function_patameter_config` (
 `id` INT(10) NOT NULL AUTO_INCREMENT,
 `func_name` varchar(255) NOT NULL COMMENT '函数名称',
 `input_type` TEXT NOT NULL COMMENT '输入参数',
 `return_type` TEXT NOT NULL COMMENT '返回参数',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`id`),
 KEY `multi_key` (`id`,`func_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数参数配置表';

CREATE TABLE IF not EXISTS `bksql_function_update_log` (
 `id` INT(10) NOT NULL AUTO_INCREMENT,
 `func_name` varchar(255) NOT NULL COMMENT '函数名称',
 `version` varchar(255) NOT NULL COMMENT '函数版本',
 `release_log` TEXT NOT NULL COMMENT '发布日志',
 `updated_by` varchar(50) NOT NULL DEFAULT '' COMMENT '操作人',
 `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
 `description` TEXT DEFAULT NULL COMMENT '描述',
 PRIMARY KEY (`id`),
 KEY `multi_key` (`func_name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数变更日志记录表';

CREATE TABLE IF not EXISTS `bksql_function_sandbox_config` (
  `sandbox_name` varchar(255) NOT NULL COMMENT '沙箱名称',
  `java_security_policy` TEXT DEFAULT NULL COMMENT 'java沙箱配置',
  `python_disable_imports` TEXT DEFAULT NULL COMMENT 'python禁用import包',
  `python_install_packages` TEXT DEFAULT NULL COMMENT 'python安装包',
  `description` TEXT DEFAULT NULL COMMENT '描述',
  PRIMARY KEY (`sandbox_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='BKSQL函数沙箱配置表';

CREATE TABLE IF not EXISTS `processing_udf_info` (
	`id` INT(10) NOT NULL AUTO_INCREMENT,
	`processing_id` varchar(255) NOT NULL COMMENT '处理节点ID',
	`processing_type` varchar(32) NOT NULL COMMENT '处理节点类型',
	`udf_name` varchar(255) NOT NULL COMMENT '函数名称',
	`udf_info` TEXT NOT NULL COMMENT '函数配置',
	`description` TEXT DEFAULT NULL COMMENT '描述',
	PRIMARY KEY (`id`),
	KEY(`processing_id`, `udf_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='计算中udf和processing的编辑信息';


CREATE TABLE IF not EXISTS `processing_udf_job` (
	`id` INT(10) NOT NULL AUTO_INCREMENT,
	`job_id` varchar(255) NOT NULL COMMENT '作业ID',
	`processing_id` varchar(255) NOT NULL COMMENT '处理节点ID',
	`processing_type` varchar(32) NOT NULL COMMENT '处理节点类型',
	`udf_name` varchar(255) NOT NULL COMMENT '函数名称',
	`udf_info` TEXT NOT NULL COMMENT '函数配置',
	`description` TEXT DEFAULT NULL COMMENT '描述',
	PRIMARY KEY (`id`),
	KEY(`job_id`,`processing_id`, `udf_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='计算中udf和processing的运行信息';

CREATE TABLE IF NOT EXISTS `dataflow_flow_udf_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `func_name` varchar(255) NOT NULL COMMENT '函数名称',
  `action` varchar(32) NOT NULL comment 'start_debug, start_deploy',
  `start_time` datetime(6) DEFAULT NULL COMMENT '开始时间',
  `end_time` datetime(6) DEFAULT NULL COMMENT '结束时间',
  `status` varchar(32) NOT NULL COMMENT '状态',
  `logs_zh` longtext COMMENT '日志中文内容',
  `logs_en` longtext COMMENT '日志英文内容',
  `context` longtext COMMENT '日志上下文信息',
  `version` varchar(255) COMMENT '函数的版本',
  `created_by` varchar(128) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `description` text NOT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='dataflow函数调试/部署流水表';

-- 沙箱配置表
INSERT INTO bksql_function_sandbox_config(`sandbox_name`, `java_security_policy`, `python_disable_imports`, `python_install_packages`) VALUES
('default', null, 'os',null);

INSERT INTO `bksql_function_config_v3` (`func_name`, `func_alias`, `func_group`, `explain`, `func_type`, `func_udf_type`, `func_language`, `example`, `example_return_value`, `version`, `support_framework`, `active`, `updated_at`, `description`, `created_by`)
VALUES
  ('abs', 'abs', 'Mathematical Function', 'Take the absolute value.', 'Internal Function', '', '', 'SELECT abs(-5) as result FROM TABLE', '5', '', 'stream,batch', 1, '2019-08-09 21:58:49', '', ''),
  ('avg', 'avg', 'Aggregate Function', 'Returns the average number.', 'Internal Function', '', '', 'SELECT avg(field) as result FROM TABLE', '10.00', '', 'stream,batch', 1, '2019-08-09 21:59:03', '', ''),
  ('case when', 'case when', 'Conditional Function', 'If a is TRUE, b is returned; if c is TRUE, d is returned; otherwise, e is returned.', 'Internal Function', '', '', 'SELECT case when 1<>1 then \'data\' else \'flow\' end as result FROM TABLE', 'flow', '', 'stream,batch', 1, '2019-08-09 21:59:04', '', ''),
  ('ceil', 'ceil', 'Mathematical Function', 'Returns the smallest integer greater than or equal to a number.', 'Internal Function', '', '', 'SELECT ceil(1.2) as result FROM TABLE', '2', '', 'stream,batch', 1, '2019-08-09 21:59:04', '', ''),
  ('char_length', 'char_length', 'String Function', 'Returns the number of characters in the string.', 'Internal Function', '', '', 'SELECT char_length(\'dataflow\') as result FROM TABLE', '8', '', 'stream,batch', 1, '2019-08-09 21:59:05', '', ''),
  ('concat', 'concat', 'String Function', 'Splicing two or more string values to form a new string.', 'Internal Function', '', '', 'SELECT concat(\'data\',\'flow\') as result FROM TABLE', 'dataflow', '', 'stream,batch', 1, '2019-08-09 21:59:06', '', ''),
  ('concat_ws', 'concat_ws', 'String Function', 'Connect each parameter value and the delimiter specified by the first parameter separator into a new string.', 'Internal Function', '', '', 'SELECT concat_ws(\'|\',\'data\',\'flow\') as result FROM TABLE', 'data|flow', '', 'stream,batch', 1, '2019-08-09 21:59:06', '', ''),
  ('count', 'count', 'Aggregate Function', 'Count the number of input numbers.', 'Internal Function', '', '', 'SELECT count(*) as result FROM TABLE', '100', '', 'stream,batch', 1, '2019-08-09 21:59:06', '', ''),
  ('floor', 'floor', 'Mathematical Function', 'Returns the largest integer less than or equal to a number.', 'Internal Function', '', '', 'SELECT floor(1.9) as result FROM TABLE', '1', '', 'stream,batch', 1, '2019-08-09 21:59:07', '', ''),
  ('if', 'if', 'Conditional Function', 'The Boolean value of the first parameter is the judgment criterion. If it is true, the second parameter is returned; if it is false, the third parameter is returned.', 'Internal Function', '', '', 'SELECT if(1=1,\'data\',\'flow\') as result FROM TABLE', 'data', '', 'stream,batch', 1, '2019-08-09 21:59:08', '', ''),
  ('last', 'last', 'Aggregate Function', 'Return the last data.', 'Internal Function', '', '', 'SELECT last(field) as result FROM TABLE', '50', '', 'stream,batch', 1, '2019-08-09 21:59:08', '', ''),
  ('lower', 'lower', 'String Function', 'Returns a string converted to a lowercase character.', 'Internal Function', '', '', 'SELECT lower(\'DATAFLOW\') as result FROM TABLE', 'dataflow', '', 'stream,batch', 1, '2019-08-09 21:59:08', '', ''),
  ('ltrim', 'ltrim', 'String Function', 'Remove the left blank character.', 'Internal Function', '', '', 'SELECT ltrim(\'  dataflow\') as result FROM TABLE', 'dataflow', '', 'stream,batch', 1, '2019-08-09 21:59:09', '', ''),
  ('max', 'max', 'Aggregate Function', 'Returns the maximum value of the input value.', 'Internal Function', '', '', 'SELECT max(field) as result FROM TABLE', '50', '', 'stream,batch', 1, '2019-08-09 21:59:09', '', ''),
  ('min', 'min', 'Aggregate Function', 'Returns the minimum value of the input value.', 'Internal Function', '', '', 'SELECT min(field) as result FROM TABLE', '1', '', 'stream,batch', 1, '2019-08-09 21:59:10', '', ''),
  ('mod', 'mod', 'Mathematical Function', 'Find the remainder and return the remainder of dividing field a by b.', 'Internal Function', '', '', 'SELECT mod(5,2) as result FROM TABLE', '1', '', 'stream,batch', 1, '2019-08-09 21:59:10', '', ''),
  ('power', 'power', 'Mathematical Function', 'Returns the (n)th power of field a.', 'Internal Function', '', '', 'SELECT power(2,3) as result FROM TABLE', '8', '', 'stream,batch', 1, '2019-08-09 21:59:11', '', ''),
  ('replace', 'replace', 'String Function', 'Replace the substring of the string str with the regular pattern pattern with the string replacement and return the new string. Matching is being replaced, the argument is null or it is legally invalid.', 'Internal Function', '', '', 'SELECT replace(\'JACKandJUE\',\'J\',\'BL\') as result FROM TABLE', 'BLACKandBLUE', '', 'stream,batch', 1, '2019-08-09 21:59:11', '', ''),
  ('round', 'round', 'Mathematical Function', 'Round off the value, round off the field a and retain n decimal places.', 'Internal Function', '', '', 'SELECT round(6.59) as result FROM TABLE', '7', '', 'stream,batch', 1, '2019-08-09 21:59:12', '', ''),
  ('rtrim', 'rtrim', 'String Function', 'Remove the white space character on the right.', 'Internal Function', '', '', 'SELECT rtrim(\'dataflow   \') as result FROM TABLE', 'dataflow', '', 'stream,batch', 1, '2019-08-09 21:59:12', '', ''),
  ('split', 'split', 'String Function', 'Sep as a delimiter, the string str is divided into several segments, which take the index segment, can not return NULL, index from 0.', 'Internal Function', '', '', 'SELECT split(0.1.2.3.4,\'.\',3) as result FROM TABLE', '3', '', 'stream,batch', 1, '2019-08-09 21:59:13', '', ''),
  ('sqrt', 'sqrt', 'Mathematical Function', 'Returns the square root of the number.', 'Internal Function', '', '', 'SELECT sqrt(4) as result FROM TABLE', '2', '', 'stream,batch', 1, '2019-08-09 21:59:13', '', ''),
  ('substring', 'substring', 'String Function', 'Gets a string substring, intercepts a substring of length len from position start, and if len is not specified it intercepts the end of the string. start starts from 1, and starts with zero as 1 when viewed.', 'Internal Function', '', '', 'SELECT substring(\'abcefg123\',2,3) as result FROM TABLE', 'cef', '', 'stream,batch', 1, '2019-08-09 21:59:14', '', ''),
  ('substring_index', 'substring_index', 'String Function', 'Returns the string before the nth delimiter delim in the string str. If n is a positive number, return from the last (counting from the left) delimiter to all characters on the left. If count is negative, return from the last (counting from the right) to all characters on the right.', 'Internal Function', '', '', 'SELECT substring_index(\'1.2.3.4\',\'.\',\'3\') as result FROM TABLE', '1.2.3', '', 'stream,batch', 1, '2019-08-09 21:59:15', '', ''),
  ('sum', 'sum', 'Aggregate Function', 'Returns the sum of the values between all input values.', 'Internal Function', '', '', 'SELECT sum(field) as result FROM TABLE', '100', '', 'stream,batch', 1, '2019-08-09 21:59:15', '', ''),
  ('trim', 'trim', 'String Function', 'Remove the white space on both sides.', 'Internal Function', '', '', 'SELECT trim(\'  dataflow   \') as result FROM TABLE', 'dataflow', '', 'stream,batch', 1, '2019-08-09 21:59:16', '', ''),
  ('truncate', 'truncate', 'Mathematical Function', 'Preserve the number of decimal places and specify the number of digits.', 'Internal Function', '', '', 'SELECT truncate(12.365,2) as result FROM TABLE', '12.36', '', 'stream,batch', 1, '2019-08-09 21:59:16', '', ''),
  ('upper', 'upper', 'String Function', 'Returns a string converted to uppercase characters.', 'Internal Function', '', '', 'SELECT upper(\'dataflow\') as result FROM TABLE', 'DATAFLOW', '', 'stream,batch', 1, '2019-08-09 21:59:23', '', '');

INSERT INTO `bksql_function_patameter_config` (`id`, `func_name`, `input_type`, `return_type`, `description`)
VALUES
  (8, 'count', '[\"int\"]', '[\"long\"]', ''),
  (9, 'count', '[\"double\"]', '[\"long\"]', ''),
  (10, 'count', '[\"long\"]', '[\"long\"]', ''),
  (11, 'count', '[\"float\"]', '[\"long\"]', ''),
  (12, 'count', '[\"string\"]', '[\"long\"]', ''),
  (13, 'last', '[\"int\"]', '[\"int\"]', ''),
  (14, 'last', '[\"double\"]', '[\"double\"]', ''),
  (15, 'last', '[\"long\"]', '[\"long\"]', ''),
  (16, 'last', '[\"float\"]', '[\"float\"]', ''),
  (17, 'last', '[\"string\"]', '[\"string\"]', ''),
  (18, 'min', '[\"int\"]', '[\"int\"]', ''),
  (19, 'min', '[\"double\"]', '[\"double\"]', ''),
  (20, 'min', '[\"long\"]', '[\"long\"]', ''),
  (21, 'min', '[\"float\"]', '[\"float\"]', ''),
  (22, 'min', '[\"int\"]', '[\"int\"]', ''),
  (23, 'min', '[\"double\"]', '[\"double\"]', ''),
  (24, 'min', '[\"long\"]', '[\"long\"]', ''),
  (25, 'min', '[\"float\"]', '[\"float\"]', ''),
  (26, 'min', '[\"string\"]', '[\"string\"]', ''),
  (27, 'max', '[\"int\"]', '[\"int\"]', ''),
  (28, 'max', '[\"double\"]', '[\"double\"]', ''),
  (29, 'max', '[\"long\"]', '[\"long\"]', ''),
  (30, 'max', '[\"float\"]', '[\"float\"]', ''),
  (31, 'max', '[\"int\"]', '[\"int\"]', ''),
  (32, 'max', '[\"double\"]', '[\"double\"]', ''),
  (33, 'max', '[\"long\"]', '[\"long\"]', ''),
  (34, 'max', '[\"float\"]', '[\"float\"]', ''),
  (35, 'max', '[\"string\"]', '[\"string\"]', ''),
  (36, 'sum', '[\"int\"]', '[\"double\"]', ''),
  (37, 'sum', '[\"double\"]', '[\"double\"]', ''),
  (38, 'sum', '[\"long\"]', '[\"double\"]', ''),
  (39, 'sum', '[\"float\"]', '[\"double\"]', ''),
  (40, 'avg', '[\"int\"]', '[\"double\"]', ''),
  (41, 'avg', '[\"double\"]', '[\"double\"]', ''),
  (42, 'avg', '[\"long\"]', '[\"double\"]', ''),
  (43, 'avg', '[\"float\"]', '[\"double\"]', ''),
  (44, 'trim', '[\"string\"]', '[\"string\"]', ''),
  (45, 'substring', '[\"string\", \"int\"]', '[\"string\"]', ''),
  (46, 'substring', '[\"string\", \"int\", \"int\"]', '[\"string\"]', ''),
  (47, 'lower', '[\"string\"]', '[\"string\"]', ''),
  (48, 'ltrim', '[\"string\"]', '[\"string\"]', ''),
  (49, 'upper', '[\"string\"]', '[\"string\"]', ''),
  (50, 'substring_index', '[\"string\", \"string\", \"int\"]', '[\"string\"]', ''),
  (51, 'concat_ws', '[\"string...\"]', '[\"string\"]', ''),
  (52, 'replace', '[\"string\", \"string\", \"string\"]', '[\"string\"]', ''),
  (53, 'char_length', '[\"string\"]', '[\"string\"]', ''),
  (54, 'split', '[\"string\", \"string\", \"int\"]', '[\"string\"]', ''),
  (55, 'rtrim', '[\"string\"]', '[\"string\"]', ''),
  (56, 'concat', '[\"string...\"]', '[\"string\"]', ''),
  (57, 'truncate', '[\"double\", \"int\"]', '[\"double\"]', ''),
  (58, 'truncate', '[\"float\", \"int\"]', '[\"float\"]', ''),
  (59, 'power', '[\"int\", \"int\"]', '[\"double\"]', ''),
  (60, 'power', '[\"double\", \"int\"]', '[\"double\"]', ''),
  (61, 'power', '[\"long\", \"int\"]', '[\"double\"]', ''),
  (62, 'power', '[\"float\", \"int\"]', '[\"double\"]', ''),
  (63, 'floor', '[\"int\"]', '[\"double\"]', ''),
  (64, 'floor', '[\"double\"]', '[\"double\"]', ''),
  (65, 'floor', '[\"long\"]', '[\"double\"]', ''),
  (66, 'floor', '[\"float\"]', '[\"double\"]', ''),
  (67, 'sqrt', '[\"int\"]', '[\"double\"]', ''),
  (68, 'sqrt', '[\"double\"]', '[\"double\"]', ''),
  (69, 'sqrt', '[\"long\"]', '[\"double\"]', ''),
  (70, 'sqrt', '[\"float\"]', '[\"double\"]', ''),
  (71, 'ceil', '[\"int\"]', '[\"double\"]', ''),
  (72, 'ceil', '[\"double\"]', '[\"double\"]', ''),
  (73, 'ceil', '[\"long\"]', '[\"double\"]', ''),
  (74, 'ceil', '[\"float\"]', '[\"double\"]', ''),
  (75, 'abs', '[\"int\"]', '[\"int\"]', ''),
  (76, 'abs', '[\"double\"]', '[\"double\"]', ''),
  (77, 'abs', '[\"long\"]', '[\"long\"]', ''),
  (78, 'abs', '[\"float\"]', '[\"float\"]', ''),
  (79, 'round', '[\"int\", \"int\"]', '[\"double\"]', ''),
  (80, 'round', '[\"double\", \"int\"]', '[\"double\"]', ''),
  (81, 'round', '[\"long\", \"int\"]', '[\"double\"]', ''),
  (82, 'round', '[\"float\", \"int\"]', '[\"double\"]', ''),
  (83, 'mod', '[\"long\", \"long\"]', '[\"int\"]', ''),
  (84, 'if', '[\"boolean\", \"int\", \"int\"]', '[\"int\"]', ''),
  (85, 'if', '[\"boolean\", \"float\", \"float\"]', '[\"float\"]', ''),
  (86, 'if', '[\"boolean\", \"double\", \"double\"]', '[\"double\"]', ''),
  (87, 'if', '[\"boolean\", \"long\", \"long\"]', '[\"long\"]', ''),
  (88, 'if', '[\"boolean\", \"string\", \"string\"]', '[\"string\"]', ''),
  (89, 'case when', '[\"\"]', '[\"\"]', '');

