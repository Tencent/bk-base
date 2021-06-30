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

USE bkdata_basic;

ALTER TABLE result_table MODIFY COLUMN `sensitivity` varchar(32) DEFAULT 'private' COMMENT '敏感性 public/private/sensitive';
ALTER TABLE result_table MODIFY COLUMN `project_id` varchar(32) NOT NULL;
ALTER TABLE project_info MODIFY COLUMN `project_id` int(11) AUTO_INCREMENT COMMENT '项目id';

INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('view', '视图', 1, '');

INSERT IGNORE INTO `content_language_config` (`content_key`, `language`, `content_value`, `active`, `description`)
VALUES
	('Internal Function','zh-cn','内置函数',1,'dataflow'),
	('Predefined function','zh-cn','预先定义的函数',1,'dataflow'),
	('Custom Function','zh-cn','自定义函数',1,'dataflow'),
	('To be open','zh-cn','待开放',1,'dataflow'),

	('String Function','zh-cn','字符串函数',1,'dataflow'),
	('String processing functions','zh-cn','进行字符串处理的函数',1,'dataflow'),
	('Mathematical Function','zh-cn','数学函数',1,'dataflow'),
	('Commonly used mathematical functions','zh-cn','常用的数学函数',1,'dataflow'),
	('Conditional Function','zh-cn','条件函数',1,'dataflow'),
	('Commonly used conditional judgment functions','zh-cn','常用的条件判断函数',1,'dataflow'),
	('Aggregate Function','zh-cn','聚合函数',1,'dataflow'),
	('Calculate a set of values and return a single value','zh-cn','对一组值进行计算，并返回单个值',1,'dataflow'),

	('Count the number of input numbers.','zh-cn','返回输入数的个数。',1,'dataflow'),
	('Return the last data.','zh-cn','返回最后一条数据。',1,'dataflow'),
	('Returns the minimum value of the input value.','zh-cn','返回输入值的最小值。',1,'dataflow'),
	('Returns the maximum value of the input value.','zh-cn','返回输入值的最大值。',1,'dataflow'),
	('Returns the sum of the values between all input values.','zh-cn','返回所有输入值之间的数值之和。',1,'dataflow'),
	('Returns the average number.','zh-cn','返回平均数。',1,'dataflow'),

	('Remove the white space on both sides.','zh-cn','移除两侧空白字符。',1,'dataflow'),
	('Returns a string converted to uppercase characters.','zh-cn','返回转换为大写字符的字符串。',1,'dataflow'),
	('Returns a string converted to a lowercase character.','zh-cn','返回转换为小写字符的字符串。',1,'dataflow'),
	('Remove the left blank character.','zh-cn','移除左侧空白字符。',1,'dataflow'),
	('Returns the string before the nth delimiter delim in the string str. If n is a positive number, return from the last (counting from the left) delimiter to all characters on the left. If count is negative, return from the last (counting from the right) to all characters on the right.','zh-cn','返回字符串str中在第n个出现的分隔符delim之前的字串。如果n是一个正数，返回从最后的（从左边开始计数）分隔符到左边所有字符。如果count是负数，返回从最后的（从右边开始计数）分隔到右边所有字符。',1,'dataflow'),
	('Connect each parameter value and the delimiter specified by the first parameter separator into a new string.','zh-cn','将每个参数值和第一个参数separator指定的分隔符依次连接到一起组成新的字符串。',1,'dataflow'),
	('Replace the substring of the string str with the regular pattern pattern with the string replacement and return the new string. Matching is being replaced, the argument is null or it is legally invalid.','zh-cn','用字符串replacement替换字符串str中正则模式为pattern的子串，返回新的字符串。正在匹配替换， 参数为null或者正则不合法返回null。',1,'dataflow'),
	('Gets a string substring, intercepts a substring of length len from position start, and if len is not specified it intercepts the end of the string. start starts from 1, and starts with zero as 1 when viewed.','zh-cn','获取字符串子串，截取从位置start开始长度为len的子串，若未指定len则截取到字符串结尾。start 从1开始，start为零当1看待。',1,'dataflow'),
	('Returns the number of characters in the string.','zh-cn','返回字符串中的字符数。',1,'dataflow'),
	('Sep as a delimiter, the string str is divided into several segments, which take the index segment, can not return NULL, index from 0.','zh-cn','以sep作为分隔符，将字符串str分隔成若干段，取其中的第index段，取不到返回NULL，index从0开始。',1,'dataflow'),
	('Remove the white space character on the right.','zh-cn','移除右侧空白字符。',1,'dataflow'),
	('Splicing two or more string values to form a new string.','zh-cn','连接两个或多个字符串值从而组成一个新的字符串。',1,'dataflow'),

	('Preserve the number of decimal places and specify the number of digits.','zh-cn','保留数值小数点后指定位数。',1,'dataflow'),
	('Returns the largest integer less than or equal to a number.','zh-cn','返回小于或等于数字的最大整数。',1,'dataflow'),
	('Returns the (n)th power of field a.','zh-cn','返回字段a的n次方。',1,'dataflow'),
	('Returns the square root of the number.','zh-cn','返回数字的平方根。',1,'dataflow'),
	('Returns the smallest integer greater than or equal to a number.','zh-cn','返回大于或等于数字的最小整数。',1,'dataflow'),
	('Take the absolute value.','zh-cn','取绝对值。',1,'dataflow'),
	('Round off the value, round off the field a and retain n decimal places.','zh-cn','四舍五入取值，对字段a四舍五入取值并保留小数点后n位。',1,'dataflow'),
	('Find the remainder and return the remainder of dividing field a by b.','zh-cn','求余数，返回对字段a除以b的余数。',1,'dataflow'),

	('The Boolean value of the first parameter is the judgment criterion. If it is true, the second parameter is returned; if it is false, the third parameter is returned.','zh-cn','以第一个参数的布尔值为判断标准，若为true，则返回第二个参数，若为false，则返回第三个参数。',1,'dataflow'),
	('If a is TRUE, b is returned; if c is TRUE, d is returned; otherwise, e is returned.','zh-cn','如果a为TRUE，则返回b；如果c为TRUE，则返回d；否则返回e 。',1,'dataflow');

use bkdata_meta;

ALTER TABLE `databus_channel_cluster_config` ADD ip_list text COMMENT 'ip列表';