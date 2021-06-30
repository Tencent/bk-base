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

INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('描述业务过程相关的标签，表示数据主题域、数据含义等','en','business tags are used to describe the business process which represent the data subject domain and meaning etc.','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('描述数据来源的组件、设备及系统相关的标签','en','source tags are used to describe data source-related components，equipments and systems','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('描述数据特征、类型相关的标签，比如：日志、指标、关系配置、属性配置、事件','en','descriptive tags are used to describe data characteristics and type, such as logs, indicators, events,  and configuration includes attribute and relational data','');

INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('业务标签','en','business tag','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('来源标签','en','source tag','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('数据描述','en','descriptive tag','');