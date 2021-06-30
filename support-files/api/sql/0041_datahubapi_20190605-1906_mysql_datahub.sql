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

set NAMES utf8;

use bkdata_basic;

-- 存储场景
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("分析型数据库", "en", "Analytical Database", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("日志检索", "en", "Log Search", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("关系型数据库", "en", "Relational Database", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("分布式文件系统", "en", "Distributed Filesystem", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("消息队列", "en", "Queue", "storekit");


-- 集群配置
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("ES公共集群", "en", "ES common cluster", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("tsdb公共集群", "en", "tsdb common cluster", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("tspider公共集群", "en", "Tspider common cluster", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("MySQL公共集群", "en", "Mysql common cluster", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("HDFS公共集群", "en", "HDFS common cluster", "storekit");
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("queue公共集群", "en", "Queue common cluster", "storekit");

-- 集群过期时间
INSERT INTO content_language_config(content_key, language, content_value, description) VALUES("{\"min_expire\": 1, \"list_expire\": [{\"name\": \"1\\u5929\", \"value\": \"1d\"}, {\"name\": \"3\\u5929\", \"value\": \"3d\"}, {\"name\": \"7\\u5929\", \"value\": \"7d\"}, {\"name\": \"14\\u5929\", \"value\": \"14d\"}, {\"name\": \"30\\u5929\", \"value\": \"30d\"}], \"max_expire\": 30}", "en", "{\"min_expire\": 1, \"max_expire\": 30, \"list_expire\": [{\"name\": \"1day\", \"value\": \"1d\"},{\"name\": \"3days\", \"value\": \"3d\"},{\"name\": \"7days\", \"value\": \"7d\"},{\"name\": \"14days\", \"value\": \"14d\"},{\"name\": \"30days\", \"value\": \"30d\"}]}", "storekit");
