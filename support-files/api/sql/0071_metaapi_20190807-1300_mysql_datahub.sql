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

use bkdata_meta;

UPDATE dm_category_config SET category_name = 'operating_system' WHERE category_name = 'performance';
INSERT INTO `dm_category_config`(`id`, `category_name`, `category_alias`, `parent_id`, `seq_index`, `icon`, `active`, `visible`, `created_by`, `created_at`, `updated_by`, `updated_at`, `description`) VALUES (45, 'events', '事件数据', 1, 7, NULL, 1, 1, 'admin', '2019-08-06 15:26:56', 'admin', NULL, '业务监控告警、故障及业务正常状态下发出的公告类事件');

UPDATE `dm_category_config` SET `category_name` = 'notice', `category_alias` = '公告', `parent_id` = 45, `seq_index` = 1, `updated_by` = 'admin', `updated_at` = '2019-08-06 15:28:36', `description` = '公告事件，比如网络公告、营销活动公告等；' WHERE `id` = 10;
UPDATE `dm_category_config` SET `category_name` = 'alarm',  `category_alias` = '告警', `parent_id` = 45, `seq_index` = 2, `updated_by` = 'admin', `updated_at` = '2019-08-06 15:28:36', `description` = '业务告警事件' WHERE `id` = 9;
INSERT INTO `dm_category_config`(`id`, `category_name`, `category_alias`, `parent_id`, `seq_index`, `icon`, `active`, `visible`, `created_by`, `created_at`, `updated_by`, `updated_at`, `description`) VALUES (46, 'fault', '故障', 45, 3, NULL, 1, 1, 'admin', '2019-08-06 15:28:36', NULL, NULL, '业务故障事件');

update bkdata_meta.access_raw_data set data_category='operating_system' where data_category='performance';
update bkdata_basic.access_raw_data set data_category='operating_system' where data_category='performance';

use bkdata_basic;

update content_language_config set content_value='operating_system'where content_key='操作系统';
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('告警','en','alert','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('公告','en','notice','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('事件数据','en','events','');
INSERT INTO content_language_config (content_key, language, content_value, description) VALUES ('故障','en','fault','');