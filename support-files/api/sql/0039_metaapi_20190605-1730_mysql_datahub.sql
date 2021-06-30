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

-- Running upgrade 0d6edf96ab0a -> 59f5ee4bc81e

CREATE TABLE IF NOT EXISTS platform_config (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    platform_name VARCHAR(32) NOT NULL,
    platform_alias VARCHAR(255) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (platform_name)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

ALTER TABLE data_processing ADD COLUMN platform VARCHAR(32) NOT NULL DEFAULT 'bkdata';

ALTER TABLE result_table ADD COLUMN count_freq_unit VARCHAR(32) NOT NULL DEFAULT 's';

ALTER TABLE result_table ADD COLUMN is_managed TINYINT(1) NOT NULL DEFAULT '1';

ALTER TABLE result_table ADD COLUMN platform VARCHAR(32) NOT NULL DEFAULT 'bkdata';

ALTER TABLE result_table ADD COLUMN data_category VARCHAR(32) DEFAULT '';

-- 字典表维护
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('storage', '存储', 1, '');

INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ("yyyy-MM-dd''T''HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", '2019-05-21T08:27:52', 0, 'd,h,m', '');


use bkdata_meta;

-- Running upgrade 0d6edf96ab0a -> 59f5ee4bc81e

CREATE TABLE IF NOT EXISTS platform_config (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    platform_name VARCHAR(32) NOT NULL,
    platform_alias VARCHAR(255) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (platform_name)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

ALTER TABLE data_processing ADD COLUMN platform VARCHAR(32) NOT NULL DEFAULT 'bkdata';

ALTER TABLE result_table ADD COLUMN count_freq_unit VARCHAR(32) NOT NULL DEFAULT 's';

ALTER TABLE result_table ADD COLUMN is_managed TINYINT(1) NOT NULL DEFAULT '1';

ALTER TABLE result_table ADD COLUMN platform VARCHAR(32) NOT NULL DEFAULT 'bkdata';

ALTER TABLE storage_result_table ADD COLUMN data_type VARCHAR(32);

-- Running upgrade cec42ca93f55 -> 58553addbbd2

ALTER TABLE result_table ADD COLUMN data_category VARCHAR(32) DEFAULT '';

-- 字典表维护
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('storage', '存储', 1, '');

INSERT INTO `time_format_config` (`time_format_name`, `time_format_alias`, `time_format_example`, `timestamp_len`,`format_unit`,`description`) VALUES ("yyyy-MM-dd''T''HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss", '2019-05-21T08:27:52', 0, 'd,h,m', '');
