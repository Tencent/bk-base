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

INSERT INTO `dm_category_config`
VALUES (45, 'other', '其他', 0, 3, NULL, 1, 1, 'admin', '2019-04-24 05:00:04', 'admin', '2019-04-24 15:42:00',
        '其他；');

ALTER TABLE access_raw_data ADD permission varchar(30) DEFAULT 'all' NULL COMMENT '数据限制，"all","read_only"';

DROP TABLE data_category_config;

ALTER TABLE access_raw_data DROP FOREIGN KEY access_raw_data_ibfk_1;

ALTER TABLE access_raw_data ADD FOREIGN KEY(storage_channel_id) REFERENCES databus_channel_cluster_config (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_processing ADD FOREIGN KEY(processing_type) REFERENCES processing_type_config (processing_type_name) ON UPDATE CASCADE;

ALTER TABLE data_processing ADD FOREIGN KEY(project_id) REFERENCES project_info (project_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_processing_relation MODIFY processing_id VARCHAR(255) NOT NULL;

ALTER TABLE data_processing_relation ADD FOREIGN KEY(processing_id) REFERENCES data_processing (processing_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_processing_relation ADD FOREIGN KEY(storage_cluster_config_id) REFERENCES storage_cluster_config (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_processing_relation ADD FOREIGN KEY(channel_cluster_config_id) REFERENCES databus_channel_cluster_config (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_transferring ADD FOREIGN KEY(transferring_type) REFERENCES transferring_type_config (transferring_type_name) ON UPDATE CASCADE;

ALTER TABLE data_transferring ADD FOREIGN KEY(project_id) REFERENCES project_info (project_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_transferring_relation MODIFY transferring_id VARCHAR(255) NOT NULL;

ALTER TABLE data_transferring_relation ADD FOREIGN KEY(channel_cluster_config_id) REFERENCES databus_channel_cluster_config (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_transferring_relation ADD FOREIGN KEY(transferring_id) REFERENCES data_transferring (transferring_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE data_transferring_relation ADD FOREIGN KEY(storage_cluster_config_id) REFERENCES storage_cluster_config (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE result_table ADD FOREIGN KEY(processing_type) REFERENCES processing_type_config (processing_type_name) ON UPDATE CASCADE;

ALTER TABLE result_table ADD FOREIGN KEY(project_id) REFERENCES project_info (project_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE result_table_field ADD FOREIGN KEY(result_table_id) REFERENCES result_table (result_table_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE result_table_field ADD FOREIGN KEY(field_type) REFERENCES field_type_config (field_type) ON UPDATE CASCADE;

ALTER TABLE storage_result_table ADD FOREIGN KEY(result_table_id) REFERENCES result_table (result_table_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE storage_result_table ADD FOREIGN KEY(storage_channel_id) REFERENCES databus_channel_cluster_config (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE storage_result_table ADD FOREIGN KEY(storage_cluster_config_id) REFERENCES storage_cluster_config (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE storage_scenario_config ADD COLUMN cluster_type VARCHAR(128);


SET NAMES utf8;

USE bkdata_basic;

DELETE FROM processing_type_config where processing_type_name = 'model';

INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('stream_model', '实时模型', 1, '');
INSERT INTO `processing_type_config` (`processing_type_name`, `processing_type_alias`, `active`, `description`) VALUES ('batch_model', '离线模型', 1, '');

