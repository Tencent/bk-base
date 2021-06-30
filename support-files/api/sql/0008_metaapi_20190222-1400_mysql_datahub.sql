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

CREATE DATABASE IF NOT EXISTS bkdata_meta;

USE bkdata_meta;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS alembic_version (
    version_num VARCHAR(32) NOT NULL,
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 0b1f35d99379

CREATE TABLE IF NOT EXISTS belongs_to_config (
    belongs_id VARCHAR(255) NOT NULL,
    belongs_name VARCHAR(255) NOT NULL,
    belongs_alias VARCHAR(255),
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT,
    PRIMARY KEY (belongs_id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS cluster_group_config (
    cluster_group_id VARCHAR(255) NOT NULL,
    cluster_group_name VARCHAR(255) NOT NULL,
    cluster_group_alias VARCHAR(255),
    scope ENUM('private','public') NOT NULL,
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT,
    PRIMARY KEY (cluster_group_id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS content_language_config (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    content_key VARCHAR(511) NOT NULL,
    language VARCHAR(64) NOT NULL,
    content_value VARCHAR(511) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS data_category_config (
    id INTEGER(10) NOT NULL AUTO_INCREMENT,
    data_category_name VARCHAR(128) NOT NULL,
    data_category_alias VARCHAR(128),
    active TINYINT(1) NOT NULL DEFAULT '1',
    created_by VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS data_processing (
    project_id INTEGER(11) NOT NULL,
    processing_id VARCHAR(255) NOT NULL,
    processing_alias VARCHAR(255) NOT NULL,
    processing_type VARCHAR(32) NOT NULL,
    generate_type VARCHAR(32) NOT NULL DEFAULT 'user',
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (processing_id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS data_processing_del (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    processing_id VARCHAR(255) NOT NULL,
    processing_content TEXT NOT NULL,
    processing_type VARCHAR(32) NOT NULL,
    status VARCHAR(255) NOT NULL,
    disabled_by VARCHAR(50),
    disabled_at TIMESTAMP NULL,
    deleted_by VARCHAR(50) NOT NULL DEFAULT '',
    deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS data_processing_relation (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    data_directing VARCHAR(128),
    data_set_type VARCHAR(128),
    data_set_id VARCHAR(255),
    storage_cluster_config_id INTEGER(11),
    channel_cluster_config_id INTEGER(11),
    storage_type VARCHAR(32),
    processing_id VARCHAR(255),
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS data_transferring (
    project_id INTEGER(11) NOT NULL,
    transferring_id VARCHAR(255) NOT NULL,
    transferring_alias VARCHAR(255) NOT NULL,
    transferring_type VARCHAR(32) NOT NULL,
    generate_type VARCHAR(32) NOT NULL DEFAULT 'user',
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (transferring_id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS data_transferring_del (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    transferring_id VARCHAR(255) NOT NULL,
    transferring_content TEXT NOT NULL,
    transferring_type VARCHAR(32) NOT NULL,
    status VARCHAR(255) NOT NULL,
    disabled_by VARCHAR(50),
    disabled_at TIMESTAMP NULL,
    deleted_by VARCHAR(50) NOT NULL DEFAULT '',
    deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS data_transferring_relation (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    data_directing VARCHAR(128),
    data_set_type VARCHAR(128),
    data_set_id VARCHAR(255),
    storage_cluster_config_id INTEGER(11),
    channel_cluster_config_id INTEGER(11),
    storage_type VARCHAR(32) NOT NULL,
    transferring_id VARCHAR(255),
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS databus_channel_cluster_config (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    cluster_name VARCHAR(32) NOT NULL,
    cluster_type VARCHAR(32) NOT NULL,
    cluster_role VARCHAR(32) NOT NULL,
    cluster_domain VARCHAR(128) NOT NULL,
    cluster_backup_ips VARCHAR(128) NOT NULL,
    cluster_port INTEGER(8) NOT NULL,
    zk_domain VARCHAR(128) NOT NULL,
    zk_port INTEGER(8) NOT NULL,
    zk_root_path VARCHAR(128) NOT NULL,
    active TINYINT(1) DEFAULT '1',
    priority INTEGER(8) DEFAULT '0',
    attribute VARCHAR(128) DEFAULT 'bkdata',
    created_by VARCHAR(128),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(128),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (cluster_name)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS encoding_config (
    id INTEGER(10) NOT NULL AUTO_INCREMENT,
    encoding_name VARCHAR(128) NOT NULL,
    encoding_alias VARCHAR(128),
    active TINYINT(1) NOT NULL DEFAULT '1',
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS field_type_config (
    field_type VARCHAR(128) NOT NULL,
    field_type_name VARCHAR(128) NOT NULL,
    field_type_alias VARCHAR(128) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (field_type)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS job_status_config (
    status_id VARCHAR(255) NOT NULL,
    status_name VARCHAR(255) NOT NULL,
    status_alias VARCHAR(255),
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT,
    PRIMARY KEY (status_id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS processing_type_config (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    processing_type_name VARCHAR(255) NOT NULL,
    processing_type_alias VARCHAR(255) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (processing_type_name)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS project_cluster_group_config (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    project_id INTEGER(11) NOT NULL,
    cluster_group_id VARCHAR(256) NOT NULL,
    created_at DATETIME NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS project_data (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    project_id INTEGER(11) NOT NULL,
    bk_biz_id INTEGER(11) NOT NULL,
    result_table_id VARCHAR(128),
    active TINYINT(1) NOT NULL DEFAULT '1',
    created_at DATETIME NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS project_del (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    project_id INTEGER(11) NOT NULL,
    project_name VARCHAR(255) NOT NULL,
    project_content TEXT NOT NULL,
    status VARCHAR(255) NOT NULL,
    disabled_by VARCHAR(50),
    disabled_at TIMESTAMP NULL,
    deleted_by VARCHAR(50) NOT NULL DEFAULT '',
    deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS project_info (
    project_id INTEGER(7) NOT NULL AUTO_INCREMENT,
    project_name VARCHAR(255) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    bk_app_code VARCHAR(255) NOT NULL,
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    deleted_by VARCHAR(50),
    deleted_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (project_id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS result_table (
    bk_biz_id INTEGER(7) NOT NULL,
    project_id INTEGER(11),
    result_table_id VARCHAR(255) NOT NULL,
    result_table_name VARCHAR(255) NOT NULL,
    result_table_name_alias VARCHAR(255) NOT NULL,
    result_table_type VARCHAR(32),
    processing_type VARCHAR(32) NOT NULL,
    generate_type VARCHAR(32) NOT NULL DEFAULT 'user',
    sensitivity VARCHAR(32) DEFAULT 'public',
    count_freq INTEGER(11) NOT NULL DEFAULT '0',
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (result_table_id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS result_table_del (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    result_table_id VARCHAR(255) NOT NULL,
    result_table_content TEXT NOT NULL,
    status VARCHAR(255) NOT NULL,
    disabled_by VARCHAR(50),
    disabled_at TIMESTAMP NULL,
    deleted_by VARCHAR(50) NOT NULL DEFAULT '',
    deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS result_table_field (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    result_table_id VARCHAR(255) NOT NULL,
    field_index INTEGER(7) NOT NULL,
    field_name VARCHAR(255) NOT NULL,
    field_alias VARCHAR(255) NOT NULL,
    description TEXT,
    field_type VARCHAR(255) NOT NULL,
    is_dimension TINYINT(1) NOT NULL,
    origins VARCHAR(255),
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS result_table_type_config (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    result_table_type_name VARCHAR(255) NOT NULL,
    result_table_type_alias VARCHAR(255),
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT,
    PRIMARY KEY (id),
    UNIQUE (result_table_type_name)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS storage_cluster_config (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    cluster_name VARCHAR(128) NOT NULL,
    cluster_type VARCHAR(128),
    cluster_domain VARCHAR(255) NOT NULL,
    cluster_group VARCHAR(128) NOT NULL,
    connection_info TEXT,
    priority INTEGER(11) NOT NULL,
    version VARCHAR(128) NOT NULL,
    belongs_to VARCHAR(128) DEFAULT 'bkdata',
    created_by VARCHAR(128),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(128),
    updated_at TIMESTAMP NULL,
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS storage_cluster_expires_config (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    cluster_name VARCHAR(128) NOT NULL,
    cluster_type VARCHAR(128),
    cluster_subtype VARCHAR(128),
    expires TEXT,
    created_by VARCHAR(128),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(128),
    updated_at TIMESTAMP NULL,
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS storage_result_table (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    result_table_id VARCHAR(128) NOT NULL,
    storage_cluster_config_id INTEGER(11),
    physical_table_name VARCHAR(255),
    expires VARCHAR(45),
    storage_channel_id INTEGER(11),
    storage_config TEXT,
    active TINYINT(1) NOT NULL DEFAULT '1',
    priority INTEGER(11) NOT NULL DEFAULT '0',
    generate_type VARCHAR(32) NOT NULL DEFAULT 'user',
    created_by VARCHAR(128),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(128),
    updated_at TIMESTAMP NULL,
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS storage_scenario_config (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    storage_scenario_name VARCHAR(128) NOT NULL,
    storage_scenario_alias VARCHAR(128) NOT NULL,
    active TINYINT(1) DEFAULT '0',
    created_by VARCHAR(128),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(128),
    updated_at TIMESTAMP NULL,
    description TEXT,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS time_format_config (
    id INTEGER(10) NOT NULL AUTO_INCREMENT,
    time_format_name VARCHAR(128) NOT NULL,
    time_format_alias VARCHAR(128),
    time_format_example VARCHAR(128),
    timestamp_len INTEGER(11) NOT NULL DEFAULT '0',
    format_unit VARCHAR(12) DEFAULT 'd',
    active TINYINT(1) DEFAULT '1',
    created_by VARCHAR(50) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS transferring_type_config (
    id INTEGER(7) NOT NULL AUTO_INCREMENT,
    transferring_type_name VARCHAR(255) NOT NULL,
    transferring_type_alias VARCHAR(255) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT '1',
    description TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (transferring_type_name)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

CREATE TABLE IF NOT EXISTS access_raw_data (
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    bk_biz_id INTEGER(11) NOT NULL,
    raw_data_name VARCHAR(128) NOT NULL,
    raw_data_alias VARCHAR(128) NOT NULL,
    sensitivity VARCHAR(32) DEFAULT 'public',
    data_source VARCHAR(32) NOT NULL,
    data_encoding VARCHAR(32) DEFAULT 'UTF8',
    data_category VARCHAR(32) DEFAULT 'UTF8',
    data_scenario VARCHAR(128) NOT NULL,
    bk_app_code VARCHAR(128) NOT NULL,
    storage_partitions INTEGER(11),
    created_by VARCHAR(128),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_by VARCHAR(128),
    updated_at TIMESTAMP NULL,
    description TEXT NOT NULL,
    maintainer VARCHAR(255),
    storage_channel_id INTEGER(11),
    PRIMARY KEY (id),
    FOREIGN KEY(storage_channel_id) REFERENCES databus_channel_cluster_config (id)
)ENGINE=InnoDB CHARSET=utf8 COLLATE utf8_general_ci;

INSERT INTO alembic_version (version_num) VALUES ('0b1f35d99379');

ALTER TABLE storage_cluster_config DROP COLUMN cluster_domain;

UPDATE alembic_version SET version_num='e484ffb10eca' WHERE alembic_version.version_num = '0b1f35d99379';

