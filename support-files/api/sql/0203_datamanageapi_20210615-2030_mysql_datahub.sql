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

-- 数据模型设计草稿态相关表

CREATE TABLE IF NOT EXISTS `dmm_model_field_stage` (
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    model_id int(11) NOT NULL COMMENT '模型ID',
    field_name varchar(255) NOT NULL COMMENT '字段名称',
    field_alias varchar(255) NOT NULL COMMENT '字段别名',
    field_type varchar(32) NOT NULL COMMENT '数据类型，可选 long/int/...',
    field_category varchar(32) NOT NULL COMMENT '字段类型，可选维度、度量',
    is_primary_key tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否是主键 0：否，1：是',
    description text NULL COMMENT '字段描述',
    field_constraint_content text NULL COMMENT '字段约束',
    field_clean_content text NULL COMMENT '清洗规则',
    origin_fields text NULL COMMENT '计算来源字段，若有计算逻辑，则该字段有效',
    field_index int(11) NOT NULL COMMENT '字段位置',
    source_model_id int(11) NULL COMMENT '来源模型，若有则为扩展字段，目前仅针对维度扩展字段使用',
    source_field_name varchar(255) NULL COMMENT '来源字段，若有则为扩展字段，目前仅针对维度扩展字段使用',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    CONSTRAINT `dmm_model_field_stage_unique_model_field` UNIQUE(model_id, field_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型字段表草稿态临时表';

CREATE TABLE IF NOT EXISTS `dmm_model_relation_stage` (
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    model_id int(11) NOT NULL COMMENT '模型ID',
    field_name varchar(255) NOT NULL COMMENT '字段名称',
    related_model_id int(11) NOT NULL COMMENT '关联模型ID',
    related_field_name varchar(255) NOT NULL COMMENT '关联字段名称',
    related_method varchar(32) NOT NULL COMMENT '关联方式，可选 left-join/inner-join/right-join',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    CONSTRAINT `dmm_model_relation_stage_unique_model_relation` UNIQUE(model_id, related_model_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型关系表草稿态临时表';

CREATE TABLE IF NOT EXISTS `dmm_model_calculation_atom_image_stage` (
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
    model_id int(11) NOT NULL COMMENT '模型ID',
    project_id int(11) NOT NULL COMMENT '项目ID',
    calculation_atom_name varchar(255) NOT NULL COMMENT '统计口径名称，要求英文字母加下划线',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    CONSTRAINT `calc_atom_image_stage_unique_model_id_calculation_atom_name` UNIQUE(model_id, calculation_atom_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型统计口径影像草稿态临时表，对集市中统计口径的引用';

CREATE TABLE IF NOT EXISTS `dmm_model_indicator_stage` (
    indicator_id int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
    model_id int(11) NOT NULL COMMENT '模型ID',
    project_id int(11) NOT NULL COMMENT '项目ID',
    indicator_name varchar(255) NOT NULL COMMENT '指标名称，要求英文字母加下划线，不可修改，全局唯一',
    indicator_alias varchar(255) NOT NULL COMMENT '指标别名',
    description text NULL COMMENT '指标描述',
    calculation_atom_name varchar(255) NOT NULL COMMENT '统计口径名称',
    aggregation_fields text NOT NULL COMMENT '聚合字段列表，使用逗号隔开',
    filter_formula text NULL COMMENT '过滤SQL，例如：system="android" AND area="sea"',
    condition_fields text NULL COMMENT '过滤字段，若有过滤逻辑，则该字段有效',
    scheduling_type varchar(32) NOT NULL COMMENT '计算类型，可选 stream、batch',
    scheduling_content text NOT NULL COMMENT '调度内容',
    parent_indicator_name varchar(255) NULL COMMENT '默认为NULL，表示直接从明细表里派生，不为NULL，标识从其他指标派生',
    hash varchar(64) NULL COMMENT '指标关键参数对应的hash值',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`indicator_id`),
    CONSTRAINT `indicator_stage_unique_model_id_indicator_name` UNIQUE(model_id, indicator_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型指标表草稿态临时表';

alter table dmm_model_relation add related_model_version_id varchar(64) NULL COMMENT '维度模型版本ID';

alter table dmm_model_indicator add hash varchar(64) NULL COMMENT '指标关键参数对应的hash值';
