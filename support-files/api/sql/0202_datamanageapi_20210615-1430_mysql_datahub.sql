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

-- 数据模型设计相关表


CREATE TABLE IF NOT EXISTS `dmm_model_info` (
    model_id int(11) NOT NULL AUTO_INCREMENT COMMENT '模型ID',
    model_name varchar(255) NOT NULL COMMENT '模型名称，英文字母加下划线，全局唯一',
    model_alias varchar(255) NOT NULL COMMENT '模型别名',
    model_type varchar(32) NOT NULL COMMENT '模型类型，可选事实表、维度表',
    project_id int(11) NOT NULL COMMENT '项目ID',
    description text NULL COMMENT '模型描述',
    publish_status varchar(32) NOT NULL COMMENT '发布状态，可选 developing/published/re-developing',
    active_status varchar(32) NOT NULL DEFAULT 'active' COMMENT '可用状态, active/disabled/conflicting',
    # 主表信息
    table_name varchar(255) NOT NULL COMMENT '主表名称',
    table_alias varchar(255) NOT NULL COMMENT '主表别名',
    step_id int(11) NOT NULL DEFAULT 0 COMMENT '模型构建&发布完成步骤',
    latest_version_id varchar(64) NULL COMMENT '模型最新发布版本ID',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`model_id`),
    CONSTRAINT `dmm_model_info_unique_model_name` UNIQUE(model_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型主表';


CREATE TABLE IF NOT EXISTS `dmm_model_top` (
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
    model_id int(11) NOT NULL COMMENT '模型ID',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型用户置顶表';


CREATE TABLE IF NOT EXISTS `dmm_model_field` (
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    model_id int(11) NOT NULL COMMENT '模型ID',
    field_name varchar(255) NOT NULL COMMENT '字段名称',
    field_alias varchar(255) NOT NULL COMMENT '字段别名',
    field_type varchar(32) NOT NULL COMMENT '数据类型，可选 long/int/...',
    field_category varchar(32) NOT NULL COMMENT '字段类型，可选维度、度量',
    is_primary_key tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否是主键 0：否，1：是',
    description text NULL COMMENT '字段描述',
    /*
    -- constraint_id       COMMENT '字段约束，可选值范围/正则/邮箱/电话/...',
    -- constraint_content  COMMENT '约束参数，主要包括修正方式、单位、Range 大小值',
    -- 示例
        {
            "op": "OR",
            "groups": [
                {
                    "op": "AND",
                    "items": [
                        {"constraint_id": "", "constraint_content": ""},
                        {"constraint_id": "", "constraint_content": ""}
                    ]
                },
                {
                    "op": "OR",
                    "items": [
                        {"constraint_id": "", "constraint_content": ""},
                        {"constraint_id": "", "constraint_content": ""}
                    ]
                }
           ]
        }
    */
    field_constraint_content text NULL COMMENT '字段约束',
    /*
    -- clean_option   COMMENT '字段清洗选项，可选 SQL、映射表',
    -- clean_content  COMMENT '字段清洗内容'
    -- 示例
        {
            clean_option: 'SQL',
            clean_content: 'case ... when ... case ... when ...'
        }
    */
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
    CONSTRAINT `dmm_model_field_unique_model_field` UNIQUE(model_id, field_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型字段表';



CREATE TABLE IF NOT EXISTS `dmm_model_relation` (
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
    CONSTRAINT `dmm_model_relation_unique_model_relation` UNIQUE(model_id, related_model_id)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型关系表';


CREATE TABLE IF NOT EXISTS `dmm_model_calculation_atom` (
    model_id int(11) NOT NULL COMMENT '模型ID',
    project_id int(11) NOT NULL COMMENT '项目ID',
    calculation_atom_name varchar(255) NOT NULL COMMENT '统计口径名称，要求英文字母加下划线',
    calculation_atom_alias varchar(255) NOT NULL COMMENT '统计口径别名',
    origin_fields text NULL COMMENT '计算来源字段，若有计算逻辑，则该字段有效',
    description text NULL COMMENT '统计口径描述',
    field_type varchar(32) NOT NULL COMMENT '字段类型',
    /*
    -- table 示例（表单）
        {
            'option': 'TABLE',
            'content': {
                'calculation_field': 'price',
                'calculation_function': 'sum'
            }
        }
    -- SQL 示例
        {
            'option': 'SQL',
            'content': {
                'calculation_formula': 'sum(price)'
            }
        }
    */
    -- 用于前端展示
    calculation_content text NOT NULL COMMENT '统计方式',
    calculation_formula text NOT NULL COMMENT '统计SQL，例如：sum(price)',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`calculation_atom_name`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型统计口径表';


CREATE TABLE IF NOT EXISTS `dmm_model_calculation_atom_image` (
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
    model_id int(11) NOT NULL COMMENT '模型ID',
    project_id int(11) NOT NULL COMMENT '项目ID',
    calculation_atom_name varchar(255) NOT NULL COMMENT '统计口径名称，要求英文字母加下划线',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    CONSTRAINT `calculation_atom_image_unique_model_id_calculation_atom_name` UNIQUE(model_id, calculation_atom_name)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型统计口径影像表，对集市中统计口径的引用';


CREATE TABLE IF NOT EXISTS `dmm_model_indicator` (
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
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`indicator_name`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型指标表';

CREATE TABLE IF NOT EXISTS `dmm_model_release` (
    version_id varchar(64) NOT NULL COMMENT '模型版本ID',
    version_log text NOT NULL COMMENT '模型版本日志',
    model_id int(11) NOT NULL COMMENT '模型ID',
    /*
    -- 示例
        {
            "model_id": 23,
            "model_name": "fact_item_flow_15",
            "model_alias": "道具流水表",
            "description": "道具流水",
            "table_name": "fact_item_flow_15",
            "table_alias": "道具流水表",
            "tags": [
                {
                    "tag_alias": "xx",
                    "tag_code": "yy"
                }
            ],
            "publish_status": "developing",
            "active_status": "active",
            "created_by": "xx",
            "created_at": "2020-10-14 16:20:10",
            "updated_by": "xx",
            "updated_at": "2020-11-07 22:05:53",
            "model_type": "fact_table",
            "step_id": 4,
            "project_id": 4,
            "model_detail": {
                "fields": [],
                "calculation_atoms": [],
                "model_relation": [],
                "indicators": [],
            }
        }
    */
    model_content text NOT NULL COMMENT '模型版本内容',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_by varchar(50) DEFAULT NULL COMMENT '更新者',
    updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`version_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型发布版本';


-- 字典表
CREATE TABLE IF NOT EXISTS `dmm_field_constraint_config` (
    constraint_index int(11) NOT NULL COMMENT '约束index',
    constraint_id varchar(64) NOT NULL COMMENT '字段约束ID',
    constraint_type varchar(32) NOT NULL COMMENT '约束类型，可选general/specific',
    group_type varchar(32) NOT NULL COMMENT '约束分组',
    constraint_name varchar(64) NOT NULL COMMENT '约束名称',
    constraint_value text NULL COMMENT '约束规则，与数据质量打通，用于生成质量检测规则',
    /*
    -- type   COMMENT '类型校验',
    -- content  COMMENT '内容校验'
    -- 示例
        {
            "type": "string_validator",
            "content": {
                "regex": "^(\(|\[)(\-)?\d+\,(\-)?\d+(\)|\])$"
            }
        }
    */
    validator text NULL COMMENT '约束规则校验',
    description text NULL COMMENT '字段约束说明',
    editable tinyint(1) NOT NULL COMMENT '是否可以编辑 0：不可编辑，1：可以编辑',
    allow_field_type text NULL COMMENT '允许的字段数据类型',
    PRIMARY KEY (`constraint_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='字段约束表';

INSERT INTO `dmm_field_constraint_config` VALUES
(1,'eq','general',0,'=','1','{"content": null, "type": "number_validator"}',NULL,1,'["int", "long", "double", "float"]'),
(2,'not_eq','general',0,'!=','1','{"content": null, "type": "number_validator"}',NULL,1,'["int", "long", "double", "float"]'),
(3,'gt','general',0,'>','1','{"content": null, "type": "number_validator"}',NULL,1,'["int", "long", "double", "float"]'),
(4,'lt','general',0,'<','1','{"content": null, "type": "number_validator"}',NULL,1,'["int", "long", "double", "float"]'),
(5,'gte','general',0,'>=','1','{"content": null, "type": "number_validator"}',NULL,1,'["int", "long", "double", "float"]'),
(6,'lte','general',0,'<=','1','{"content": null, "type": "number_validator"}',NULL,1,'["int", "long", "double", "float"]'),
(7,'str_eq','general',0,'等于','qq','{"content": null, "type": "string_validator"}',NULL,1,'["string"]'),
(8,'str_not_eq','general',0,'不等于','qq','{"content": null, "type": "string_validator"}',NULL,1,'["string"]'),
(9,'strat_with','general',0,'开头是','http','{"content": null, "type": "string_validator"}',NULL,1,'["string"]'),
(10,'end_with','general',0,'结尾是','.com','{"content": null, "type": "string_validator"}',NULL,1,'["string"]'),
(11,'include','general',0,'包含','http','{"content": null, "type": "string_validator"}',NULL,1,'["string"]'),
(12,'not_include','general',0,'不包含','http','{"content": null, "type": "string_validator"}',NULL,1,'["string"]'),
(13,'not_null','general',0,'非空',NULL,NULL,NULL,0,'["int", "long", "double", "float", "string"]'),
(14,'value_enum','general',1,'枚举值','1,2,3','{"content": {"regex": "^[a-zA-Z0-9_\\\\u4e00-\\\\u9fa5]+(\\\\,(\\\\s)?[a-zA-Z0-9_\\\\u4e00-\\\\u9fa5]+)*$"}, "type": "string_validator"}','is one of',1,'["int", "long", "double", "float", "string"]'),
(15,'reverse_value_enum','general',1,'反向枚举值','1,2,3','{"content": {"regex": "^[a-zA-Z0-9_\\\\u4e00-\\\\u9fa5]+(\\\\,(\\\\s)?[a-zA-Z0-9_\\\\u4e00-\\\\u9fa5]+)*$"}, "type": "string_validator"}','is not one of',1,'["int", "long", "double", "float", "string"]'),
(16,'regex','general',1,'正则表达式','^[0-9]{1,2}$','{"content": null, "type": "regex_validator"}',NULL,1,'["int", "long", "double", "float", "string"]'),
(17,'email_format','specific',1,'邮箱','^[A-Za-z0-9\u4e00-\u9fa5]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+$',NULL,NULL,0,'["string"]'),
(18,'tel_number_format','specific',1,'中国地区移动电话','^((\+86|86|\[86\]|\(86\)|\+086|086|\[086\]|\(086\)|\+0086|0086|\[0086\]|\(0086\))\s)?([1]|[1])[345678][0-9]{9}$',NULL,NULL,0,'["string"]'),
(19,'id_number_format','specific',1,'中国身份证号','^([1-9]\d{5}[12]\d{3}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])\d{3}[0-9xX])$',NULL,NULL,0,'["string"]'),
(20,'ip_format','specific',1,'IP地址','^((?:(?:25[0-5]|2[0-4]\\d|[01]?\\d?\\d)\\.){3}(?:25[0-5]|2[0-4]\\d|[01]?\\d?\\d))$',NULL,NULL,0,'["string"]'),
(21,'qq_format','specific',1,'QQ号码','^[1-9][0-9]{4,}$',NULL,NULL,0,'["string"]'),
(22,'post_code_format',1,'specific','中国邮政编码','^[1-9]{1}(\d+){5}$',NULL,NULL,0,'["string"]'),
(23,'url_format','specific',1,'网址','(http[s]?:\/\/|[a-zA-Z0-9]{1,}\.{1}|\b)(?:\w{1,}\.{1}){1,5}(?:com|org|edu|gov|uk|net|ca|de|jp|fr|au|us|ru|ch|it|nl|se|no|es|mil|iq|io|ac|ly|sm){1}(?:\/[a-zA-Z0-9]{1,})*',NULL,NULL,0,'["string"]');


CREATE TABLE IF NOT EXISTS `dmm_calculation_function_config` (
    function_name varchar(64) NOT NULL COMMENT 'SQL 统计函数名称',
    output_type varchar(32) NOT NULL COMMENT '输出字段类型',
    allow_field_type text NOT NULL COMMENT '允许被聚合字段的数据类型',
    PRIMARY KEY (`function_name`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='SQL 统计函数表';

INSERT INTO `dmm_calculation_function_config` VALUES
('sum','long','["int", "long", "double", "float"]'),
('count','long','["int", "long", "double", "float", "string"]'),
('count_distinct','long','["int", "long", "double", "float", "string"]'),
('avg','long','["int", "long", "double", "float"]'),
('max','long','["int", "long", "double", "float"]'),
('min','long','["int", "long", "double", "float"]');

CREATE TABLE IF NOT EXISTS `dmm_model_user_operation_log` (
    id int(11) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    model_id int(11) NOT NULL COMMENT '模型ID',
    object_type varchar(32) NOT NULL COMMENT '操作对象类型',
    object_operation varchar(32) NOT NULL COMMENT '操作类型',
    object_id varchar(255) NOT NULL COMMENT '操作对象ID',
    object_name varchar(255) NOT NULL COMMENT '操作对象英文名',
    object_alias varchar(255) NOT NULL COMMENT '操作对象中文名',
    content_before_change text NULL COMMENT '操作前的模型内容',
    content_after_change text NULL COMMENT '操作后的模型内容',
    created_by varchar(50) NOT NULL COMMENT '创建者',
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    extra text NULL COMMENT '附加信息',
    PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据模型用户操作流水记录';
