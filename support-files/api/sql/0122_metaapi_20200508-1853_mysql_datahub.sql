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

set names utf8;

-- -----------------------------------------------------
-- Schema bkdata_meta
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `bkdata_meta` DEFAULT CHARACTER SET utf8 ;
-- -----------------------------------------------------
-- Schema bkdata_meta
-- -----------------------------------------------------

-- CREATE SCHEMA IF NOT EXISTS `bkdata_meta` DEFAULT CHARACTER SET utf8 ;
USE `bkdata_meta` ;

-- -----------------------------------------------------
-- Table `bkdata_meta`.`scene_info`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`scene_info` (
  `scene_name` VARCHAR(64) NOT NULL,
  `scene_alias` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `scene_index` INT(11) NOT NULL DEFAULT 1,
  `scene_type` VARCHAR(32) NOT NULL,
  `scene_level` INT(11) NOT NULL DEFAULT 1,
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing' COMMENT 'developing/finished',
  `brief_info` VARCHAR(255) NULL DEFAULT '',
  `properties` TEXT NULL,
  `parent_scene_name` VARCHAR(64) NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`scene_name`),
  UNIQUE INDEX `scene_alias_UNIQUE` (`scene_alias` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_template`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_template` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `template_name` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `properties` TEXT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing',
  `active` TINYINT NOT NULL DEFAULT 1,
  `scene_name` VARCHAR(64) NOT NULL,
  `template_alias` VARCHAR(64) NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`modeling_step`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`modeling_step` (
  `step_name` VARCHAR(64) NOT NULL,
  `step_alias` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `brief_info` VARCHAR(255) NULL,
  `step_type` VARCHAR(32) NULL,
  `step_level` INT NOT NULL DEFAULT 1,
  `step_index` INT NOT NULL DEFAULT 1,
  `step_icon` VARCHAR(255) NULL,
  `properties` TEXT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `parent_step_name` VARCHAR(64) NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`step_name`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`action_type`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`action_type` (
  `action_type` VARCHAR(64) NOT NULL,
  `action_type_alias` VARCHAR(64) NOT NULL,
  `action_type_index` INT NOT NULL DEFAULT 1,
  `parent_action_type` VARCHAR(64) NOT NULL,
  `icon` VARCHAR(255) NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`action_type`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`algorithm`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`algorithm` (
  `algorithm_name` VARCHAR(64) NOT NULL,
  `algorithm_alias` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `algorithm_type` VARCHAR(32) NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`algorithm_name`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`action_info`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`action_info` (
  `action_name` VARCHAR(64) NOT NULL,
  `action_alias` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing',
  `input_config` TEXT NULL,
  `output_config` TEXT NULL,
  `logic_config` TEXT NOT NULL,
  `target_id` VARCHAR(64) NULL,
  `target_type` VARCHAR(64) NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`action_name`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_template_node`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_template_node` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `node_name` VARCHAR(64) NOT NULL DEFAULT '',
  `node_alias` VARCHAR(64) NOT NULL DEFAULT '',
  `node_index` INT NOT NULL DEFAULT 1,
  `default_node_config` MEDIUMTEXT NULL,
  `properties` TEXT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `template_id` INT NOT NULL,
  `step_name` VARCHAR(64) NOT NULL,
  `action_name` VARCHAR(64) NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`scene_step_action`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`scene_step_action` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `action_name` VARCHAR(64) NOT NULL,
  `step_name` VARCHAR(64) NOT NULL,
  `scene_name` VARCHAR(64) NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `bkdata_meta`.`action_input_arg`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`action_input_arg` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `arg_name` VARCHAR(64) NOT NULL,
  `action_name` VARCHAR(64) NOT NULL,
  `arg_alias` VARCHAR(64) NOT NULL,
  `arg_index` INT NOT NULL DEFAULT 1,
  `data_type` VARCHAR(32) NOT NULL,
  `properties` TEXT NULL,
  `description` TEXT NULL,
  `default_value` TEXT NULL,
  `advance_config` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`action_visual`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`action_visual` (
  `action_name` VARCHAR(64) NOT NULL,
  `id` INT NOT NULL AUTO_INCREMENT,
  `visual_name` VARCHAR(64) NOT NULL,
  `visual_index` INT NULL,
  `brief_info` VARCHAR(255) NULL,
  `scene_name` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`action_visual_component`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`action_visual_component` (
  `component_name` VARCHAR(64) NOT NULL,
  `component_alias` VARCHAR(64) NOT NULL,
  `component_type` VARCHAR(45) NULL,
  `description` MEDIUMTEXT NULL,
  `remark` TEXT NULL,
  `properties` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`component_name`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_info`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_info` (
  `model_id` VARCHAR(128) NOT NULL,
  `model_name` VARCHAR(64) NOT NULL,
  `model_alias` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `project_id` INT NOT NULL,
  `model_type` VARCHAR(64) NOT NULL DEFAULT 'process' COMMENT 'process / modelflow / 还有复合模型等等',
  `sensitivity` VARCHAR(32) NOT NULL DEFAULT 'private',
  `active` TINYINT NOT NULL DEFAULT 1,
  `properties` TEXT NULL,
  `scene_name` VARCHAR(64) NOT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing' COMMENT 'developing/finished/release',
  `train_mode` VARCHAR(32) NOT NULL DEFAULT 'supervised',
  `sample_set_id` INT NULL,
  `input_standard_config` TEXT NULL COMMENT '输入的标准',
  `output_standard_config` TEXT NULL COMMENT '输出的标准',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`model_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_experiment`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_experiment` (
  `experiment_id` INT NOT NULL AUTO_INCREMENT,
  `experiment_name` VARCHAR(64) NOT NULL,
  `experiment_alias` VARCHAR(64) NOT NULL,
  `description` TEXT NULL,
  `project_id` INT NOT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing' COMMENT 'developing/finished/release',
  `pass_type` VARCHAR(32) NOT NULL DEFAULT 'not_confirmed' COMMENT 'not_confirmed/failed/pass',
  `active` TINYINT NOT NULL DEFAULT 1,
  `experiment_config` MEDIUMTEXT NULL,
  `model_file_config` TEXT NULL,
  `execute_config` TEXT NULL,
  `properties` TEXT NULL,
  `model_id` VARCHAR(64) NOT NULL,
  `template_id` INT NOT NULL,
  `experiment_index` INT NOT NULL DEFAULT 1,
  `sample_latest_time` TIMESTAMP NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`experiment_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_experiment_node`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_experiment_node` (
  `node_id` VARCHAR(128) NOT NULL,
  `model_id` VARCHAR(128) NOT NULL,
  `model_experiment_id` INT NOT NULL,
  `node_name` VARCHAR(64) NOT NULL,
  `node_alias` VARCHAR(64) NOT NULL,
  `node_config` MEDIUMTEXT NULL,
  `properties` TEXT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `step_name` VARCHAR(64) NOT NULL,
  `node_index` INT NOT NULL DEFAULT 1,
  `action_name` VARCHAR(64) NOT NULL,
  `node_role` VARCHAR(32) NOT NULL,
  `input_config` TEXT NULL,
  `output_config` TEXT NULL,
  `execute_config` TEXT NULL,
  `run_status` VARCHAR(32) NOT NULL DEFAULT 'not_run' COMMENT 'not_run / pending / running /  success / failed',
  `operate_status` VARCHAR(32) NOT NULL DEFAULT 'init' COMMENT ' init / updated / finished',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`node_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_release`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_release` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `model_experiment_id` INT NOT NULL,
  `model_id` VARCHAR(128) NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `description` TEXT NULL,
  `publish_status` VARCHAR(32) NOT NULL DEFAULT 'latest',
  `model_config_template` MEDIUMTEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_instance`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_instance` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `model_release_id` INT NOT NULL,
  `model_id` VARCHAR(128) NOT NULL,
  `model_experiment_id` INT NOT NULL,
  `project_id` INT NOT NULL,
  `flow_id` INT DEFAULT NULL,
  `flow_node_id` INT DEFAULT NULL,
  `instance_config` MEDIUMTEXT NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `upgrade_config` TEXT NULL,
  `sample_feedback_config` TEXT NULL,
  `serving_mode` VARCHAR(32) NOT NULL DEFAULT 'realtime',
  `instance_status` VARCHAR(32) NULL,
  `execute_config` TEXT NULL,
  `data_processing_id` VARCHAR(255) NOT NULL,
  `input_result_table_ids` TEXT NULL,
  `output_result_table_ids` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_session`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_session` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `model_experiment_node_id` INT NOT NULL,
  `session_key` VARCHAR(32) NOT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'init',
  `session_data_config` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`modeling_task`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`modeling_task` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `task_type` VARCHAR(32) NULL DEFAULT 'serving',
  `task_mode` VARCHAR(32) NULL DEFAULT 'realtime',
  `executor_type` VARCHAR(32) NULL DEFAULT 'local / jobnavi',
  `executor_config` TEXT NULL COMMENT 'local / jobnavi config',
  `owner_id` INT NOT NULL COMMENT '外键bkdata_meta.model_instance.id',
  `status` VARCHAR(32) NOT NULL DEFAULT 'no-start',
  `task_config` TEXT NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `schedule_type` VARCHAR(32) NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_experiment_result`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_experiment_result` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `model_experiment_id` INT NOT NULL,
  `evaluation_result` MEDIUMTEXT NOT NULL,
  `sample_set_id` INT NOT NULL,
  `sample_latest_time` TIMESTAMP NULL,
  `evaluation_status` VARCHAR(32) NOT NULL DEFAULT 'not_confirmed' COMMENT '与 experiment pass_type 一致，not_confirmed / pass / failed',
  `trained_result_info` TEXT NULL,
  `trained_model_content` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;



-- -----------------------------------------------------
-- Table `bkdata_meta`.`action_visual_component_relation`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`action_visual_component_relation` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `action_visual_id` INT NOT NULL ,
  `component_name` VARCHAR(64) NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_template_node_upstream`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_template_node_upstream` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `node_id` INT NOT NULL,
  `parent_node_id` INT NOT NULL,
  `properties` TEXT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `complete_type` VARCHAR(32) NOT NULL DEFAULT 'success',
  `concurrent_type` VARCHAR(32) NOT NULL DEFAULT 'sequence',
  `execute_index` INT NOT NULL DEFAULT 1,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`model_experiment_node_upstream`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`model_experiment_node_upstream` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `properties` TEXT NULL,
  `complete_type` VARCHAR(32) NOT NULL DEFAULT 'success',
  `concurrent_type` VARCHAR(32) NOT NULL DEFAULT 'sequence',
  `execute_index` INT NOT NULL DEFAULT 1,
  `node_id` VARCHAR(128) NOT NULL,
  `model_id` VARCHAR(128) NOT NULL,
  `parent_node_id` VARCHAR(128) NOT NULL,
  `model_experiment_id` INT NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`feature_model_experiment`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_model_experiment` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `feature_id` INT NOT NULL,
  `model_experiment_id` INT NOT NULL,
  `feature_index` INT NOT NULL DEFAULT 1,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`sample_set_model_experiment`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`sample_set_model_experiment` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sample_set_id` INT NOT NULL,
  `model_experiment_id` INT NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`action_output_field`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`action_output_field` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `action_name` VARCHAR(64) NOT NULL,
  `field_name` VARCHAR(64) NOT NULL,
  `field_alias` VARCHAR(64) NOT NULL,
  `field_index` INT NOT NULL DEFAULT 1,
  `data_type` VARCHAR(32) NOT NULL,
  `properties` TEXT NULL,
  `description` TEXT NULL,
  `default_value` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`modeling_task_execute_log`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`modeling_task_execute_log` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `modeling_task_id` INT NOT NULL,
  `execute_id` INT NULL,
  `status` VARCHAR(32) NULL,
  `task_result` VARCHAR(32) NOT NULL,
  `start_time` TIMESTAMP NULL,
  `end_time` TIMESTAMP NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`modeling_task_execute_result`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`modeling_task_execute_result` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `task_execute_id` INT NOT NULL COMMENT 'modeling_task_execute_log.id',
  `modeling_task_id` INT NOT NULL ,
  `task_result` VARCHAR(32) NULL,
  `task_result_content` MEDIUMTEXT NULL,
  `result_type` VARCHAR(32) NULL,
  `err_message` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `bkdata_meta`.`aggregation_method`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`aggregation_method` (
  `agg_name` VARCHAR(32) NOT NULL COMMENT '聚合名,主键',
  `agg_name_alias` VARCHAR(64) NULL DEFAULT NULL COMMENT '聚合中文名',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`agg_name`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`sample_set`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`sample_set` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sample_set_name` VARCHAR(255) NOT NULL,
  `sample_set_alias` VARCHAR(255) NOT NULL,
  `description` VARCHAR(255) NULL,
  `sensitivity` VARCHAR(32) NOT NULL,
  `project_id` INT(11) NOT NULL,
  `scene_name` VARCHAR(64) NOT NULL,
  `sample_latest_time` TIMESTAMP NULL,
  `sample_feedback` TINYINT NOT NULL,
  `sample_feedback_config` TEXT NULL COMMENT '样本反馈配置',
  `properties` TEXT NULL,
  `sample_size` BIGINT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `sample_type` VARCHAR(45) NOT NULL DEFAULT 'timeseries' COMMENT '样本集的类型,暂时是 时序类型的',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`storage_sample_set`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`storage_sample_set` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sample_set_id` INT NOT NULL,
  `physical_table_name` VARCHAR(64) NOT NULL,
  `expires` VARCHAR(32) NULL,
  `storage_config` TEXT NOT NULL,
  `storage_cluster_config_id` INT(11) NOT NULL,
  `storage_cluster_name` VARCHAR(32) NOT NULL,
  `storage_cluster_type` VARCHAR(32) NOT NULL,
  `active` TINYINT NULL,
  `priority` INT(11) NOT NULL DEFAULT 0 COMMENT '优先级, 优先级越高, 先使用这个存储',
  `generate_type` VARCHAR(32) NOT NULL DEFAULT 'system' COMMENT '用户还是系统产生的user / system',
  `content_type` VARCHAR(32) NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`sample_result_table`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`sample_result_table` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `result_table_id` VARCHAR(255) NOT NULL,
  `sample_set_id` INT NOT NULL,
  `fields_mapping` TEXT NULL DEFAULT NULL COMMENT '字段映射信息,json结构',
  `bk_biz_id` INT NOT NULL COMMENT '样本集业务',
  `properties` TEXT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`timeseries_sample_fragments`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`timeseries_sample_fragments` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `start_time` TIMESTAMP NULL,
  `end_time` TIMESTAMP NULL,
  `generate_type` VARCHAR(32) NOT NULL DEFAULT 'manual' COMMENT '来源manual还是sample_feedback',
  `labels` TEXT NULL COMMENT '这个片段的labels信息, {“start_time”:xx, “end_time”: xxx, \"anomaly_label\": 1, ”remark\": \"备注”,  \"anomaly_label_type\": “manual”}',
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing',
  `labels_type` VARCHAR(32) NOT NULL DEFAULT 'timeseries' COMMENT 'Label的类别,默认是teimseries',
  `result_table_id` VARCHAR(255) NOT NULL,
  `sample_set_id` INT(11) NOT NULL,
  `line_id` VARCHAR(255) NOT NULL,
  `group_fields` TEXT NULL,
  `group_dimension` TEXT NULL,
  `properties` TEXT NULL,
  `sample_size` BIGINT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '时序数据片段存储';


-- -----------------------------------------------------
-- Table `bkdata_meta`.`timeseries_sample_label`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`timeseries_sample_label` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `start` TIMESTAMP NULL,
  `end` TIMESTAMP NULL,
  `remark` TEXT NULL,
  `anomaly_label_type` VARCHAR(64) NOT NULL,
  `result_table_id` VARCHAR(255) NOT NULL,
  `sample_set_id` INT(11) NOT NULL,
  `line_id` VARCHAR(255) NOT NULL,
  `anomaly_label` VARCHAR(64) NOT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'init',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '时序异常标注存储';


-- -----------------------------------------------------
-- Table `bkdata_meta`.`sample_features`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`sample_features` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sample_set_id` INT NOT NULL,
  `field_name` VARCHAR(255) NOT NULL,
  `field_alias` VARCHAR(255) NOT NULL,
  `description` VARCHAR(255) NOT NULL,
  `field_index` INT(11) NOT NULL,
  `is_dimension` TINYINT NOT NULL DEFAULT 0,
  `field_type` VARCHAR(32) NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `origin` TEXT NULL COMMENT '从哪些原始的样本集字段计算出来的, 列表 []',
  `attr_type` VARCHAR(32) NOT NULL COMMENT '字段的属性类型 feature / metric / label / ts_field',
  `properties` TEXT NULL,
  `generate_type` VARCHAR(32) NOT NULL COMMENT '原生 还是 生成的, origin / generate',
  `field_evaluation` TEXT NULL COMMENT '字段评估结果,如特征重要性 importance 和 质量评分 quality',
  `parents` TEXT NULL COMMENT '父字段, 列表 []',
  `feature_transform_node_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '特征来自于哪个特征准备的节点, 节点id',
  `require_type` VARCHAR(64) NOT NULL COMMENT 'required / not_required / inner_used',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY  `sample_set_id` (`sample_set_id`,`field_name`))
ENGINE = InnoDB
COMMENT = '样本集特征列表';


-- -----------------------------------------------------
-- Table `bkdata_meta`.`timeseries_sample_config`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`timeseries_sample_config` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sample_set_id` INT NOT NULL,
  `ts_depend` VARCHAR(32) NOT NULL DEFAULT '0d' COMMENT '历史依赖',
  `ts_freq` VARCHAR(32) NOT NULL DEFAULT 0 COMMENT '统计频率',
  `ts_depend_config` TEXT NULL DEFAULT NULL,
  `ts_freq_config` TEXT NULL DEFAULT NULL,
  `description` VARCHAR(255) NULL COMMENT '描述',
  `properties` TEXT NULL,
  `ts_config_status` VARCHAR(32) NOT NULL DEFAULT 'not_confirmed' COMMENT 'not_confirmed / confirmed',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '存放时序类型的样本集配置';


-- -----------------------------------------------------
-- Table `bkdata_meta`.`user_operation_log`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`user_operation_log` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `operation` VARCHAR(64) NOT NULL,
  `module` VARCHAR(64) NOT NULL,
  `operator` VARCHAR(64) NOT NULL,
  `description` VARCHAR(255) NOT NULL,
  `operation_result` VARCHAR(64) NOT NULL,
  `errors` TEXT NULL,
  `extra` TEXT NULL,
  `object` VARCHAR(255) NOT NULL COMMENT '操作的对象',
  `object_id` VARCHAR(255) NOT NULL COMMENT '对象的id',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `bkdata_meta`.`storage_model`
-- -----------------------------------------------------

CREATE TABLE IF NOT EXISTS `bkdata_meta`.`storage_model` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `physical_table_name` VARCHAR(64) NOT NULL,
  `expires` VARCHAR(32) NULL,
  `storage_config` TEXT NOT NULL,
  `storage_cluster_config_id` INT(11) NOT NULL,
  `storage_cluster_name` VARCHAR(32) NOT NULL,
  `storage_cluster_type` VARCHAR(32) NOT NULL,
  `active` TINYINT NULL,
  `priority` INT(11) NOT NULL DEFAULT 0 COMMENT '优先级, 优先级越高, 先使用这个存储',
  `generate_type` VARCHAR(32) NOT NULL DEFAULT 'system' COMMENT '用户还是系统产生的user / system',
  `content_type` VARCHAR(32) NULL DEFAULT 'model',
  `model_id` VARCHAR(128) NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`algorithm_type` (
  `algorithm_type` VARCHAR(64) NOT NULL,
  `algorithm_type_alias` VARCHAR(45) NOT NULL,
  `algorithm_type_index` INT NOT NULL,
  `parent_algorithm_type` VARCHAR(45) NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`algorithm_type`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_template` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `template_name` VARCHAR(64) NOT NULL,
  `template_alias` VARCHAR(255) NOT NULL,
  `status` VARCHAR(32) NOT NULL,
  `scene_name` VARCHAR(64) NOT NULL DEFAULT 'developing',
  `properties` TEXT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_template_node` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `node_name` VARCHAR(255) NOT NULL,
  `node_alias` VARCHAR(255) NOT NULL,
  `node_index` INT NOT NULL,
  `default_node_config` MEDIUMTEXT NULL,
  `properties` TEXT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `step_name` VARCHAR(64) NOT NULL,
  `action_name` VARCHAR(64) NOT NULL,
  `feature_template_id` INT NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature` (
  `feature_name` VARCHAR(255) NOT NULL,
  `feature_alias` VARCHAR(255) NOT NULL,
  `feature_type` VARCHAR(64) NOT NULL,
  `description` VARCHAR(255) NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`feature_name`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_transform_info` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sample_set_id` INT NOT NULL,
  `description` VARCHAR(255) NULL,
  `status` VARCHAR(32) NULL,
  `scene_name` VARCHAR(64) NOT NULL,
  `active` TINYINT NULL,
  `properties` TEXT NULL,
  `run_status` VARCHAR(32) NOT NULL DEFAULT 'not_run' COMMENT 'not_run / pending / running /  success / failed',
  `feature_template_id` INT NOT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_transform_node_upstream` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `feature_transform_node_id` VARCHAR(128) NOT NULL,
  `parent_feature_transform_node_id` VARCHAR(128) NOT NULL,
  `properties` TEXT NULL DEFAULT NULL,
  `concurrent_type` VARCHAR(32) NOT NULL DEFAULT 'sequence',
  `complete_type` VARCHAR(32) NOT NULL DEFAULT 'success',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`scene_sample_origin_features` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `scene_name` VARCHAR(64) NOT NULL,
  `field_name` VARCHAR(255) NOT NULL,
  `field_alias` VARCHAR(255) NOT NULL,
  `description` VARCHAR(255) NULL,
  `field_index` INT NOT NULL,
  `is_dimension` TINYINT NOT NULL,
  `field_type` VARCHAR(64) NOT NULL,
  `active` VARCHAR(45) NOT NULL,
  `attr_type` VARCHAR(45) NOT NULL,
  `properties` VARCHAR(45) NULL,
  `require_type` VARCHAR(64) NOT NULL COMMENT 'required / not_required / inner_used',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_transform_node` (
  `id` VARCHAR(128) NOT NULL COMMENT '特征工程节点 id， 为 {action_name}_{8位uuid}',
  `feature_info_id` INT NOT NULL,
  `action_name` VARCHAR(64) NOT NULL,
  `input_config` TEXT NULL,
  `output_config` TEXT NULL,
  `execute_config` TEXT NULL,
  `node_index` INT(11) NOT NULL,
  `run_status` VARCHAR(32) NOT NULL COMMENT 'not_run / pending / running /  success / failed',
  `operate_status` VARCHAR(32) NOT NULL COMMENT ' init / updated / finished',
  `step_name` VARCHAR(64) NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `properties` TEXT NULL,
  `node_config` TEXT NULL,
  `select` TINYINT NOT NULL DEFAULT 1 COMMENT '是否被选中',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_type` (
  `feature_type_name` VARCHAR(64) NOT NULL,
  `feature_type_alias` VARCHAR(255) NULL,
  `type_index` INT(11) NULL,
  `parent_feature_type_name` VARCHAR(64) NULL,
  `description` VARCHAR(255) NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`feature_type_name`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`evaluation` (
  `evaluation_id` VARCHAR(128) NOT NULL,
  PRIMARY KEY (`evaluation_id`))
ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `bkdata_meta`.`operation` (
  `operation_id` VARCHAR(128) NOT NULL,
  PRIMARY KEY (`operation_id`))
ENGINE = InnoDB;


CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_template_node_upstream` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `node_id` INT NOT NULL,
  `parent_node_id` INT NULL,
  `node_index` INT NOT NULL,
  `properties` TEXT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `complete_type` VARCHAR(32) NOT NULL DEFAULT 'success',
  `concurrent_type` VARCHAR(32) NOT NULL DEFAULT 'sequence',
  `execute_index` INT NOT NULL DEFAULT 1,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB;

-- ------------------------------------------------------------
-- Table `bkdata_meta`.`timeseries_sample_fragments_stage`
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`timeseries_sample_fragments_stage` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `start_time` TIMESTAMP NULL,
  `end_time` TIMESTAMP NULL,
  `generate_type` VARCHAR(32) NOT NULL DEFAULT 'manual' COMMENT '来源manual还是sample_feedback',
  `labels` TEXT NULL COMMENT '这个片段的labels信息, {“start_time”:xx, “end_time”: xxx, \"anomaly_label\": 1, ”remark\": \"备注”,  \"anomaly_label_type\": “manual”}',
  `status` VARCHAR(32) NOT NULL DEFAULT 'developing',
  `labels_type` VARCHAR(32) NOT NULL DEFAULT 'timeseries' COMMENT 'Label的类别,默认是teimseries',
  `result_table_id` VARCHAR(255) NOT NULL,
  `sample_set_id` INT(11) NOT NULL,
  `line_id` VARCHAR(255) NOT NULL,
  `group_fields` TEXT NULL,
  `group_dimension` TEXT NULL,
  `properties` TEXT NULL,
  `sample_size` BIGINT NULL,
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '时序数据片段存储(前台表)';

-- --------------------------------------------------------
-- Table `bkdata_meta`.`timeseries_sample_label_stage`
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`timeseries_sample_label_stage` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `start` TIMESTAMP NULL,
  `end` TIMESTAMP NULL,
  `remark` TEXT NULL,
  `anomaly_label_type` VARCHAR(64) NOT NULL,
  `result_table_id` VARCHAR(255) NOT NULL,
  `sample_set_id` INT(11) NOT NULL,
  `line_id` VARCHAR(255) NOT NULL,
  `anomaly_label` VARCHAR(64) NOT NULL,
  `status` VARCHAR(32) NOT NULL DEFAULT 'init',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '时序异常标注存储(前台表)';

-- ----------------------------------------------------------------
-- Table `bkdata_meta`.`feature_transform_node_upstream_stage`
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_transform_node_upstream_stage` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `feature_info_id` INT NOT NULL COMMENT '特征工程id',
  `feature_transform_node_id` VARCHAR(128) NOT NULL,
  `parent_feature_transform_node_id` VARCHAR(128) NOT NULL,
  `properties` TEXT NULL DEFAULT NULL,
  `execute_index` INT(11) DEFAULT 1 NOT NULL,
  `concurrent_type` VARCHAR(32) NOT NULL DEFAULT 'sequence',
  `complete_type` VARCHAR(32) NOT NULL DEFAULT 'success',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '特征工程节点关系(前台表)';

-- --------------------------------------------------------
-- Table `bkdata_meta`.`feature_transform_node_stage`
-- --------------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`feature_transform_node_stage` (
  `id` VARCHAR(128) NOT NULL COMMENT '特征工程节点 id， 为 {action_name}_{8位uuid}',
  `feature_info_id` INT NOT NULL,
  `action_name` VARCHAR(64) NOT NULL,
  `input_config` TEXT NULL,
  `output_config` TEXT NULL,
  `execute_config` TEXT NULL,
  `node_index` INT(11) NOT NULL,
  `status` VARCHAR(32) DEFAULT 'init' NOT NULL COMMENT '节点状态(用于放弃修改进行撤销) init/standby/pre_delete/finished',
  `run_status` VARCHAR(32) NOT NULL COMMENT 'not_run / pending / running /  success / failed',
  `operate_status` VARCHAR(32) NOT NULL COMMENT ' init / updated / finished',
  `step_name` VARCHAR(64) NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `properties` TEXT NULL,
  `node_config` TEXT NULL,
  `select` TINYINT NOT NULL DEFAULT 1 COMMENT '是否被选中',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`))
ENGINE = InnoDB
COMMENT = '特征工程节点配置(前台表)';

-- -----------------------------------------------------
-- Table `bkdata_meta`.`sample_features_stage`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_meta`.`sample_features_stage` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sample_set_id` INT NOT NULL,
  `field_name` VARCHAR(255) NOT NULL,
  `field_alias` VARCHAR(255) NOT NULL,
  `description` VARCHAR(255) NOT NULL,
  `field_index` INT(11) NOT NULL,
  `is_dimension` TINYINT NOT NULL DEFAULT 0,
  `field_type` VARCHAR(32) NOT NULL,
  `active` TINYINT NOT NULL DEFAULT 1,
  `origin` TEXT NULL COMMENT '从哪些原始的样本集字段计算出来的。列表 []',
  `attr_type` VARCHAR(32) NOT NULL COMMENT '字段的属性类型 feature / metric / label / ts_field',
  `properties` TEXT NULL,
  `generate_type` VARCHAR(32) NOT NULL COMMENT '原生 还是 生成的, origin / generate',
  `field_evaluation` TEXT NULL COMMENT '字段评估结果,如特征重要性 importance 和 质量评分 quality',
  `parents` TEXT NULL COMMENT '父字段',
  `feature_transform_node_id` VARCHAR(128) NOT NULL DEFAULT '' COMMENT '特征来自于哪个特征准备的节点, 节点id',
  `require_type` VARCHAR(64) NOT NULL COMMENT 'required / not_required / inner_used',
  `created_by` VARCHAR(50) NOT NULL COMMENT '创建者',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(50) NOT NULL COMMENT '更新者',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY  `sample_set_id` (`sample_set_id`,`field_name`))
ENGINE = InnoDB
COMMENT = '样本集特征列表前台表';

