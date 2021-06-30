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

-- -----------------------------------------------------
-- Schema bkdata_basic
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `bkdata_basic` DEFAULT CHARACTER SET utf8 ;
USE `bkdata_basic` ;

-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_group_info`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_group_info` (
  `resource_group_id` VARCHAR(45) NOT NULL COMMENT '资源组英文标识',
  `group_name` VARCHAR(255) NOT NULL COMMENT '资源组名称',
  `group_type` VARCHAR(45) NOT NULL COMMENT '资源组类型（public：公开,protected：业务内共享,private：私有）',
  `bk_biz_id` INT NOT NULL COMMENT '所属业务，涉及到成本的核算',
  `status` VARCHAR(45) NOT NULL DEFAULT 'approve' COMMENT '状态（approve:审批中,succeed:审批通过,reject:拒绝的,delete:删除的）',
  `process_id` VARCHAR(45) NOT NULL COMMENT '资源组创建流程ID（当前生效的）',
  `description` TEXT NULL COMMENT '描述',
  `created_by` VARCHAR(45) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`resource_group_id`),
  UNIQUE INDEX `process_id_UNIQUE` (`process_id` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COMMENT = '资源组信息';


-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_geog_area_cluster_group`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_geog_area_cluster_group` (
  `resource_group_id` VARCHAR(45) NOT NULL COMMENT '资源组英文标识',
  `geog_area_code` VARCHAR(45) NOT NULL COMMENT '区域',
  `cluster_group` VARCHAR(45) NOT NULL COMMENT 'cluster_group，兼容现有的，理解为地区资源组',
  `created_by` VARCHAR(45) NULL COMMENT '创建人',
  `created_at` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`geog_area_code`, `resource_group_id`),
  UNIQUE INDEX `cluster_group_UNIQUE` (`cluster_group` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COMMENT = '资源组区域分组（地区资源组）';


-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_cluster_config`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_cluster_config` (
  `cluster_id` VARCHAR(128) NOT NULL,
  `cluster_type` VARCHAR(45) NOT NULL COMMENT '集群类型（yarn、hdfs、tspider、es、druid、k8s、kafka)',
  `cluster_name` VARCHAR(45) NULL COMMENT '集群名称',
  `component_type` VARCHAR(45) NOT NULL COMMENT '组件类型（spark、flink、hdfs、tspider、es、druid、kafka、kafka-connect)',
  `geog_area_code` VARCHAR(45) NOT NULL COMMENT '地区',
  `resource_group_id` VARCHAR(45) NOT NULL COMMENT '资源组ID',
  `resource_type` VARCHAR(45) NOT NULL,
  `service_type` VARCHAR(45) NOT NULL,
  `src_cluster_id` VARCHAR(128) NOT NULL,
  `active` VARCHAR(45) NULL COMMENT '是否生效',
  `splitable` VARCHAR(45) NULL COMMENT '是否可以拆分分配（当前存储类的不可以拆分分配）',
  `cpu` DOUBLE(18,2) NULL COMMENT 'CPU数量，单位core',
  `memory` DOUBLE(18,2) NULL COMMENT '内存数据量，单位MB',
  `gpu` DOUBLE(18,2) NULL COMMENT 'GPU数量，单位core',
  `disk` DOUBLE(18,2) NULL COMMENT '存储容量，单位MB',
  `net` DOUBLE(18,2) NULL COMMENT '网络带宽，单位bit',
  `slot` DOUBLE(18,2) NULL COMMENT '处理槽位，单位个',
  `available_cpu` DOUBLE(18,2) NULL COMMENT '可用CPU数量，单位core',
  `available_memory` DOUBLE(18,2) NULL COMMENT '可用内存数据量，单位MB',
  `available_gpu` DOUBLE(18,2) NULL COMMENT '可用GPU数量，单位core',
  `available_disk` DOUBLE(18,2) NULL COMMENT '可用存储容量，单位MB',
  `available_net` DOUBLE(18,2) NULL COMMENT '可用网络带宽，单位bit',
  `available_slot` DOUBLE(18,2) NULL COMMENT '可用处理槽位，单位个',
  `connection_info` TEXT NULL COMMENT '连接信息',
  `expires` TEXT NULL COMMENT '存储周期（主要是存储使用）',
  `extra_info` TEXT NULL COMMENT '扩展信息',
  `priority` INT(11) NULL COMMENT '优先级',
  `version` VARCHAR(128) NULL COMMENT '集群版本',
  `belongs_to` VARCHAR(128) NULL COMMENT '集群所属',
  `description` TEXT NULL COMMENT '描述说明',
  `created_by` VARCHAR(45) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`cluster_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COMMENT = '集群配置';


-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_service_config`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_service_config` (
  `resource_type` VARCHAR(45) NOT NULL COMMENT '资源分类（processing：计算类资源、storage：存储类资源、databus：数据总线类资源（含清洗、分发））',
  `service_type` VARCHAR(45) NOT NULL COMMENT '服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）',
  `service_name` VARCHAR(128) NOT NULL,
  `active` INT(1) NOT NULL COMMENT '是否生效',
  `created_by` VARCHAR(45) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`resource_type`, `service_type`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COMMENT = '资源服务类型配置表';


-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_group_capacity`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_group_capacity` (
  `resource_group_id` VARCHAR(45) NOT NULL COMMENT '资源组英文标识',
  `geog_area_code` VARCHAR(45) NOT NULL COMMENT '地区',
  `resource_type` VARCHAR(45) NOT NULL COMMENT '资源分类（processing：计算类资源、storage：存储类资源、databus：数据总线类资源（含清洗、分发））',
  `service_type` VARCHAR(45) NOT NULL COMMENT '服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）',
  `cluster_id` VARCHAR(128) NOT NULL COMMENT '集群ID',
  `cpu` DOUBLE(18,2) NULL COMMENT 'CPU数量，单位core',
  `memory` DOUBLE(18,2) NULL COMMENT '内存数据量，单位MB',
  `gpu` DOUBLE(18,2) NULL COMMENT 'GPU数量，单位core',
  `disk` DOUBLE(18,2) NULL COMMENT '存储容量，单位MB',
  `net` DOUBLE(18,2) NULL COMMENT '网络带宽，单位bit',
  `slot` DOUBLE(18,2) NULL COMMENT '处理槽位，单位个',
  `created_by` VARCHAR(45) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`resource_group_id`, `geog_area_code`, `resource_type`, `service_type`, `cluster_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COMMENT = '资源组容量信息表';


-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_unit_config`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_unit_config` (
  `resource_unit_id` INT NOT NULL AUTO_INCREMENT COMMENT '资源套餐ID',
  `name` VARCHAR(45) NOT NULL COMMENT '配置单元名称',
  `resource_type` VARCHAR(45) NOT NULL COMMENT '资源分类（processing：计算类资源、storage：存储类资源、databus：数据总线类资源（含清洗、分发））',
  `service_type` VARCHAR(45) NOT NULL COMMENT '服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）',
  `cpu` DOUBLE(18,2) NULL COMMENT 'CPU数量，单位core',
  `memory` DOUBLE(18,2) NULL COMMENT '内存数据量，单位MB',
  `gpu` DOUBLE(18,2) NULL COMMENT 'GPU数量，单位core',
  `disk` DOUBLE(18,2) NULL COMMENT '存储容量，单位MB',
  `net` DOUBLE(18,2) NULL COMMENT '网络带宽，单位bit',
  `slot` DOUBLE(18,2) NULL COMMENT '处理槽位，单位个',
  `active` INT(1) NULL COMMENT '是否生效，主要是页面是否可见',
  `created_by` VARCHAR(45) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`resource_unit_id`))
ENGINE = InnoDB
COMMENT = '资源单元配置';


-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_group_capacity_apply_form`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_group_capacity_apply_form` (
  `apply_id` INT NOT NULL AUTO_INCREMENT COMMENT '申请单号ID',
  `apply_type` VARCHAR(45) NOT NULL COMMENT '单据类型（increase：增加扩容，decrease：减少缩容）',
  `resource_group_id` VARCHAR(45) NOT NULL COMMENT '资源组英文标识',
  `geog_area_code` VARCHAR(45) NOT NULL COMMENT '地区',
  `resource_type` VARCHAR(45) NOT NULL COMMENT '资源分类（processing：计算类资源、storage：存储类资源、databus：数据总线类资源（含清洗、分发））',
  `service_type` VARCHAR(45) NOT NULL COMMENT '服务类型（stream、batch、stream_model、batch_model、hdfs、tspider、clean）',
  `resource_unit_id` INT NOT NULL COMMENT '资源套餐ID',
  `num` INT NOT NULL,
  `operate_result` TEXT NULL COMMENT '运维实施结果（实际的分配资源，可能和套餐有些出入）',
  `status` VARCHAR(45) NOT NULL DEFAULT 'approve' COMMENT '状态（approve:审批中,succeed:审批通过,reject:拒绝的,delete:删除的）',
  `process_id` VARCHAR(45) NOT NULL COMMENT '资源组创建流程ID（当前生效的）',
  `description` TEXT NULL COMMENT '申请原因',
  `cluster_id` VARCHAR(128) NULL COMMENT '集群ID，运维实施后回填的集群ID',
  `created_by` VARCHAR(45) NOT NULL COMMENT '创建人',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_by` VARCHAR(45) NULL COMMENT '更新人',
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`apply_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COMMENT = '资源组容量申请表';

-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_job_submit_instance_log`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_job_submit_instance_log` (
  `submit_id` BIGINT(20) NOT NULL COMMENT '任务提交唯一标识',
  `cluster_id` VARCHAR(128) NOT NULL COMMENT '集群标识',
  `cluster_name` VARCHAR(45) DEFAULT NULL COMMENT '集群名称',
  `resource_type` VARCHAR(45) NOT NULL COMMENT '资源类型（processing、storage、schedule）',
  `cluster_type` VARCHAR(45) NOT NULL COMMENT '集群类型（yarn、hdfs、tspider、es、druid、k8s、kafka)',
  `inst_id` VARCHAR(128) NOT NULL COMMENT '任务提交资源实例标识',
  PRIMARY KEY (`submit_id`,`cluster_id`,`inst_id`),
  KEY `cluster_id` (`cluster_id`),
  KEY `inst_id` (`inst_id`)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8
COMMENT='任务资源提交实例表';

-- -----------------------------------------------------
-- Table `bkdata_basic`.`resource_job_submit_log`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `bkdata_basic`.`resource_job_submit_log` (
  `submit_id` BIGINT(20) NOT NULL AUTO_INCREMENT COMMENT '任务提交唯一标识',
  `job_id` VARCHAR(255) NOT NULL COMMENT '任务标识',
  `resource_group_id` VARCHAR(45) NOT NULL COMMENT '资源组ID',
  `geog_area_code` VARCHAR(45) NOT NULL COMMENT '地区',
  `status` VARCHAR(100) DEFAULT NULL COMMENT '任务状态',
  `created_by` VARCHAR(255) DEFAULT NULL COMMENT '创建人',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`submit_id`),
  KEY `job_id` (`job_id`,`status`),
  KEY `resource_group_id` (`resource_group_id`,`geog_area_code`),
  KEY `created_at` (`created_at`),
  KEY `updated_at` (`updated_at`)
)
ENGINE=InnoDB
AUTO_INCREMENT=1
DEFAULT CHARSET=utf8
COMMENT='任务资源提交记录表';
