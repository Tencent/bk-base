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

set NAMES UTF8;

USE `bkdata_meta` ;

CREATE TABLE IF NOT EXISTS `bkdata_meta`.`resource_group_info` (
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
