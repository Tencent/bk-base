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

use bkdata_meta;


CREATE TABLE IF NOT EXISTS `algorithm_model_instance` (
  `instance_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'model version ID',
  `model_release_id` int(11) DEFAULT NULL COMMENT '模型发布ID',
  `model_node_id` varchar(128) DEFAULT NULL COMMENT '模型选择的算法ID',
  `model_version_id` int(11) NOT NULL COMMENT 'model version ID',
  `model_id` varchar(64) NOT NULL COMMENT '模型的唯一ID',
  `biz_id` int(11) NOT NULL COMMENT 'biz ID',
  `project_id` int(11) NOT NULL COMMENT 'project ID',
  `flow_id` int(11) DEFAULT NULL COMMENT 'DataFlow应用的流程ID',
  `flow_node_id` int(11) DEFAULT NULL COMMENT 'DataFlow应用的节点ID',
  `training_job_id` varchar(127) DEFAULT NULL COMMENT 'if model serving mode is offline, then training_job_id is same to serving_job_id.',
  `training_when_serving` tinyint(1) NOT NULL DEFAULT '0',
  `serving_job_id` varchar(127) NOT NULL COMMENT 'an realtime job or an batch job.',
  `instance_config` mediumtext,
  `instance_status` enum('disable','enable','serving','restart') NOT NULL DEFAULT 'disable',
  `serving_mode` enum('realtime','offline','api') DEFAULT NULL,
  `api_serving_parallel` int(11) DEFAULT NULL,
  `training_reuse` tinyint(1) DEFAULT '0',
  `reuse_instance_name` varchar(64) DEFAULT NULL,
  `training_from_instance` int(11) DEFAULT NULL,
  `is_deleted` tinyint(1) NOT NULL DEFAULT '0' COMMENT '0: is using, 1: is deleted.',
  `created_by` varchar(32) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_by` varchar(32) DEFAULT NULL,
  `updated_at` timestamp  NULL DEFAULT NULL,
  `api_serving_key` varchar(64) DEFAULT NULL,
  `auto_upgrade` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否需要自动升级',
  `prediction_feedback` tinyint(4) DEFAULT '1',
  PRIMARY KEY (`instance_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='job instance for model.';

CREATE TABLE IF NOT EXISTS `algorithm_base_model` (
  `model_id` varchar(64) NOT NULL COMMENT '',
  `model_name` varchar(250) NOT NULL COMMENT 'model name, use in web, user supply.',
  `description` varchar(256) NOT NULL COMMENT 'detail comment for the model.',
  `project_id` int(11) NOT NULL COMMENT 'project ID',
  `model_type` enum('existed','flow','process') NOT NULL COMMENT 'already existed model or build by flow.',
  `is_public` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'is public, that every one can get an instance.',
  `active` tinyint(1) NOT NULL DEFAULT '1' COMMENT '',
  `created_by` varchar(32) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_by` varchar(32) DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `scene_name` varchar(64) DEFAULT NULL COMMENT '',
  `model_alias` varchar(64) DEFAULT NULL COMMENT '',
  `properties` mediumtext COMMENT '',
  `train_mode` varchar(32) DEFAULT NULL COMMENT '',
  PRIMARY KEY (`model_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='algorithm base model config table.';
