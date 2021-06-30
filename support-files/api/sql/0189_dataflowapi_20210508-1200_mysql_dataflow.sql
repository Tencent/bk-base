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

USE bkdata_flow;

SET NAMES utf8;

CREATE TABLE IF NOT EXISTS batch_one_time_execute_job (
  job_id varchar(255) NOT NULL COMMENT '作业ID',
  job_type  varchar(255) NOT NULL COMMENT 'job类型',
  execute_id BIGINT(20) NULL COMMENT 'jobnavi执行id',
  jobserver_config text DEFAULT NULL COMMENT '作业服务配置',
  cluster_group varchar(255) NULL COMMENT '集群组',
  job_config text DEFAULT NULL COMMENT '作业配置',
  processing_logic text DEFAULT NULL COMMENT '作业逻辑',
  deploy_config text DEFAULT NULL COMMENT '资源配置',
  created_by varchar(50) NOT NULL DEFAULT '' COMMENT '创建人',
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_by varchar(50) DEFAULT NULL COMMENT '修改人 ',
  updated_at timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  description text DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='一次性任务流水表';
