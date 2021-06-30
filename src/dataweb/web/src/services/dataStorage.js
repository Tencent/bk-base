/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

import Storage from '../controller/storage';

const storage = new Storage();
/** 获取入库列表（查询rt以及rt相关配置信息和分发任务信息）
 * @param bk_username  (query) 用户
 * @param raw_data_id  (query) 数据源ID
 * @param storage_type  (query) 存储类型
 * @param storage_cluster  (query) 存储集群
 * @param result_table_id  (query) 存储类型
 */
const getStorageList = {
  url: '/v3/databus/data_storages/',
};

/** 查询数据源信息列表
 * @param (query) raw_data_id
 */
const getDataSourceList = {
  url: '/v3/databus/data_storages/raw_data_source/',
  callback(res) {
    return Object.assign(res, { data: storage.foramtDataSourceList(res.data) });
  },
};

/** 获取数据入库详情（查询rt以及rt相关配置信息需整合存储配置项接口）
 * @param bk_username  (query) 用户
 * @param raw_data_id  (query) 数据源ID
 * @param storage_type  (query) 存储类型
 * @param storage_cluster  (query) 存储集群
 * @param result_table_id  (query) 存储类型
 */
const getStorageDetail = {
  url: '/v3/databus/data_storages/',
};

/** 添加入库(同步存储信息，同步配置项信息,创建启动分发任务等) */
const createStorageConfig = {
  url: '/v3/databus/data_storages/',
  method: 'POST',
};

/** 删除入库任务
 * @param rt_id  结果表id
 */
const deleteStorageConfig = {
  url: '/v3/databus/data_storages/:rt_id/',
  method: 'DELETE',
};

/** 修改入库
 * @param result_table_id
 * @param storage_type 存储类型
 */
const updateStorage = {
  url: '/v3/databus/data_storages/:result_table_id/',
  method: 'PUT',
};
/** 获取存储类型 */
const getStorageType = {
  url: '/v3/storekit/scenarios/',
  callback(res) {
    return Object.assign(res, { data: storage.foramtStorageScenarioConfigs(res.data) });
  },
};

/** 启动/重试任务
 * @param {String} result_table_id
 * @param {Array} storages
 */
const startTask = {
  url: '/v3/databus/tasks/',
  method: 'POST',
};

/** 停止任务
 * @param rt_id
 */
const stopTask = {
  url: '/v3/databus/tasks/:rt_id/',
  method: 'DELETE',
};

/** 获取元数据对应字段 */
const getRawdataFields = {
  url: '/v3/databus/data_storages/raw_data_fields/?scenario=log',
  callback(res, option) {
    return Object.assign(res, { data: storage.formatRawdataFields(res.data, option) });
  },
};

/** 获取基础配置 */
const getStorageCommon = {
  url: '/v3/storekit/scenarios/common/',
};

/**  查询操作历史记录
 * @param data_type string 数据源类型
 * @param storage_type string 存储类型
 * @param result_table_id string 结果表
 * @param raw_data_id string 源数据ID
 * @param storage_cluster string 存储集群 */
const getInStorageLog = {
  url: '/v3/databus/data_storages/oplog/',
};

export {
  getStorageList,
  getDataSourceList,
  getStorageDetail,
  createStorageConfig,
  deleteStorageConfig,
  updateStorage,
  getStorageType,
  startTask,
  stopTask,
  getRawdataFields,
  getInStorageLog,
  getStorageCommon,
};
