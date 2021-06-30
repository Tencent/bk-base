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

/** 获取我的迁移任务列表
 * @query raw_data_id
 */
const getDataRemovalList = {
  url: 'v3/databus/migrations/get_tasks/',
};

/** 获取来源存储和目标存储列表
 * @query result_table_id
 */
const getClusterList = {
  url: 'v3/databus/migrations/get_clusters/',
};

/** 创建迁移任务
 * @params  "result_table_id": "591_test_kv_src1",
 * @params  "source": "tspider",
 * @params  "dest": "hdfs",
 * @params  "start": "2019-09-02 13:00:00",
 * @params  "end": "2019-09-02 14:00:00",
 * @params  "overwrite": true
 */
const createDataRemovalTask = {
  url: 'v3/databus/migrations/',
  method: 'POST',
};

/** 获取迁移进度
 * @params id // 迁移任务的id
 */
const getDataRemovalProcess = {
  url: 'v3/databus/migrations/:id/',
};

/** 获取来源存储和目标存储可以支持的类型的接口
 * @params id // 迁移任务的id
 */
const getStorageTypeList = {
  url: 'v3/databus/migrations/get_support_clusters/',
};

/** 获取迁移概况图形数据
 * @params result_table_id
 * @query {Array} storages
 * @query start_time
 * @query end_time
 */
const getDataRemovalPreview = {
  url: '/result_tables/:result_table_id/list_measurement/',
};
export {
  getDataRemovalList,
  getClusterList,
  createDataRemovalTask,
  getDataRemovalProcess,
  getDataRemovalPreview,
  getStorageTypeList,
};
