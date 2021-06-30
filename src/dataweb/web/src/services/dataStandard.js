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

/** 获取标准化表格数据*/
const getStandardTable = {
  url: '/v3/datamanage/dstan/standard_publicity/standards/',
  method: 'POST',
};

/** 获取标准化数据详情
 *  @params dm_standard_config_id
 */
const getStandardDetail = {
  url: '/v3/datamanage/dstan/standard_publicity/get_standard_info?online=true',
};

/** 获取存储类型
 */
const getStorageType = {
  url: '/datamart/datadict/dataset_list/storage_type/',
};

/*
 * 数据字典高级搜索数据标准级联选框接口
 */
const getStandardCascadeInfo = {
  url: 'v3/datamanage/dstan/standard_publicity/standards/simple/',
};

export { getStandardTable, getStandardDetail, getStorageType, getStandardCascadeInfo };
