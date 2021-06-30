

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <div>
    <!-- 数据价值 -->
    <DataValue v-bind="$attrs" />
    <!-- 存储成本 -->
    <StorageList v-if="$route.query.dataType !== 'raw_data'" />
    <!-- 相似数据集 -->
    <DataSimilarity v-if="$route.query.dataType === 'result_table'" />
    <!-- <DataChange /> -->
    <!-- 变更记录 -->
    <DataRemovalList v-bind="$attrs" />
    <!-- 数据应用 -->
    <DataApplication />
  </div>
</template>

<script>
import DataValue from './DataValue';
import DataRemovalList from './DataRemovalList';
import DataApplication from './DataApplication';
import StorageList from './StorageList';
import DataSimilarity from './DataSimilarity';

export default {
  components: {
    DataValue,
    DataRemovalList,
    DataApplication,
    StorageList,
    DataSimilarity,
  },
};
</script>
