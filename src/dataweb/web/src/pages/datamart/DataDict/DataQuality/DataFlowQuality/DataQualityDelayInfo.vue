

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
    <QualityChartCombineTable
      :title="$t('延迟信息')"
      :leftApiParams="'data_time_delay'"
      :rightApiParams="'process_time_delay'"
      :leftChartTitle="$t('数据延迟 (秒)')"
      :rightChartTitle="$t('处理延迟 (秒)')"
      :leftChartTips="$t('当前数据自身的延迟（与上游节点或数据源有关）')"
      :rightChartTips="$t('由平台计算导致的延迟')"
      :leftTableHead="$t('平均数据延迟 (秒)')"
      :rightTableHead="$t('平均处理延迟 (秒)')"
      :rightTableRateDes="$t('变化量/率（今日平均处理延迟同比昨日、上周变化）')"
      :leftTableRateDes="$t('变化量/率（今日平均数据延迟同比昨日、上周变化）')"
      :leftTipsTableRateDes="$t('变化量/率（当前时刻数据延迟同比昨日、上周变化）')"
      :rightTipsTableRateDes="$t('变化量/率（当前时刻处理延迟同比昨日、上周变化）')"
      :leftTipsTableHead="$t('数据延迟')"
      :rightTipsTableHead="$t('处理延迟')"
      v-on="$listeners" />
    <QualityChartCombineTable
      :title="$t('丢失丢弃')"
      :leftApiParams="'loss_count'"
      :rightApiParams="'drop_count'"
      :leftChartTitle="$t('数据丢失 (条)')"
      :rightChartTitle="$t('无效数据 (条)')"
      :leftChartTips="$t('当前数据自身的延迟（与上游节点或数据源有关）')"
      :rightChartTips="$t('由于数据不符合既定规则而被视为无效数据而丢弃')"
      :leftTableHead="$t('累计数据丢失 (条)')"
      :rightTableHead="$t('累计无效数据 (条)')"
      :rightTableRateDes="$t('变化量/率（今日累计无效数据量同比昨日、上周变化）')"
      :leftTableRateDes="$t('变化量/率（今日累计数据丢失量同比昨日、上周变化）')"
      :leftTipsTableRateDes="$t('变化量/率（当前时刻数据丢失量同比昨日、上周变化）')"
      :rightTipsTableRateDes="$t('变化量/率（当前时刻无效数据量同比昨日、上周变化）')"
      :leftTipsTableHead="$t('数据丢失（条）')"
      :rightTipsTableHead="$t('无效数据（条）')"
      v-on="$listeners" />
  </div>
</template>

<script>
import QualityChartCombineTable from './components/QualityChartCombineTable.vue';

export default {
  components: {
    QualityChartCombineTable,
  },
  methods: {
    pollingInit(timeout) {
      this.$children.forEach(child => {
        child.pollingInit(timeout);
      });
    },
    init() {
      this.$children.forEach(child => {
        child.init();
      });
    },
    cancelRefresh() {
      this.$children.forEach(child => {
        child.cancelRefresh();
      });
    },
  },
};
</script>
