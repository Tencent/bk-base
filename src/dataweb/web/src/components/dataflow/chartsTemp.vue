

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
  <div
    id="charts"
    v-bkloading="{ isLoading: charts.types.length === 0 && charts.location.type !== 'algorithm_model' }"
    class="charts"
    :class="{ 'is-hidden': isloading }"
    @mouseleave="disposeCharts"
    @mouseenter="enterCharts">
    <span
      class="arrow"
      :class="{
        top: arrowPosition.isTop,
        left: arrowPosition.isLeft,
        right: arrowPosition.isRight,
        bottom: arrowPosition.isBottom,
      }" />
    <div class="charts-header clearfix">
      <div class="fl clearfix tips"
        :title="charts.location.output + '(' + charts.location.name + ')'">
        {{ charts.location.output }} ({{ charts.location.name }})
      </div>
      <span class="fr time clearfix">
        {{ $t('最近24小时') }}
        <i class="bk-icon icon-clock" />
      </span>
    </div>
    <div class="charts-content">
      <div v-if="charts.types.length === 0"
        class="charts-contents-nodata">
        <i class="bk-icon icon-no-data" />
        <p>{{ $t('暂无数据') }}</p>
      </div>
      <div v-show="warningmsg"
        class="charts-content-warning"
        :title="warningmsg">
        <i class="bk-icon icon-exclamation-circle" />
        {{ warningmsg }}
      </div>
      <bkdata-tab :active.sync="activeName"
        type="card"
        @tab-change="handleClick">
        <bkdata-tab-panel v-if="charts.types.indexOf('hasTrend') > -1"
          name="dataTrend"
          :label="$t('数据量趋势')">
          <TabItem :chartData="trendChartInfo.data" />
        </bkdata-tab-panel>
        <bkdata-tab-panel v-if="charts.types.indexOf('hasDelay') > -1"
          name="dataDelay"
          :label="$t('计算延迟_单位_秒')">
          <TabItem :chartData="delayChartInfo.data" />
        </bkdata-tab-panel>
        <bkdata-tab-panel v-if="charts.types.indexOf('loss') > -1"
          name="dataLoss"
          :label="$t('数据丢失')">
          <TabItem :chartData="lossChartInfo.data" />
        </bkdata-tab-panel>
        <bkdata-tab-panel v-if="charts.types.indexOf('offline') > -1"
          name="offline"
          :label="$t('执行记录')">
          <div class="history-do">
            <div class="history-do-record">
              <div v-if="charts.histroryLists.length > 0"
                class="clearfix">
                <p
                  v-for="item in charts.histroryLists"
                  :key="item.period_start_time"
                  :title="$t('查看错误信息')"
                  class="clearfix"
                  @click="expandErr(item)">
                  <span class="fl= status-label-success"
                    :class="{ 'status-label-failure': item.status === 'fail' }">
                    {{ item.status_str }}
                  </span>
                  <span class="fr">
                    {{ item.period_start_time }}
                  </span>
                  <template v-for="(i, index) in item.execute_list"
                    class="error-msg clearfix">
                    <div v-show="item.isShow && item.status === 'fail'"
                      :key="index"
                      class="clearfix error-msg">
                      {{ index + 1 }}. {{ $t('错误信息') }}：{{ i.err_msg }}（{{ $t('错误码') }}{{ i.err_code }}）
                    </div>
                  </template>
                </p>
              </div>
              <template v-else>
                <p style="height: 40px">
                  <span>
                    {{ $t('无历史执行记录') }}
                  </span>
                </p>
              </template>
            </div>
          </div>
        </bkdata-tab-panel>
      </bkdata-tab>
    </div>
  </div>
</template>
<script>
import TabItem from './chartTabItem';
import Vue from 'vue';

export default {
  props: {
    chartsData: {
      type: Object,
    },
  },
  data() {
    return {
      charts: {
        isAllReady: false,
        isInit: {
          hasTrend: false,
          hasDelay: false,
          hasOffline: false,
        },
        data: [],
        types: [],
        option: {
          data_delay_max: {
            delay_max: [],
            time: [],
          },
          data_trend: {
            input: [],
            output: [],
            alert_list: [],
            time: [],
          },
        },
        nodeId: '',
        location: {}, // 节点的配置信息，包括位置信息
        histroryLists: [],
      },
    };
  },
  methods: {
    // 初始化
    init(nodeId) {
      this.activeName = 'dataTrend';
      let charts = this.charts;
      charts.types = [];
      if (!nodeId) return;
      charts.data = this.chartsData.data;
      charts.nodeId = nodeId;

      charts.data.forEach(item => {
        for (let key in item) {
          if (parseInt(key) === parseInt(nodeId)) {
            charts.types = item[key].types;
            charts.option = item[key].options;
            charts.location = item[key].location;
          }
          break;
        }
      });
    },
  },
};
</script>
