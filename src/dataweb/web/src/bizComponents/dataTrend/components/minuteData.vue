

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
  <div class="wrapper">
    <div class="header clearfix">
      <div class="header-title-left clearfix">
        <span class="line" />
        {{ $t('分钟数据量') }}
      </div>
      <div class="date">
        <bkdata-date-picker v-model="minuteParams.date"
          type="datetimerange"
          format="yyyy-MM-dd HH:mm"
          :editable="false"
          :clearable="false"
          :shortcuts="pickerOptions.shortcuts"
          :shortcutClose="true"
          @change="minuteChange" />
      </div>
      <div class="refresh">
        <span v-tooltip="$t('刷新')"
          class="refesh-for-sscreen"
          @click="refreshMinute">
          <i class="bk-icon icon-refresh" />
        </span>
        <button class="refesh-for-xscreen bk-button bk-primary bk-button-mini"
          @click="refreshMinute">
          {{ $t('刷新') }}
        </button>
      </div>
    </div>
    <div v-bkloading="{ isLoading: loading.minLoading }"
      class="line-chart">
      <div v-show="minuteTime.length === 0"
        :style="recentWrapperBg"
        class="no-data">
        <img alt
          src="../../../common/images/no-data.png">
        <p>{{ $t('暂无数据') }}</p>
      </div>
      <PlotlyChart v-show="minuteTime.length > 0"
        :style="recentWrapperBg"
        :chartConfig="threeMinsCountChartConfig"
        :chartData="minuteChartData"
        :chartLayout="threeMinsCountChartLayout" />
    </div>
  </div>
</template>

<script>
import '../scss/dataAmount.scss';
import mixin from './mixin.js';
import PlotlyChart from '@/components/plotly';
export default {
  components: {
    PlotlyChart
  },
  mixins: [mixin],
  props: {
    loading: {
      type: Object,
      default: () => ({})
    },
    recentWrapperBg: {
      type: Object,
      default: () => ({})
    },
    minuteParams: {
      type: Object,
      default: () => ({})
    }
  },
  data() {
    // common layout config of charts
    const COMMON_LAYOUT_CONFIG = {
      height: 300,
      width: 537,
      margin: {
        l: 60,
        r: 60,
        b: 60,
        t: 60,
      },
      showlegend: true,
      legend: {
        xanchor: 'center',
        yanchor: 'top',
        y: -0.4,
        x: 0.5,
      },
    };
    return {
      minuteTime: [],
      minuteTimezone: '',
      minuteChartData: [{}],
      threeMinsCountChartConfig: {
        // 配置需要的操作按钮图标
        operationButtons: ['autoScale2d', 'resetScale2d'],
      },
      threeMinsCountChartLayout: {
        // 3分钟数据量UI设置
        ...COMMON_LAYOUT_CONFIG,
        xaxis: {
          dtick: 120, // 设置x轴间隔
          showgrid: false, // 是否显示x轴线条
          showline: true, // 是否绘制出该坐标轴上的直线部分
          tickwidth: 1,
          zeroline: false,
          autorange: true,
        },
        yaxis: {
          showline: true, // 是否绘制出该坐标轴上的直线部分
          tickwidth: 1,
          zeroline: false,
          rangemode: 'normal',
          autorange: true,
        },
      },
    };
  },
  methods: {
    /*
                3 分钟数据量日期选择
            */
    minuteChange() {
      if (this.minuteParams.date) {
        this.loading.minLoading = true;
        this.minuteParams.start_time = this.minuteParams.date[0].getFullYear()
                + '-' + (this.minuteParams.date[0].getMonth() + 1) + '-' + this.minuteParams.date[0].getDate()
                + ' ' + this.minuteParams.date[0].getHours() + ':' + this.minuteParams.date[0].getMinutes()
                + ':' + this.minuteParams.date[0].getSeconds();
        this.minuteParams.end_time = this.minuteParams.date[1].getFullYear()
                + '-' + (this.minuteParams.date[1].getMonth() + 1) + '-' + this.minuteParams.date[1].getDate()
                + ' ' + this.minuteParams.date[1].getHours() + ':' + this.minuteParams.date[1].getMinutes()
                + ':' + this.minuteParams.date[1].getSeconds();
        this.minuteParams.date = [this.minuteParams.start_time, this.minuteParams.end_time];
        this.getMinDataAmount();
      }
    },
    /*
                获取3分钟数据量
            */
    getMinDataAmount() {
      this.loading.minLoading = true;
      let url;
      if (this.$route.query.dataType === 'result_table') {
        url = `result_tables/${this.id}/list_rt_count/?${this.qs.stringify(this.minuteParams)}`;
      } else if (this.$route.query.dataType === 'raw_data') {
        url = `dataids/${this.id}/list_data_count_by_time/?${this.qs.stringify(this.minuteParams)}`;
      } else {
        let did = this.$route.params.did;
        url = `dataids/${did}/list_data_count_by_time/?${this.qs.stringify(this.minuteParams)}`;
      }
      this.axios
        .get(url)
        .then(res => {
          if (res.result) {
            this.minuteTime = res.data.time;
            this.minuteTimezone = res.data.timezone;
            let minuteData = {};
            if (this.minuteTime.length > 0) {
              // 缩放时需要y轴坐标自适应，不能设置dtick
              // this.threeMinsCountChartLayout.yaxis.dtick = this.getYaxisDtick(res.data.cnt)
              let dayDiff = this.getDateDiff(this.minuteParams.start_time, this.minuteParams.end_time);
              this.threeMinsCountChartLayout.xaxis.dtick = 120 * dayDiff;
              this.minuteChartData = [
                {
                  x: res.data.time,
                  y: res.data.cnt,
                  name: this.$t('数据量'),
                  type: 'lines',
                  line: {
                    color: '#3a84ff',
                  },
                },
              ];
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.minLoading = false;
        });
    },
    refreshMinute() {
      this.loading.minLoading = true;
      this.getMinDataAmount();
    },
  }
};
</script>
