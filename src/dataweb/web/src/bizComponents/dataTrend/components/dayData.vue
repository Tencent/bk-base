

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
        {{ $t('日数据量') }}
      </div>
      <div class="date">
        <bkdata-date-picker v-model="dayParams.date"
          type="daterange"
          format="yyyy-MM-dd"
          :editable="false"
          :clearable="false"
          :shortcuts="pickerOptions.shortcuts"
          :shortcutClose="true"
          @change="dayChange" />
      </div>
      <div class="refresh">
        <span v-tooltip="$t('刷新')"
          class="refesh-for-sscreen"
          @click="refreshDay">
          <i class="bk-icon icon-refresh" />
        </span>
        <button class="refesh-for-xscreen bk-button bk-primary bk-button-mini"
          @click="refreshDay">
          {{ $t('刷新') }}
        </button>
      </div>
    </div>
    <div v-bkloading="{ isLoading: loading.dayLoading }"
      class="line-chart">
      <div v-show="dayTime.length === 0"
        class="no-data">
        <img alt
          src="../../../common/images/no-data.png">
        <p>{{ $t('暂无数据') }}</p>
      </div>
      <PlotlyChart v-show="dayTime.length > 0"
        :chartConfig="dayDataCountChartConfig"
        :chartData="dayChartData"
        :chartLayout="dayDataCountChartLayout" />
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
    dayParams: {
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
      dayTime: [],
      dayTimezone: '',
      dayChartData: [{}],
      dayDataCountChartConfig: {
        // 配置需要的操作按钮图标
        operationButtons: ['autoScale2d', 'resetScale2d'],
      },
      dayDataCountChartLayout: {
        ...COMMON_LAYOUT_CONFIG,
        xaxis: {
          dtick: 7, // 设置x轴间隔
          showgrid: false, // 是否显示x轴线条
          showline: true, // 是否绘制出该坐标轴上的直线部分
          tickwidth: 1,
          zeroline: false,
          nticks: 1,
          tickvals: [],
          ticktext: [],
          // tickformat: '%m-%d'
        },
        yaxis: {
          showline: true, // 是否绘制出该坐标轴上的直线部分
          // tickwidth: 1,
          zeroline: false,
        },
      },
    };
  },
  methods: {
    /*
                日数据量日期选择
            */
    dayChange() {
      if (this.dayParams.date) {
        this.loading.dayLoading = true;
        this.dayParams.start_time = this.dayParams.date[0].getFullYear()
                + '-' + (this.dayParams.date[0].getMonth() + 1) + '-' + this.dayParams.date[0].getDate()
                + ' ' + this.dayParams.date[0].getHours() + ':' + this.dayParams.date[0].getMinutes()
                + ':' + this.dayParams.date[0].getSeconds();
        this.dayParams.end_time = this.dayParams.date[1].getFullYear()
                + '-' + (this.dayParams.date[1].getMonth() + 1) + '-' + this.dayParams.date[1].getDate()
                + ' ' + 23 + ':' + 59 + ':' + 59;
        this.dayParams.date = [this.dayParams.start_time, this.dayParams.end_time];
        this.getDayDataAmount();
      }
    },
    /*
                获取日数据量
            */
    getDayDataAmount() {
      let url;
      if (this.$route.query.dataType === 'result_table') {
        url = `result_tables/${this.id}/list_rt_count/?${this.qs.stringify(this.dayParams)}`;
      } else if (this.$route.query.dataType === 'raw_data') {
        url = `dataids/${this.id}/list_data_count_by_time/?${this.qs.stringify(this.dayParams)}`;
      } else {
        url = `dataids/${this.$route.params.did}/list_data_count_by_time/?${this.qs.stringify(this.dayParams)}`;
      }
      this.axios.get(url).then(res => {
        if (res.result) {
          this.dayTime = res.data.time;
          this.dayTimezone = res.data.timezone;
          if (this.dayTime.length > 0) {
            // 缩放时需要y轴坐标自适应，不能设置dtick
            // this.dayDataCountChartLayout.yaxis.dtick = this.getYaxisDtick(res.data.cnt)
            // this.dayDataCountChartLayout.xaxis.nticks = res.data.time.length
            this.dayDataCountChartLayout.xaxis.ticktext = res.data.time;
            this.dayDataCountChartLayout.xaxis.tickvals = res.data.time.map(t => {
              return `${t} 00:00:00`;
            });
            let dayDiff = this.getDateDiff(this.dayParams.start_time, this.dayParams.end_time);
            this.dayDataCountChartLayout.xaxis.dtick = 7 * dayDiff;
            this.dayChartData = [
              {
                x: res.data.time.map(t => {
                  return `${t} 00:00:00`;
                }),
                y: res.data.cnt,
                name: this.$t('数据量'),
                type: 'bar',
                marker: {
                  color: '#3a84ff',
                },
              },
            ];
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.loading.dayLoading = false;
      });
    },
    refreshDay() {
      this.loading.dayLoading = true;
      this.getDayDataAmount();
    },
  }
};
</script>
