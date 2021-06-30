

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
  <div class="detaile-card-wrapper">
    <div class="title">
      {{ titleName }}
      <div v-if="dataType !== 'app' && chartData.length"
        class="chart-info fr">
        <p class="data-percent">
          {{ lastChartData.rate || 0 }}<span class="unit">%</span>
        </p>
        <p :style="`color: ${chartsOptions.stroke}`">
          {{ lastChartData.used }} of {{ lastChartData.total }}
        </p>
      </div>
    </div>
    <div v-bkloading="{ isLoading: loading }"
      class="content"
      style="height: calc(100% - 40px)">
      <div v-if="chartData.length"
        class="chart"
        style="height: 100%">
        <!-- <d3Pie v-if="type === 'pie'"
                    :data="chartData"
                    :options="{
                        innerRadius: 1,
                        padAngle: 0.01,
                        defaultColor: ['#73DEB3','#73A0FA','#EB7E65','#F7C639','#7585A1'],
                        arcTitle: d => `<p>${d.data.name}</p><p>${d.data.value}</p>`
                    }"
                    height="259px" /> -->
        <BKVLine :series="lineDataSet || []"
          :labels="labels"
          :options="lineOptions"
          :autoUpdate="true" />
      </div>
      <div v-else
        class="empty">
        <span class="icon-empty" />
        <div>{{ $t('暂无数据') }}</div>
      </div>
    </div>
  </div>
</template>
<script>
import moment from 'moment';
import Bus from '@/common/js/bus.js';
import { BKVLine } from '@/components/bkChartVue/Components/index';
export default {
  components: {
    BKVLine,
  },
  props: {
    titleName: {
      type: String,
      default: '',
    },
    chartsOptions: {
      type: Object,
      default: () => ({
        fill: '#6eadc1',
        stroke: 'rgb(188, 82, 188)',
      }),
    },
    serviceType: {
      type: String,
      default: '',
    },
    clusterId: {
      type: String,
      default: '',
    },
    timeUnit: {
      type: String,
      default: 'hour',
    },
    type: {
      type: String,
      default: 'area',
    },
    dataType: {
      type: String,
      default: 'cpu',
    },
  },
  data() {
    return {
      labels: [],
      lineDataSet: null,
      lineOptions: {
        scales: {
          xAxes: {
            ticks: {
              color: '#979BA5',
              callback: (value, index, values) => {
                switch (this.timeUnit) {
                  case 'day':
                    return moment(this.labels[index]).format('YYYY-MM-DD');
                    // return value.slice(0, 10);
                  default:
                    if (index % 4 === 0) {
                      return moment(this.labels[index]).format('HH: mm');
                      // return value.slice(-8, -3);
                    } else {
                      return '';
                    }
                }
              },
            },
          },
        },
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: false,
          },
          tooltip: {
            enabled: true,
            intersect: false,
          },
        },
      },
      chartData: [],
      lastChartData: {
        value: 0,
        total: 0,
        used: 0,
      },
      loading: false,
      startTime: '',
      endTime: '',
      httpUrl: '',
    };
  },
  computed: {
    resourceTypeUrlName() {
      return `${this.$route.params.type}_metrics`;
    },
  },
  created() {
    this.initParams();
  },
  beforeDestroy() {
    Bus.$off('getResourceChartsData');
  },
  methods: {
    initParams() {
      if (this.timeUnit === '5min') {
        this.startTime = moment().format('YYYY-MM-DD 00:00:00');
        this.endTime = moment().format('YYYY-MM-DD 23:59:59');
      } else {
        this.startTime = moment().subtract('days', 6)
          .format('YYYY-MM-DD HH:mm:ss');
        this.endTime = moment().format('YYYY-MM-DD HH:mm:ss');
      }
      switch (this.dataType) {
        case 'cpu':
          this.httpUrl = 'resourceManage/getCpuHistory';
          break;
        case 'memory':
          this.httpUrl = 'resourceManage/getMemoryHistory';
          break;
        case 'disk':
          this.httpUrl = 'resourceManage/getDiskHistory';
          break;
        case 'app':
          this.httpUrl = 'resourceManage/getAppHistory';
          break;
        default:
          this.httpUrl = 'resourceManage/getTaskHistory';
          break;
      }

      Bus.$on('getResourceChartsData', this.getChartsData);
    },
    getChartsData() {
      if (!this.serviceType) return;
      this.loading = true;
      this.labels = [];
      this.bkRequest
        .httpRequest(this.httpUrl, {
          params: {
            resource_group_id: this.$route.params.resourceId,
            resourceType: this.resourceTypeUrlName,
          },
          query: {
            geog_area_code: 'inland',
            service_type: this.serviceType,
            start_time: this.startTime,
            end_time: this.endTime,
            time_unit: this.timeUnit,
            cluster_id: this.clusterId,
          },
        })
        .then(res => {
          if (res.result) {
            this.chartData =              this.dataType === 'app'
              ? res.data.history.map(item => {
                const result = {
                  key: item.time,
                  value: item.used,
                };
                return result;
              })
              : res.data.history.map(item => {
                const result = {
                  key: item.time,
                  value: item.total === 0 ? 0 : ((item.used / item.total) * 100).toFixed(2),
                  total: item.total.toFixed(2),
                  used: item.used.toFixed(2),
                };
                return result;
              });

            // 从接口获取展示的数值及单位
            this.lastChartData = res.data.last;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          let data = [];
          this.chartData.forEach(item => {
            data.push(parseFloat(item.value));
            this.labels.push(item.key);
          });
          this.lineDataSet = [
            {
              data: data,
              type: 'line',
              fill: true,
              backgroundColor: this.chartsOptions.fill,
              borderColor: this.chartsOptions.stroke,
            },
          ];
          this.loading = false;
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.detaile-card-wrapper {
  #container {
    height: 100%;
  }
  padding: 28px 24px;
  width: 100%;
  height: 370px;
  border: 1px solid #dcdee5;
  .title {
    width: 100%;
    padding-left: 6px;
    font-size: 18px;
    font-weight: 400;
    text-align: left;
    color: #63656e;
    line-height: 18px;
    border-left: 4px solid #3a84ff;
    .data-percent {
      font-size: 30px;
      text-align: center;
      margin-bottom: 10px;
      position: relative;
      .unit {
        font-size: 20px;
        position: absolute;
        top: -5px;
      }
    }
  }
  .content {
    margin-top: 35px;
    .empty {
      height: 275px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      .icon-empty {
        font-size: 65px;
      }
    }
  }
}
</style>
