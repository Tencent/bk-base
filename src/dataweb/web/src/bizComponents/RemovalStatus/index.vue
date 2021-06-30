

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
  <div class="data-preview-detail">
    <!-- 数据量start -->
    <div class="box-wrapper">
      <div class="chart-wrapper clearfix">
        <div class="left-chart">
          <div v-bkloading="{ isLoading: isChartLoading }"
            class="line-chart">
            <div v-show="chartData.length === 0 && !isChartLoading"
              :style="recent_wrapper_bg"
              class="no-data">
              <img alt
                src="@/common/images/no-data.png">
              <p>{{ $t('暂无数据') }}</p>
            </div>
            <PlotlyChart
              v-if="chartData.length > 0"
              :chartConfig="threeMinsCountChartConfig"
              :chartData="chartData"
              :chartLayout="threeMinsCountChartLayout" />
          </div>
        </div>
      </div>
    </div>
    <!-- 数据量end -->
  </div>
</template>

<script type="text/javascript">
let beautify = require('json-beautify');
import moment from 'moment';
import Vue from 'vue';
import VueClipboards from 'vue-clipboards';
import PlotlyChart from '@/components/plotly';

Vue.use(VueClipboards);
export default {
  components: {
    PlotlyChart,
  },
  props: {
    chartData: {
      type: Array,
      default: () => [],
    },
    isChartLoading: {
      type: Boolean,
      default: false,
    },
    dtick: {
      type: Number,
      default: 7
    },
    width: {
      type: Number,
      default: 537
    },
    margin: {
      type: Object,
      default: () => ({
        l: 60,
        r: 60,
        b: 90,
        t: 80,
      })
    }
  },
  data() {
    // common layout config of charts
    const COMMON_LAYOUT_CONFIG = {
      height: 300,
      width: this.width,
      margin: this.margin,
      showlegend: true,
      legend: {
        xanchor: 'center',
        yanchor: 'top',
        y: 4,
        x: 1,
      },
    };
    return {
      bizId: -1,
      deleteNodeInfo: {
        isShow: false,
        item: {},
      },
      pickerOptions: {
        shortcuts: [
          {
            text: this.$t('最近一周'),
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
              picker.$emit('pick', [start, end]);
            },
          },
          {
            text: this.$t('最近一个月'),
            onClick(picker) {
              const end = new Date();
              const start = new Date();
              start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
              picker.$emit('pick', [start, end]);
            },
          },
        ],
      },
      recent_wrapper_bg: {
        background: 'url(' + window.userPic + ') repeat',
      },
      loading: {
        historyLoading: true,
        minLoading: true,
        dayLoading: true,
        reportedLoading: true,
        nodeLoading: true,
        buttonLoading: false,
      },
      edit: false,
      dialog: {
        // 弹窗控制参数
        dialogStatus: false,
        dialogTheme: 'primary',
        confirm: this.$t('关闭'),
        hideCancel: 'dialog-pop hideCancel access-history',
        cancel: '',
        page: 1,
        page_size: 10,
        totalPage: 1,
      },
      issued: {
        // 下发弹窗
        dialogStatus: false,
      },
      reportedData: [], // 最近上报数据
      tabActiveName: 'nodeInfo',
      historyList: [], // 执行历史
      createData: {},
      popStatus: false, // 弹窗状态
      minuteTime: [],
      minuteTimezone: '',
      dayTime: [],
      dayTimezone: '',
      min_time: '',
      max_time: '',
      minuteParams: {
        // 分钟数据量参数
        frequency: '1m',
        start_time: '',
        end_time: '',
        date: [],
      },
      dayParams: {
        frequency: '1d',
        start_time: '',
        end_time: '',
        date: [],
      },
      paging: {
        // 节点分页
        totalPage: 1,
        page: 1, // 当前页码
        page_size: 10, // 每页数量
        ordering: '',
      },
      defaultDemo: {
        selected: 0,
      },
      historyId: 0,
      dayChartData: [{}],
      minuteChartData: [{}],
      threeMinsCountChartConfig: {
        // 配置需要的操作按钮图标
        operationButtons: ['autoScale2d', 'resetScale2d'],
      },
      threeMinsCountChartLayout: {
        // 3分钟数据量UI设置
        ...COMMON_LAYOUT_CONFIG,
        xaxis: {
          // dtick: 20, // 设置x轴间隔
          showgrid: false, // 是否显示x轴线条
          showline: true, // 是否绘制出该坐标轴上的直线部分
          tickwidth: 1,
          zeroline: false,
        },
        yaxis: {
          showline: true, // 是否绘制出该坐标轴上的直线部分
          tickwidth: 1,
          zeroline: false,
        },
      },
      dayDataCountChartConfig: {
        // 配置需要的操作按钮图标
        operationButtons: ['autoScale2d', 'resetScale2d'],
      },
      dayDataCountChartLayout: {
        ...COMMON_LAYOUT_CONFIG,
        xaxis: {
          dtick: this.dtick || 7, // 设置x轴间隔
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
  computed: {
    ranges() {
      let dateConf = {};
      let yesterKey = this.$t('昨天');
      let weekKey = this.$t('最近一周');
      let monthKey = this.$t('最近一个月');
      // let monthsKey = this.$t('最近三个月')
      dateConf[yesterKey] = [moment().subtract(1, 'days'), moment()];
      dateConf[weekKey] = [moment().subtract(7, 'days'), moment()];
      dateConf[monthKey] = [moment().subtract(1, 'month'), moment()];
      // dateConf[monthsKey] = [moment().subtract(3, 'month'), moment()]
      return dateConf;
    },
  },
  watch: {
    /*
                监听弹窗分页
            */
    'dialog.page': function (newVal, oldVal) {
      this.dialog.page = newVal;
      this.getHistoryList();
    },
    // chartData: {
    //     immediate: true,
    //     handler(val) {
    //         if (val.length) {
    //             if (val[0].x.length >= 100) {
    //                 this.threeMinsCountChartLayout.xaxis.dtick = 120
    //             } else {
    //                 this.threeMinsCountChartLayout.xaxis.dtick = 20
    //             }
    //         }
    //     }
    // }
  },
  mounted() {
    // this.getMinDataAmount()
  },
  methods: {
    getDateDiff(start, end) {
      let date1 = new Date(start);
      let date2 = new Date(end);
      let timeDiff = Math.abs(date2.getTime() - date1.getTime());
      let diffDays = Math.ceil(timeDiff / (1000 * 3600 * 24));
      return diffDays;
    },
    /**
     * @augments item 当前内容
     * 复制成功
     */
    copySuccess(item) {
      item.copy_Sucess = true;
      setTimeout(() => {
        item.copy_Sucess = false;
      }, 2000);
    },
    copyError(e) {
      console.log(e);
    },
    // 点击展开
    unfolded(item) {
      if (item.result === 'failed') {
        item.row_expanded = !item.row_expanded;
      }
    },
    // 编辑采集详情
    edit_detail() {
      this.edit = !this.edit;
      if (!this.edit) {
        console.log(this.$t('保存'));
      }
    },
    // 取消编辑
    cancel_edit() {
      this.edit = !this.edit;
    },
    closeDialog() {
      this.dialog.dialogStatus = false;
    },
    confirmFn() {
      this.dialog.dialogStatus = false;
    },
    cancelIssued(close) {
      close(true);
    },

    /*
                获取接入历史数据
            */
    getHistoryList() {
      let params = {
        ip: this.historyId,
        page: this.dialog.page,
        page_size: this.dialog.page_size,
      };
      this.axios
        .get('dataids/' + this.$route.params.did + '/get_exc_history/?' + this.qs.stringify(params))
        .then(res => {
          if (res.result) {
            this.historyList = res.data.results;
            for (var i = this.historyList.length - 1; i >= 0; i--) {
              // .row_expanded = false
              this.$set(this.historyList[i], 'row_expanded', false);
            }
            let totalPage = res.data.count;
            // 总页数不能为0
            this.dialog.totalPage = totalPage !== 0 ? totalPage : 1;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.loading.historyLoading = false;
        });
    },
    /*
                关闭弹窗
            */
    closePop() {
      this.popStatus = false;
    },

    /*
                3 分钟数据量日期选择
            */
    minuteChange() {
      if (this.minuteParams.date) {
        this.loading.minLoading = true;
        this.minuteParams.start_time =          this.minuteParams.date[0].getFullYear()
          + '-'
          + (this.minuteParams.date[0].getMonth() + 1)
          + '-'
          + this.minuteParams.date[0].getDate()
          + ' '
          + this.minuteParams.date[0].getHours()
          + ':'
          + this.minuteParams.date[0].getMinutes()
          + ':'
          + this.minuteParams.date[0].getSeconds();
        this.minuteParams.end_time =          this.minuteParams.date[1].getFullYear()
          + '-'
          + (this.minuteParams.date[1].getMonth() + 1)
          + '-'
          + this.minuteParams.date[1].getDate()
          + ' '
          + this.minuteParams.date[1].getHours()
          + ':'
          + this.minuteParams.date[1].getMinutes()
          + ':'
          + this.minuteParams.date[1].getSeconds();
        this.minuteParams.date = [this.minuteParams.start_time, this.minuteParams.end_time];
        this.getMinDataAmount();
      }
    },
    /*
                日数据量日期选择
            */
    dayChange() {
      if (this.dayParams.date) {
        this.loading.dayLoading = true;
        this.dayParams.start_time =          this.dayParams.date[0].getFullYear()
          + '-'
          + (this.dayParams.date[0].getMonth() + 1)
          + '-'
          + this.dayParams.date[0].getDate()
          + ' '
          + this.dayParams.date[0].getHours()
          + ':'
          + this.dayParams.date[0].getMinutes()
          + ':'
          + this.dayParams.date[0].getSeconds();
        this.dayParams.end_time =          this.dayParams.date[1].getFullYear()
          + '-'
          + (this.dayParams.date[1].getMonth() + 1)
          + '-'
          + this.dayParams.date[1].getDate()
          + ' '
          + 23
          + ':'
          + 59
          + ':'
          + 59;
        this.dayParams.date = [this.dayParams.start_time, this.dayParams.end_time];
        this.getDayDataAmount();
      }
    },

    refreshMinute() {
      this.loading.minLoading = true;
      this.getMinDataAmount();
    },
    refreshDay() {
      this.loading.dayLoading = true;
      this.getDayDataAmount();
    },

    init() {
      this.loading.nodeLoading = true;
      /*
                    初始化时间
                */
      let myDate = new Date();
      let minDate = new Date(new Date().getTime() - 24 * 60 * 60 * 1000); // 24小时前
      let minTime = new Date(new Date().getTime() - 91 * 24 * 60 * 60 * 1000); // 3个月前
      this.min_time = minTime.getFullYear() + '-' + (minTime.getMonth() + 1) + '-' + minTime.getDate();
      this.max_time = myDate.getFullYear() + '-' + (myDate.getMonth() + 1) + '-' + myDate.getDate();
      this.minuteParams.start_time =        minDate.getFullYear()
        + '-'
        + (minDate.getMonth() + 1)
        + '-'
        + minDate.getDate()
        + ' '
        + minDate.getHours()
        + ':'
        + minDate.getMinutes()
        + ':'
        + minDate.getSeconds();
      this.minuteParams.end_time =        myDate.getFullYear()
        + '-'
        + (myDate.getMonth() + 1)
        + '-'
        + myDate.getDate()
        + ' '
        + myDate.getHours()
        + ':'
        + myDate.getMinutes()
        + ':'
        + myDate.getSeconds();
      this.minuteParams.date = [this.minuteParams.start_time, this.minuteParams.end_time];
      this.getMinDataAmount();
      let dayDate = new Date(new Date().getTime() - 6 * 24 * 60 * 60 * 1000); // 一周前面
      this.dayParams.start_time = dayDate.getFullYear()
            + '-' + (dayDate.getMonth() + 1) + '-' + dayDate.getDate() + ' 00:00:00';
      this.dayParams.end_time = myDate.getFullYear()
            + '-' + (myDate.getMonth() + 1) + '-' + myDate.getDate() + ' 23:59:59';
      this.dayParams.date = [this.dayParams.start_time, this.dayParams.end_time];
      this.getDayDataAmount();
      if (!this.$route.params.did) {
        this.loading.reportedLoading = false;
        return;
      }
      /*
                    获取最近上报数据
                */
      this.axios.get('/v3/databus/rawdatas/' + this.$route.params.did + '/tail/').then(res => {
        if (res.result) {
          this.reportedData = [];
          for (let i = 0; i < res.data.length; i++) {
            this.reportedData.push({
              text: this.formatTailData(res.data[i].value),
              source: res.data[i].value,
              show: false,
              copy_Sucess: false,
            });
          }
        } else {
          // this.getMethodWarning(res.message, res.code)
        }
        this.loading.reportedLoading = false;
      });
    },

    formatTailData(data) {
      let res = '';
      try {
        res = JSON.stringify(JSON.parse(data));
      } catch (e) {
        res = data;
      }

      return res;
    },

    /*
                获取最近上报数据点击下拉展开
            */
    slide(item, e) {
      item.show = !item.show;
      if (item.show) {
        // item.json = item.text
        item.text = beautify(JSON.parse(item.source), null, 4, 80);
      } else {
        item.text = this.formatTailData(item.source);
        $(e.target)
          .closest('li')
          .find('.data')
          .css('display', '-webkit-box');
      }
    },
    /**
     * 动态获取y轴坐标刻度
     * */
    getYaxisDtick(dataArr) {
      const MAX = Math.ceil(Math.max(...dataArr) / 4);
      const STEP = Math.pow(10, MAX.toString().length - 1);
      return Math.floor(MAX / STEP) * STEP;
    },
    /*
                获取3分钟数据量
            */
    getMinDataAmount() {
      this.loading.minLoading = true;
      this.axios
        .get('dataids/' + 100758 + '/list_data_count_by_time/?' + this.qs.stringify(this.minuteParams))
        .then(res => {
          if (res.result) {
            this.minuteTime = res.data.time;
            this.minuteTimezone = res.data.timezone;
            let minuteData = {};
            if (this.minuteTime.length > 0) {
              this.threeMinsCountChartLayout.yaxis.dtick = this.getYaxisDtick(res.data.cnt);
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
    /*
                获取日数据量
            */
    getDayDataAmount() {
      this.axios
        .get('dataids/' + 100758 + '/list_data_count_by_time/?' + this.qs.stringify(this.dayParams))
        .then(res => {
          if (res.result) {
            this.dayTime = res.data.time;
            this.dayTimezone = res.data.timezone;
            if (this.dayTime.length > 0) {
              this.dayDataCountChartLayout.yaxis.dtick = this.getYaxisDtick(res.data.cnt);
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
  },
};
</script>

<style media="screen" lang="scss" scoped>
$bk-primary: #3a84ff;
$text-color: #212232;
$bg-color: #fff;
$data-success: #9dcb6b;
$data-doing: #fe771d;
$data-fail: #ff5656;
$data-nostart: #dedede;
.hide {
  display: none;
}
.data-preview-detail {
  position: relative;
  padding: 7px 10px;
  background: $bg-color;
  color: $text-color;
  background-color: #fafafa;
  width: 50%;
  .host-list {
    top: 0px;
    left: 610px !important;
  }
  .ip-select-bottom {
    text-align: center;
    padding-top: 30px;
    position: absolute;
    width: 100%;
    bottom: 0;
    height: 97px;
    border-top: 1px solid #efefef;
    button {
      margin: 0 20px;
      width: 97px;
    }
  }
  .sideslider-select1 {
    padding: 20px 50px;
    .bk-form-content {
      margin-left: 90px;
    }
    .bk-label {
      top: 25px;
    }
  }
  .bk-date .range-action a {
    display: inline-block;
    font-size: 14px;
    color: #3c96ff;
    text-decoration: none;
    line-height: 16px;
    padding: 8px 0 8px 16px;
  }
  .bk-date .range-action {
    padding: 6px 0;
  }
  .bk-date .date-range-view {
    background: #ebf4ff;
  }
  .sub-header {
    color: #737987;
    margin: 0 -70px;
    height: 61px;
    line-height: 61px;
    border-bottom: 1px solid #dfe2ef;
    padding-left: 75px;
    button {
      background-color: #3a84ff;
      width: 60px;
      height: 26px;
      line-height: 25px;
      color: #fff;
      font-size: 11px;
      padding: 0px;
      border: none;
      display: inline-block;
      vertical-align: 2px;
    }
    li {
      display: inline-block;
      position: relative;
      padding: 0 25px 0 0;
      line-height: 60px;
      &:not(:last-child) {
        cursor: pointer;
      }
      &:last-child {
        color: #212232;
      }
      i {
        line-height: 60px;
        font-size: 10px;
        position: absolute;
        right: 4px;
        top: 0;
        transform: scale(0.7);
      }
    }
    .breadcrumb {
      height: 60px;
    }
  }
  .box-wrapper {
    .title {
      font-size: 12px;
      .title-left {
        float: left;
        font-size: 16px;
        .line {
          display: inline-block;
          vertical-align: top;
          width: 4px;
          height: 19px;
          background: $bk-primary;
          margin-right: 16px;
        }
      }
      .title-right {
        float: left;
        color: $bk-primary;
        .data-icon {
          display: inline-block;
          margin-left: 30px;
          cursor: pointer;
          line-height: 21px;
          > i {
            display: inline-block;
            vertical-align: -1px;
            width: 14px;
            height: 14px;
            margin-right: 10px;
            position: relative;
            top: 2px;
          }
        }
      }
    }
    .detail-wrapper {
      margin: 0 -70px;
      padding: 0 70px 0px;
      li {
        height: 61px;
        line-height: 60px;
        border-bottom: 1px solid $bg-color;
        font-size: 0;
        .title {
          font-size: 14px;
          float: left;
          width: 130px;
          background-color: #fafafafa;
          color: #737987;
          text-align: right;
          padding: 0 20px;
          border-right: 1px solid $bg-color;
        }
        .detail {
          font-size: 14px;
          float: left;
          padding: 0 20px;
          width: calc(100% - 130px);
          height: 60px;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }
      }
      .left-box {
        float: left;
        background: #fff;
        width: 35%;
        height: 243px;
        box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      }
      .right-box {
        float: right;
        background: #fff;
        box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
        width: calc(65% - 25px);
        height: 243px;
        &.database {
          li:nth-child(1),
          li:nth-child(2),
          li:nth-child(3),
          li:nth-child(4) {
            width: 50%;
            float: left;
          }
        }
        li:nth-child(1),
        li:nth-child(3) {
          border-right: 1px solid $bg-color;
        }
        li {
          float: left;
          width: 100%;
        }
      }
    }
    .chart-wrapper {
      background: #fafafa;
      .bk-date .daterange-dropdown-panel {
        z-index: 1000;
      }
      .bk-date .date-dropdown-panel {
        right: 0;
        left: auto;
        z-index: 1001;
      }
      .header {
        border-bottom: 1px solid #f2f4f9;
        padding: 10px 15px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        .header-title-left {
          float: left;
          font-size: 16px;
          .line {
            display: inline-block;
            vertical-align: top;
            width: 2px;
            height: 19px;
            background: $bk-primary;
            margin-right: 16px;
          }
        }
      }

      .left-chart {
        background: #fff;
        box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
        float: left;
        height: 300px;
        position: relative;
        width: 100%;
      }
      .line-chart {
        height: calc(100% - 87px);
      }
      .right-chart {
        background: #fff;
        box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
        float: right;
        height: 300px;
        position: relative;
        width: calc(50% - 13px);
      }
      .no-data {
        position: absolute;
        text-align: center;
        left: 50%;
        top: 50%;
        transform: translate(-50%, 0);
        p {
          color: #cfd3dd;
        }
      }
    }
    .node-wrapper {
      background: #fff;
      box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      table {
        width: 100%;
        text-align: left;
        tr {
          height: 48px;
          th {
            cursor: pointer;
            background: #fafafa;
            font-weight: normal;
            i {
              float: right;
              width: 14px;
              height: 14px;
              /*&:hover {
                                opacity: 0.8;
                            }*/
            }
          }
          td {
            &.success {
              color: $data-success;
            }
            &.exception {
              color: $data-doing;
            }
            &.doing {
              color: $data-doing;
            }
            &.nostart {
              color: $data-nostart;
            }
            &.fail {
              color: $data-fail;
            }
            &.btn-wrapper {
              font-size: 0;
              a {
                font-size: 14px;
                color: $bk-primary;
                margin-right: 30px;
                padding: 14px 0;
                &:hover {
                  color: #333982;
                }
                &.disable {
                  cursor: not-allowed;
                  color: #ccc;
                }
              }
              .btn-delete {
                position: relative;
                &:hover {
                  .delete-confirm {
                    display: block;
                  }
                }
              }
              .delete-confirm {
                display: none;
                position: absolute;
                bottom: 40px;
                left: -40px;
                width: 256px;
                background: #e1ecff;
                border: 1px solid #d4d6f3;
                padding: 15px;
                font-size: 14px;
                color: #737987;
                &:before {
                  content: '';
                  position: absolute;
                  left: 48px;
                  bottom: -11px;
                  width: 0;
                  height: 0;
                  border: 5px solid;
                  border-color: #d4d6f3 transparent transparent transparent;
                }
                &:after {
                  content: '';
                  position: absolute;
                  left: 48px;
                  bottom: -10px;
                  width: 0;
                  height: 0;
                  border: 5px solid;
                  border-color: #e1ecff transparent transparent transparent;
                }
              }
              .confirm-buttom {
                margin-top: 5px;
              }
            }
            &.progress-bar {
              .bar-wrapper {
                position: relative;
                width: 100%;
                height: 6px;
                border-radius: 3px;
                background: #e6e6e6;
                .bar {
                  position: absolute;
                  top: 0;
                  left: 0;
                  height: 6px;
                  border-radius: 3px;
                  background: #85d14d;
                }
              }
            }
          }
        }
        td,
        th {
          padding: 0 15px 0 42px;
          border: 1px solid $bg-color;
        }
        .no-data {
          margin: 38px 0;
          text-align: center;
          p {
            color: #cfd3dd;
          }
        }
      }
      .page-wrapper {
        text-align: right;
        padding: 20px 30px 20px 0;
      }
    }
    .recent-wrapper {
      background: #fff;
      box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      min-height: 150px;
      .title {
        width: 250px;
        padding: 10px 0px;
      }
      ul {
        position: relative;
        min-height: 70px;
        li {
          border-top: 1px solid $bg-color;
          display: flex;
          min-height: 60px;
          &:hover {
            background: #f4f6fb;
            .symbol {
              line-height: 46px;
              background: #a3c5fd;
              border: 1px solid #3a84ff;
            }
          }
          .symbol {
            box-sizing: border-box;
            line-height: 48px;
            width: 44px;
            text-align: center;
            border-right: 1px solid $bg-color;
            background: #fafafa;
          }
          .content {
            padding: 9px 0;
            width: calc(100% - 230px);
            padding-left: 15px;
            font-size: 14px;
            line-height: 30px;
            word-break: break-all;
            display: flex;
            justify-content: flex-start;
            .active {
              text-overflow: ellipsis;
              overflow: hidden;
              white-space: nowrap;
            }
            .show-all {
              display: inline !important;
            }
            .data {
              white-space: pre-wrap;
              word-break: break-all;
              display: -webkit-box;
              -webkit-box-orient: vertical;
              -webkit-line-clamp: 3;
              overflow: hidden;
            }
          }
          .slide {
            line-height: 48px;
            color: $bk-primary;
            text-align: right;
            flex: 1;
            position: relative;
            // overflow: hidden;
            .slide-button {
              padding: 14px 15px;
              cursor: pointer;
              color: $bk-primary;
              &:hover {
                opacity: 0.8;
              }
            }
            .copy-button {
              margin-right: 15px;
              position: relative;
              .copytips {
                position: absolute;
                top: 50px;
                font-size: 12px;
                line-height: 16px;
                font-style: normal;
                font-weight: 400;
                text-align: left;
                color: rgb(255, 255, 255);
                z-index: 1070;
                transform: translate(0px, -50%);
                padding: 10px;
                background: rgb(80, 80, 80);
                border-radius: 5px;
                left: -20px;
                pointer-events: none;
                &:before {
                  content: '';
                  position: absolute;
                  top: calc(-5px);
                  width: 0;
                  height: 0;
                  border: 7px solid;
                  transform: translate(0, -50%);
                  left: 37px;
                  border-color: transparent transparent #505050 transparent;
                }
              }
            }
            i {
              position: relative;
              top: 4px;
              display: inline-block;
              width: 16px;
              height: 16px;
              // background: url('../../common/images/icon/icon-arrows-down-circle.png');
              margin-left: 10px;
              transition: 0.2s ease all;
            }
          }
          &.active {
            background: rgba(244, 246, 251, 0.5);
            .symbol {
              line-height: 46px;
              background: #a3c5fd;
              border: 1px solid #3a84ff;
              min-height: 100px;
            }
            .content {
              width: calc(100% - 230px);
              overflow: initial;
              white-space: inherit;
              overflow-x: auto;
            }
            .slide {
              i {
                transform: rotate(180deg);
              }
            }
          }
        }
        .no-data {
          position: absolute;
          text-align: center;
          left: 50%;
          top: 50%;
          transform: translate(-50%, -50%);
          p {
            color: #cfd3dd;
          }
        }
      }
    }
  }
  .access-history {
    .table {
      width: 100%;
      text-align: left;
      margin-top: 25px;
      .sorting {
        float: right;
        width: 14px;
        height: 14px;
        // background: url('../../common/images/icon/icon-sort.png');
        cursor: pointer;
        &:hover {
          opacity: 0.8;
        }
      }
      .bk-table > thead > tr > th {
        font-weight: normal;
        color: #212232;
      }
      .bk-table > tbody > tr > td {
        font-size: 14px;
        color: #212232;
        &.success {
          color: $data-success;
        }
        &.doing {
          color: $data-doing;
        }
        &.failure {
          color: $data-fail;
        }
        &.member {
          color: #3a84ff;
        }
        &.errormsg {
          padding: 0;
        }
      }
    }
    .table-page {
      text-align: right;
      padding: 20px;
      border: 1px solid #e6e6e6;
      border-top: none;
    }
    .bk-dialog-outer button {
      margin-top: 0;
    }
    .dialog-pop .bk-dialog-footer,
    .bk-dialog-footer {
      height: 80px;
      line-height: 60px;
    }
    .dialog-pop .close-pop {
      right: -13px;
      top: -14px;
    }
    .expand-wrapper {
      height: 100px;
      padding: 20px;
      overflow-y: auto;
      background-color: #f4f6fb;
    }
  }
  .el-date-editor .el-range__close-icon {
    right: 0;
  }

  @media (min-width: 800px) {
    .refesh-for-sscreen {
      display: block;
      cursor: pointer;

      &:hover {
        color: #3a84ff;
      }
    }

    .refesh-for-xscreen {
      display: none;
    }
  }
  @media (min-width: 1523px) {
    .refesh-for-xscreen {
      display: block;
    }

    .refesh-for-sscreen {
      display: none;
    }
  }
}
</style>
