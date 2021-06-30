

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
  <div :class="{ noselect: isScroll }"
    class="retrieval">
    <div class="result">
      <div v-show="showTitle"
        class="clearfix titel-wraper">
        <p class="fl title">
          {{ $t('结果数据表') }}：
          <span>
            {{ resultTable }}
            <!-- todo中文名 -->
            <!-- <span v-if="chineseData"> ({{chineseData}})</span> -->
          </span>
        </p>
      </div>
      <div class="searchbox">
        <div class="time-choice">
          <bkdata-date-picker
            v-show="!nodeSearch"
            v-model="datePickerArr"
            :placeholder="$t('请选择')"
            :type="'datetimerange'"
            :shortcuts="shortcuts"
            style="width: 510px"
            format="yyyy-MM-dd HH:mm"
            @change="dateChange" />
        </div>
        <div class="search-box">
          <bkdata-input
            v-model="esParams.keyword"
            :placeholder="$t('请输入需要检索的关键字')"
            autofocus
            clearable
            class="search-input"
            type="text"
            @enter="search" />
          <button
            :class="{ 'is-loading': searchLoading }"
            :disabled="!resultTable"
            style="margin-right: 5px"
            class="bk-button bk-primary bk-button-large"
            type="button"
            @click="search">
            <span>{{ btnText }}</span>
          </button>
          <a v-show="!nodeSearch"
            class="grammar"
            :href="textsearch"
            target="_blank">
            <i class="bk-icon icon-help-fill" />
            {{ $t('检索规则') }}
          </a>
        </div>
        <p class="number">
          {{ numberPrompt }}
        </p>
        <p class="time-cost">
          <span v-if="searchLoading">{{ $t('耗时') }}（{{ timerTxt }}s）</span>
          <span v-if="!loading.searchLoading && searchFailed"
            class="failed">
            {{ failedInfo }}
          </span>
          <bkdata-popover
            v-if="!loading.searchLoading && searchFailed && failedError && failedError.error"
            placement="top"
            :theme="'light'">
            <a href="jacascript:;">{{ $t('详情') }}</a>
            <div slot="content"
              style="white-space: normal">
              <div class="bk-text-danger error-tooltips-detail bk-scroll-y pt10 pb5 pl10 pr10">
                {{ failedError.error }}
              </div>
            </div>
          </bkdata-popover>
          <i18n v-show="timerTxt !== 0 && !searchLoading && !searchFailed"
            path="找到_条结果_用时_秒"
            tag="span">
            <strong place="num">{{ resultList.total }}</strong>
            <strong place="time">{{ timerTxt }}</strong>
          </i18n>
          <span v-if="!searchLoading && !loading.searchFailed && !failedError"
            class="ml5">
            {{ UTCtips }}
            <bkdata-popover class="item"
              effect="dark"
              placement="right">
              <div slot="content"
                class="tooltip time-tooltip">
                <p>
                  <span class="subtitle">{{ $t('当前时间') }}</span>
                  : {{ noteTime.user_time }}
                </p>
                <p>
                  <span class="subtitle">{{ $t('转换时间') }}</span>
                  : {{ noteTime.utc_time }}
                </p>
              </div>
              <span class="note">
                <i class="bk-icon icon-info-circle" />
              </span>
            </bkdata-popover>
          </span>
        </p>
      </div>
      <bkdata-tab :active="active_Name"
        class="esTab"
        @tab-change="tabChanged">
        <bkdata-tab-panel v-if="!nodeSearch"
          :label="$t('检索历史')"
          class="estab-panel"
          name="history">
          <div v-bkloading="{ isLoading: historyLoading }">
            <bkdata-table
              :data="historyList"
              :stripe="true"
              :outerBorder="false"
              :pagination="pagination"
              :emptyText="$t('暂无数据')"
              @page-change="handlePageChange"
              @page-limit-change="handlePageLimitChange">
              <bkdata-table-column :label="$t('关键词')">
                <template slot-scope="props">
                  <span
                    v-if="props.row.keyword"
                    :id="'esHistoryList' + props.$index"
                    class="beyond"
                    @mouseleave="removeTip"
                    @mouseover="showTips('esHistoryList' + props.$index, props.row.keyword)">
                    {{ props.row.keyword }}
                  </span>
                  <p v-else
                    class="no-keyword">
                    {{ $t('无关键字') }}
                  </p>
                </template>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('查询范围')"
                width="200">
                <template slot-scope="props">
                  <p>{{ props.row.search_range_start_time }} ~</p>
                  <p>{{ props.row.search_range_end_time }}</p>
                </template>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('查询时间')"
                width="180"
                prop="time" />
              <bkdata-table-column :label="$t('耗时_秒')"
                width="120"
                prop="time_taken" />
              <bkdata-table-column :label="$t('结果条数')"
                width="100"
                prop="total" />
              <bkdata-table-column :label="$t('操作')"
                width="100">
                <template slot-scope="props">
                  <a class="check-again"
                    href="javascript:;"
                    @click="checkAgain(props.row)">
                    {{ $t('再次查询') }}
                  </a>
                </template>
              </bkdata-table-column>
            </bkdata-table>
          </div>
        </bkdata-tab-panel>
        <bkdata-tab-panel v-if="charShow || nodeSearch"
          :label="$t('查询结果')"
          class="estab-panel"
          name="searchInfo">
          <div v-bkloading="{ isLoading: chartLoading }"
            class="chart-wrapper">
            <div class="chart-menu">
              <span class="f14 title-time">{{ searchParams.start_time }} - {{ searchParams.end_time }}</span>
              <ul v-show="!noCharData"
                class="fr"
                style="z-index: 1000">
                <li
                  :class="{ 'chart-active': timeRound == 'minute' }"
                  class="inline-block vm"
                  @click="changeChart('minute')">
                  {{ $t('分') }}
                </li>
                <li
                  :class="{ 'chart-active': timeRound == 'hour' }"
                  class="inline-block vm"
                  @click="changeChart('hour')">
                  {{ $t('小时') }}
                </li>
                <li :class="{ 'chart-active': timeRound == 'day' }"
                  class="inline-block vm"
                  @click="changeChart('day')">
                  {{ $t('天') }}
                </li>
                <li
                  :class="{ 'chart-active': timeRound == 'week' }"
                  class="inline-block vm"
                  @click="changeChart('week')">
                  {{ $t('周') }}
                </li>
                <li
                  :class="{ 'chart-active': timeRound == 'month' }"
                  class="inline-block vm"
                  @click="changeChart('month')">
                  {{ $t('月') }}
                </li>
              </ul>
            </div>
            <!-- <div id="chart" style="width: 100%;height:240px;" v-show="!noCharData"></div> -->
            <div id="chart">
              <PlotlyChart
                v-show="!noCharData"
                :chartConfig="queryChartConfig"
                :chartData="queryChartData"
                :chartLayout="queryChartLayout" />
            </div>

            <div v-show="noCharData"
              class="no-data">
              <img alt
                src="../../../common/images/no-data.png">
              <p v-if="!nodeSearch">
                {{ $t('暂无数据_请查看') }}
                <a class="blue-font"
                  target="_blank"
                  :href="textsearch">
                  {{ $t('检索规则') }}
                </a>
              </p>
              <p v-else>
                {{ $t('暂无数据') }}
              </p>
            </div>
          </div>
          <div v-if="!resultLoading && resultList.list.length"
            style="min-height: 30vh; padding-bottom: 30px">
            <SearchResultTable
              :serveSidePage="true"
              :resultList="resultList"
              :pagination="esPagination"
              @changed="handleResultChange" />
          </div>
        </bkdata-tab-panel>
      </bkdata-tab>
    </div>
    <!-- <bkdata-sideslider :class="'grammar-rules'"
            id="bkdata_dataquery_slider"
            :is-show.sync="sideslider.show"
            :quick-close="true"
            :title="sideslider.title"
            :width="sideslider.width">
            <div class="rules-content"
                slot="content">
                <div @click="sideslider.show = !sideslider.show"
                    class="close">
                    <i class="bk-icon icon-close"></i>
                </div>
                <iframe :src="textsearch"
                    class="rules"
                    height="100%"
                    title="iframe example 1"
                    width="100%"></iframe>
            </div>
        </bkdata-sideslider> -->
  </div>
</template>

<script>
const today = new Date();
const yesterday = new Date(
  today.getFullYear(),
  today.getMonth(),
  today.getDate() - 1,
  today.getHours(),
  today.getMinutes(),
  today.getSeconds()
);
import moment from 'moment';
import { postMethodWarning } from '@/common/js/util.js';
import $ from 'jquery';
import PlotlyChart from '@/components/plotly';
import SearchResultTable from './SearchResultTable';
import axios from 'axios';
import mixin from '@/common/js/queryMixin.js';

export default {
  components: {
    PlotlyChart,
    SearchResultTable,
  },
  mixins: [mixin],
  props: {
    loading: {
      type: Boolean,
    },
    resultTable: {
      type: String,
    },
    chineseData: {
      type: String,
    },
    nodeSearch: {
      type: Boolean,
      default: false,
    },
    showTitle: {
      type: Boolean,
      default: true,
    },
  },

  data() {
    return {
      shortcuts: [
        {
          text: this.$t('今天'),
          value() {
            const end = new Date();
            const start = new Date();
            return [start, end];
          },
          onClick: picker => {
            console.error(picker);
          },
        },
        {
          text: this.$t('最近7天'),
          value() {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 7);
            return [start, end];
          },
        },
        {
          text: this.$t('最近15天'),
          value() {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 15);
            return [start, end];
          },
        },
        {
          text: this.$t('最近30天'),
          value() {
            const end = new Date();
            const start = new Date();
            start.setTime(start.getTime() - 3600 * 1000 * 24 * 30);
            return [start, end];
          },
        },
      ],
      stopBtn: false,
      source: null,
      isScroll: false,
      textsearch: this.$store.getters['docs/getPaths'].esSearchRule,
      scrollTop: false,
      headerFixed: false,
      scrollShow: false, // 滚动条显示
      searchFailed: false,
      failedInfo: '',
      topShow: false, // 回到顶部按钮显示
      sideslider: {
        // 侧栏弹窗
        show: false,
        width: 1110,
        title: '1',
      },
      searchLoading: false,
      resultTableLoading: true,
      noCharData: false,
      historyList: [], // 查询历史
      historyLoading: false,
      searchTime: false,
      select: {
        // 显示项下拉
        optionsList: [],
        selected: [],
        multiSelect: true,
      },
      charShow: false,
      date: {
        // 日期选择
        date: [],
      },
      datePickerArr: [moment(yesterday).format('YYYY-MM-DD HH:mm:ss'), moment(today).format('YYYY-MM-DD HH:mm:ss')],
      moreLoading: false,
      timeRound: 'hour',
      searchParams: {
        start_time: moment(yesterday).format('YYYY-MM-DD HH:mm:ss'),
        end_time: moment(today).format('YYYY-MM-DD HH:mm:ss'),
      },
      esParams: {
        start_time: moment(yesterday).format('YYYY-MM-DD HH:mm:ss'),
        end_time: moment(today).format('YYYY-MM-DD HH:mm:ss'),
        page: 1,
        page_size: 30,
        keyword: '',
        totalPage: '',
      },
      resultList: {
        // 查询结果列表
        time_taken: 0,
        total: 0,
        list: [],
      },
      resultLoading: false,
      chartLoading: false,
      scrollMore: true,
      timerTxt: 0,
      tabname: '',
      queryChartData: [{}],
      queryChartConfig: {
        operationButtons: ['toImage', 'autoScale2d', 'resetScale2d'],
      },
      queryChartLayout: {
        height: 240,
        width: '100%',
        margin: {
          l: 60,
          r: 90,
          b: 100,
          t: 0,
        },
        showlegend: true,
        legend: {
          x: 0.5,
          y: 1.3,
        },
        xaxis: {
          autorange: true,
          showgrid: false, // 是否显示x轴线条
          showline: true, // 是否绘制出该坐标轴上的直线部分
          tickwidth: 1,
          zeroline: true,
          type: 'category',
        },
        yaxis: {
          autotick: false,
          showline: true, // 是否绘制出该坐标轴上的直线部分
          tickwidth: 1,
          tickmode: 'auto',
          zeroline: false,
        },
      },
      failedError: null,
      curPage: 1,
      pageCount: 10,
      totalPage: 1,
    };
  },
  computed: {
    btnText() {
      return this.stopBtn ? this.$t('停止') : this.$t('检索');
    },
    numberPrompt: function () {
      let isNum = this.esParams.keyword.trim() && !isNaN(Number(this.esParams.keyword));
      return isNum ? this.$t('int类型检索_请使用字段名_字段值') : '  ';
    },
    active_Name: function () {
      return !this.nodeSearch ? 'history' : 'searchInfo';
    },
    noteTime: function () {
      const time = {};
      time.user_time = `${moment().format('YYYY-MM-DD HH:mm:ss')} ${new Date().toString()
        .slice(25)}`;
      time.utc_time = `${moment.utc().format('YYYY-MM-DD HH:mm:ss')} GTM+0000 UTC`;
      return time;
    },
    esPagination() {
      return {
        count: this.esParams.totalPage,
        limit: this.esParams.page_size,
        current: this.esParams.page,
      };
    },
    pagination() {
      return {
        count: this.totalPage,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
  },
  created() {
    // this.init()
    localStorage.setItem(':sidebar', 'false');
  },
  methods: {
    forMateDate(time) {
      const index = time.indexOf('-');
      return time.slice(index + 1);
    },
    handlePageChange(page) {
      this.curPage = page;
      this.getHistoryList();
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
      this.getHistoryList();
    },
    handleResultChange(val) {
      if (this.esParams.page !== val.current || this.esParams.page_size !== val.limit) {
        this.esParams.page = val.current;
        this.esParams.page_size = val.limit;
        this.inquire();
      }
    },
    showTips(id, content) {
      this.toolTip(id, content, 'right', false, false);
    },
    removeTip() {
      $('.def-tooltip').remove();
    },
    tabChanged(name) {
      this.tabname = name;
    },
    optionsChange() {
      console.log(this.select.selected);
    },
    // 時間選擇
    dateChange() {
      this.date.date = this.datePickerArr;
      this.timeRound = 'hour';
      this.timeFormate();
    },
    getTheDate() {
      let nowDate = new Date();
      let yesterday = new Date(nowDate - 24 * 3600 * 1000);
      this.date.date = [yesterday, nowDate];
      this.timeFormate();
    },
    timeFormate() {
      this.esParams.start_time = moment(this.datePickerArr[0]).format('YYYY-MM-DD HH:mm:ss');
      this.esParams.end_time = moment(this.datePickerArr[1]).format('YYYY-MM-DD HH:mm:ss');
    },
    // 点击再次查询
    checkAgain(item) {
      this.esParams.keyword = item.keyword;
      this.date.date = [new Date(item.search_range_start_time), new Date(item.search_range_end_time)];
      this.timeFormate();
      this.search();
    },
    // 获取历史
    getHistoryList() {
      if (!this.resultTable) {
        return;
      }
      this.historyLoading = true;
      this.axios
        .get('/v3/datalab/es_query/' + this.resultTable + '/list_query_history/', {
          params: {
            storage_type: 'es',
            page: this.curPage,
            totalPage: this.totalPage,
            page_size: this.pageCount,
          },
        })
        .then(res => {
          if (res.result) {
            let list = [];
            for (let i = 0; i < res.data.results.length; i++) {
              list.push(res.data.results[i]);
            }
            this.totalPage = res.data.count;
            this.historyList = list;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.historyLoading = false;
        });
    },
    // 获取图表数据
    changeChart(interval) {
      this.chartLoading = true;
      this.timeRound = interval;
      this.searchParams.start_time = this.esParams.start_time;
      this.searchParams.end_time = this.esParams.end_time;
      let chartParm = {
        keyword: this.esParams.keyword,
        interval: interval,
        start_time: this.searchParams.start_time,
        end_time: this.searchParams.end_time,
      };
      let self = this;
      const plotlyChartBottomMargin = {
        minute: 150,
        hour: 100,
        day: 50,
        week: 50,
        month: 50,
      };

      this.queryChartLayout.margin.b = plotlyChartBottomMargin[interval];

      this.axios
        .post('/v3/datalab/es_query/' + this.resultTable + '/get_es_chart/', chartParm, {
          emulateJSON: true,
          cancelToken: this.source.token,
        })
        .then(response => {
          if (response.result) {
            this.chartLoading = false;
            if (response.data.cnt.length === 0) {
              this.noCharData = true;
              return;
            }
            this.noCharData = false;
            if (['minute', 'hour'].includes(interval)) {
              response.data.time = response.data.time.map(item => this.forMateDate(item));
            }
            this.$nextTick(() => {
              // 绘制图表
              let dayData = {
                xAxis: response.data.time,
                series: [
                  {
                    name: this.$t('数据量'),
                    type: 'bar',
                    data: response.data.cnt,
                  },
                ],
              };
              this.queryChartLayout.xaxis.tickangle = response.data.time.length > 20 ? 45 : null;
              this.renderChart(dayData);
            });
            this.stopBtn = false;
          } else {
            this.noCharData = true;
            postMethodWarning(response.message, 'error');
            this.chartLoading = false;
          }
        })
        ['catch'](thrown => {
          this.noCharData = true;
          this.chartLoading = false;
        });
    },

    // 图表渲染
    renderChart(data) {
      this.queryChartData = [
        {
          x: data.xAxis,
          y: data.series[0].data,
          name: data.series[0].name,
          type: 'bar',
          marker: {
            color: '#3a84ff',
          },
        },
      ];

      const rightSpace = this.timeRound === 'hour' ? 80 : 60;
      this.queryChartLayout.width = $('#chart').width() - rightSpace;
      this.chartLoading = false;
      // end
    },

    // 点击搜索一下
    search() {
      if (this.stopBtn) {
        this.stopBtn = false;
        this.source && this.source.cancel('Operation canceled by the user.');
        return;
      }
      this.stopBtn = true;
      this.source = axios.CancelToken.source();
      if (this.searchLoading) return;
      if (this.date.date === null) {
        this.date.date = {};
      }
      if (this.date.date.length === 0) {
        this.getTheDate();
      }

      this.esParams.page = 1;
      this.charShow = true;
      this.inquire();
      this.changeChart(this.timeRound);

      this.$nextTick(() => {
        document.querySelectorAll('.retrieval .bk-tab-label-item')[1].click();
      });
    },
    async inquire() {
      this.resultLoading = true;
      if (this.searchLoading) return;
      this.searchLoading = true;
      this.searchFailed = false;
      this.scrollMore = false;
      // 计时开始
      let seed = 0; // 每10毫秒该值增加1
      let timer = window.setInterval(() => {
        seed++;
        this.timerTxt = seed / 100;
      }, 10);
      // 日志检索
      await this.axios
        .post('/v3/datalab/es_query/' + this.resultTable + '/query/', this.esParams, {
          cancelToken: this.source.token,
          responseType: 'arraybuffer',
        })
        .then(response => {
          const buffer = new Buffer(response, 'binary');
          const textdata = buffer.toString();
          const res = JSON.parse(textdata, function (key, value) {
            if (/^-?\d+$/.test(value)) {
              if (/^-?\d{1,15}[0-8]?$/.test(value)) {
                return value;
              } else {
                return `${value}`;
              }
            } else {
              return value;
            }
          });
          if (res.result) {
            this.resultList.time_taken = res.data.time_taken;
            this.resultList.total = res.data.total;
            this.resultList = res.data;
            this.esParams.totalPage = res.data.total;
          } else {
            this.resultList.list = [];
            this.resultList.select_fields_order = [];
            this.searchFailed = true;
            this.failedInfo = res.message;
            this.failedError = res.errors;
            postMethodWarning(res.message, 'error');
          }
        })
        ['catch'](thrown => {
          if (this.Axios.origin.isCancel(thrown)) {
            console.log('Search request canceled', thrown.message);
          }
        })
        ['finally'](() => {
          this.getHistoryList();
          this.stopBtn = false;
          window.clearInterval(timer); // 计时结束
          this.searchLoading = false;
          this.resultLoading = false;
        });
    },
    init() {
      if (this.date.date.length === 0) {
        this.getTheDate();
      }
      this.topShow = false;
    },
  },
};
</script>
<style lang="scss">
.retrieval {
  &.noselect {
    user-select: none;
  }
  .blue-font {
    color: #3a84ff;
  }
  .result {
    width: 100%;
    padding: 17px 25px 20px;
    transition: transform 0.5s;

    .bk-tab-section {
      padding: 0 !important;
    }
    .title {
      border-left: none;
      color: #212232;
      font-size: 16px;
      padding-left: 17px;
      line-height: 26px;
      position: relative;
      background: linear-gradient(to right, #fafafa, white);
      &:before {
        content: '';
        width: 4px;
        height: 26px;
        background: #3a84ff;
        position: absolute;
        left: 0;
        top: 0px;
      }
    }
    .titel-wraper {
      margin: 0;
      span {
        background: #fafafa;
      }
    }
    /*max-width: 1480px;*/
    .time-choice {
      margin: 10px 15px;
      width: 660px;
      display: inline-block;
      text-align: left;
      .time {
        margin-right: 25px;
        cursor: pointer;
        &:hover {
          color: #3a84ff;
        }
      }
      .chart-active {
        color: #3a84ff;
      }
      .time-tip {
        margin-right: 25px;
      }
      .el-input__inner {
        border: none;
        color: #737987;
      }
      .el-date-editor .el-range-input {
        color: #3a84ff;
      }
    }
    .searchbox {
      max-width: 1200px;
      min-height: 155px;
      /*margin: 0 auto;
                text-align: center;*/
      position: relative;
      .search-box {
        display: flex;
        align-items: center;
      }
      .grammar {
        margin-left: 25px;
        color: #3a84ff;
        line-height: 32px;
        display: inline-block;
        margin-top: 9px;
        .bk-icon {
          color: #c3cdd7;
          margin-right: 5px;
        }
      }
      .search-input {
        flex: 1;
        font-size: 16px;
        margin: 0 15px;
        max-width: 510px;
        height: 42px;
        border-radius: 2px 0 0 2px;
        border: none;
        line-height: 42px;
        vertical-align: top;
        border-right: none;
        outline: none;
        color: #212232;
        &::-webkit-input-placeholder {
          color: #c3cdd7;
        }
        &:focus {
          border-color: #3a84ff;
        }
      }
      button {
        width: 120px;
        height: 30px;
        top: 1px;
        border: none;
        color: #fff;
        font-size: 16px;
        border-radius: 0 2px 2px 0;
        line-height: 30px;
      }
      .number {
        min-height: 17px;
        font-size: 12px;
        color: #ffb400;
      }
      .time-cost {
        height: 32px;
        line-height: 32px;
        /*margin-left: 10px;*/
        font-size: 14px;
        color: #737987;
        text-align: left;
        // width: 660px;
        display: inline-block;
        strong {
          font-weight: bold;
          font-size: 15px;
        }
        .note {
          font-size: 16px;
          vertical-align: middle;
          cursor: pointer;
          margin-left: 6px;
          color: #212232;
        }
      }
      .failed {
        color: #fe621d;
        word-break: break-all;
      }
    }
    .display-option {
      float: right;
      width: 235px;
      font-size: 14px;
      .display-options {
        width: 180px;
        float: right;
      }
    }
    .no-data {
      position: absolute;
      left: 50%;
      top: 50%;
      transform: translate(-50%, -50%);
      text-align: center;
      padding: 0;
    }

    .chart-wrapper {
      background: white;
      padding-top: 20px;
      border-bottom: none;
      min-height: 286px;
      .chart-menu {
        padding-left: 40px;
        padding-right: 15px;
        text-align: center;
        height: 25px;
        position: relative;
        .title-time {
          line-height: 25px;
          color: #333333;
        }
        ul {
          font-size: 0px;
          position: absolute;
          right: 15px;
          top: 0px;
        }
        li {
          background: #fff;
          font-size: 12px;
          border: 1px solid #ddd;
          padding: 3px 12px;
          margin-left: -1px;
          color: #999;
          cursor: pointer;
          &:hover {
            background: #f5f5f5;
          }
        }
        .chart-active {
          background: #f5f5f5;
          cursor: default;
        }
      }
      .bk-loading {
        z-index: 4;
      }
    }
  }
  .esTab {
    .estab-panel {
      ::v-deep .bk-tab2-head {
        height: 42px;
        .tab2-nav-item {
          height: 42px !important;
          line-height: 42px !important;
          border-right: 1px solid #ccc;
        }
      }
    }
  }

  .no-data {
    display: block;
    text-align: center;
    padding: 65px 0;
    p {
      color: #cfd3dd;
    }
  }
}

#bkdata_dataquery_slider {
  &.grammar-rules {
    top: 60px;
    left: inherit;
    width: 710px;
    background: none;
    z-index: 5;
    .bk-sideslider-wrapper {
      box-shadow: -3px 0 12px 0px rgba(33, 34, 50, 0.4);
      overflow-y: inherit;
      padding: 0;
    }
    .bk-sideslider-closer {
      background: #3a84ff;
      width: 26px;
      height: 48px;
      line-height: 48px;
      position: absolute;
      left: -26px;
    }
    .bk-sideslider-title {
      padding: 0px 0px 0px 35px !important;
      font-weight: normal;
      border: none;
      color: #212232;
    }
    .bk-sideslider-content {
      height: calc(100vh - 108px);
    }
    .bk-sideslider-header {
      height: auto;
      .bk-sideslider-title {
        display: none;
      }
    }
    .rules-content {
      height: 100%;
      overflow: hidden;
      .rules {
        border: none;
        // margin: -55px 0 0;
        height: calc(100% + 178px);
      }
      .close {
        width: 26px;
        height: 48px;
        line-height: 48px;
        text-align: center;
        position: absolute;
        background: #3a84ff;
        font-size: 12px;
        color: #fff;
        top: 0px;
        left: -26px;
      }
      .title {
        font-size: 16px;
        font-weight: bold;
        color: #212232;
        line-height: 42px;
      }
    }
  }
}
</style>
