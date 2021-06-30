

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
  <div v-bkloading="{ isLoading: loading }"
    :class="{ noselect: isScroll }"
    class="retrieval">
    <div class="result"
      style="padding: 17px 10px 20px">
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
        <div class="time-choice"
          style="margin: 10px 0">
          <span class="time-tip">{{ $t('时间') }} :</span>
          <span :class="{ 'chart-active': timeRound == 'day' }"
            class="time"
            @click="weekTime">
            {{ $t('最近7天') }}
          </span>
          <span :class="{ 'chart-active': timeRound == 'week' }"
            class="time"
            @click="monthTime">
            {{ $t('最近30天') }}
          </span>
          <bkdata-date-picker
            v-show="!nodeSearch"
            v-model="date.date"
            :placeholder="$t('请选择')"
            :type="'datetimerange'"
            format="yyyy-MM-dd HH:mm"
            @change="dateChange" />
        </div>
        <div class="search-box">
          <bkdata-input
            v-model="esParams.keyword"
            :placeholder="$t('请输入需要检索的关键字')"
            autofocus
            clearable
            class="search-input ml0"
            type="text"
            @enter="search" />
          <button
            :class="{ 'is-loading': searchLoading }"
            class="bk-button bk-primary bk-button-large"
            type="button"
            @click="search">
            <span>{{ btnText }}</span>
          </button>
          <a v-show="!nodeSearch"
            class="grammar"
            href="javascript:;"
            @click="sideslider.show = true">
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
        <bkdata-tab-panel v-if="charShow || nodeSearch"
          :label="$t('查询结果')"
          name="searchInfo">
          <div v-bkloading="{ isLoading: chartLoading }"
            class="chart-wrapper">
            <div class="chart-menu">
              <span class="f14 title-time">
                {{ searchParams.start_time }} - {{ searchParams.end_time }}
              </span>
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
                v-show="!noCharData && !chartLoading"
                :chartConfig="queryChartConfig"
                :chartData="queryChartData"
                :chartLayout="queryChartLayout" />
            </div>

            <div v-show="noCharData"
              class="no-data">
              <img alt
                src="../../common/images/no-data.png">
              <p v-if="!nodeSearch">
                {{ $t('暂无数据_请查看') }}
                <a class="blue-font"
                  href="javascript:;"
                  @click="sideslider.show = true">
                  {{ $t('检索规则') }}
                </a>
              </p>
              <p v-else>
                {{ $t('暂无数据') }}
              </p>
            </div>
          </div>
          <div v-if="resultList.list.length !== 0"
            class="result-table esTable">
            <div v-show="headerFixed"
              class="fixbg" />
            <div v-show="scrollShow"
              :class="{ motionless: headerFixed }"
              class="es-scroll">
              <div class="drag-bar" />
            </div>
            <div class="table-box">
              <div :class="{ motionless: headerFixed, noSroll: !scrollShow }"
                class="table-head clearfix">
                <ul v-if="resultList.list.length > 0"
                  class="table-ehead clearfix">
                  <li>{{ $t('序号') }}</li>
                  <li v-for="(item, index) in resultList.select_fields_order"
                    :key="index"
                    :title="item.description">
                    {{ item.value }}
                  </li>
                </ul>
              </div>
              <table v-if="resultList.list.length > 0"
                class="bk-table table">
                <thead>
                  <tr class="active">
                    <th width="64">
                      {{ $t('序号') }}
                    </th>
                    <th v-for="(item, ix) in resultList.select_fields_order"
                      :key="ix">
                      {{ item.value }}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(item, index) in resultList.list"
                    :key="index">
                    <td>{{ index + 1 }}</td>
                    <!--eslint-disable vue/no-v-html-->
                    <td v-for="(list, lx) in resultList.select_fields_order"
                      :key="lx"
                      v-html="item[list.value]" />
                  </tr>
                </tbody>
              </table>
              <div v-if="resultList.list.length === 0 && !moreLoading"
                class="no-data">
                <img alt
                  src="../../common/images/no-data.png">
                <p>{{ $t('暂无数据') }}</p>
              </div>
              <!--<div class="moreLoading" v-show="moreLoading">-->
              <div v-show="moreLoading"
                class="moreLoading">
                <p>
                  <i class="bk-icon icon-circle-2-1" />
                  {{ $t('加载更多中') }}
                </p>
              </div>
              <div v-show="esParams.page === esParams.totalPage && !moreLoading"
                class="moreLoading">
                <p>{{ $t('没有更多了') }}</p>
              </div>
            </div>
          </div>
        </bkdata-tab-panel>
      </bkdata-tab>
      <div v-show="topShow"
        class="back-top"
        @click="backTop">
        <i class="bk-icon icon-back-top" />
        <span class="tips">{{ $t('返回顶部') }}</span>
      </div>
    </div>
    <bkdata-sideslider
      :class="'grammar-rules'"
      :isShow.sync="sideslider.show"
      :title="sideslider.title"
      :width="sideslider.width">
      <div slot="content"
        class="rules-content">
        <div class="close"
          @click="sideslider.show = !sideslider.show">
          <i class="bk-icon icon-close" />
        </div>
        <iframe :src="textsearch"
          class="rules"
          height="100%"
          title="iframe example 1"
          width="100%" />
      </div>
    </bkdata-sideslider>
  </div>
</template>

<script>
import moment from 'moment';
import { postMethodWarning } from '@/common/js/util.js';
import $ from 'jquery';
import PlotlyChart from '@/components/plotly';
import axios from 'axios';
import mixin from '@/common/js/queryMixin.js';

export default {
  components: {
    PlotlyChart,
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
      stopBtn: false,
      source: null,
      isScroll: false,
      textsearch: this.$store.getters['docs/getPaths'].esSearchRule,
      scrollTop: false,
      headerFixed: false,
      scrollShow: false, // 滚动条显示
      searchFailed: false,
      failedInfo: '',
      failedError: null,
      topShow: false, // 回到顶部按钮显示
      sideslider: {
        // 侧栏弹窗
        show: false,
        width: 710,
        title: '1',
      },
      searchLoading: false,
      resultTableLoading: true,
      noCharData: false,
      historyLoading: false,
      searchTime: false,
      select: {
        // 显示项下拉
        optionsList: [],
        selected: [],
        multiSelect: true,
      },
      historyPage: {
        storage_type: 'es',
        page: 1,
        totalPage: 1,
        page_size: 10,
      },
      charShow: false,
      date: {
        // 日期选择
        date: [],
      },
      moreLoading: false,
      timeRound: 'hour',
      searchParams: {
        start_time: '2018-02-01 11:11',
        end_time: '2018-02-02 11:11',
      },
      esParams: {
        start_time: '',
        end_time: '',
        page: 1,
        page_size: 30,
        keyword: '',
      },
      resultList: {
        // 查询结果列表
        time_taken: 0,
        total: 0,
        list: [],
      },
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
        width: 520,
        margin: {
          l: 60,
          r: 90,
          b: 100,
          t: 20,
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
  },
  watch: {
    'resultList.list': function (newVal, oldVal) {
      this.resultList.list = newVal;
    },
    'esParams.page': function (newVal, oldVal) {
      if (this.resultList.list.length !== 0) {
        this.esParams.page = newVal;
        if (newVal > oldVal) {
          this.inquire();
        }
      }
    },
    resultTable: function (newVal, oldVal) {
      this.searchFailed = false;
      this.esParams.page = 1;
      this.noCharData = true;
      this.scrollShow = false;
      this.nodeSearch
        ? document.querySelectorAll('.esTab .bk-tab-label-item')[1].click()
        : document.querySelectorAll('.esTab .bk-tab-label-item')[0].click();
      this.charShow = false; // 隐藏检索结果
      this.resultList.list.length = []; // 上一次清除数据
    },
  },
  created() {
    // this.init()
    localStorage.setItem(':sidebar', 'false');
  },
  methods: {
    showTips(id, content) {
      this.toolTip(id, content, 'right', false, false);
    },
    removeTip() {
      $('.def-tooltip').remove();
    },
    tabChanged(name) {
      this.tabname = name;
      if (name === 'searchInfo') {
        // this.myScroll()
        // this.changeChart(this.timeRound)
        // if (this.resultTable) {
        //     this.inquire()
        // }
      }
    },
    optionsChange() {
      console.log(this.select.selected);
    },
    // 時間選擇
    dateChange() {
      if (this.date.date !== null) {
        let h1, min1, s1;
        if (this.date.date[0].getHours() > 9) {
          h1 = this.date.date[0].getHours();
        } else {
          h1 = '0' + this.date.date[0].getHours();
        }
        if (this.date.date[0].getMinutes() > 9) {
          min1 = this.date.date[0].getMinutes();
        } else {
          min1 = '0' + this.date.date[0].getMinutes();
        }
        if (this.date.date[0].getSeconds() > 9) {
          s1 = this.date.date[0].getSeconds();
        } else {
          s1 = '0' + this.date.date[0].getSeconds();
        }
        this.esParams.start_time =          this.date.date[0].getFullYear()
          + '-'
          + (this.date.date[0].getMonth() + 1)
          + '-'
          + this.date.date[0].getDate()
          + ' '
          + h1
          + ':'
          + min1;
        this.esParams.end_time =          this.date.date[1].getFullYear()
          + '-'
          + (this.date.date[1].getMonth() + 1)
          + '-'
          + this.date.date[1].getDate()
          + ' '
          + h1
          + ':'
          + min1;
      }
    },
    getTheDate() {
      let nowDate = new Date();
      let yesterday = new Date(nowDate - 24 * 3600 * 1000);
      this.date.date = [yesterday, nowDate];
      this.dateChange();
    },
    weekTime() {
      this.timeRound = 'day';
      let nowDate = new Date();
      let weekDate = new Date(nowDate - 7 * 24 * 3600 * 1000);
      this.date.date = [weekDate, nowDate];
      this.dateChange();
    },
    monthTime() {
      this.timeRound = 'week';
      let nowDate = new Date();
      let monthDate = new Date(nowDate - 30 * 24 * 3600 * 1000);
      this.date.date = [monthDate, nowDate];
      this.dateChange();
    },
    // 点击再次查询
    checkAgain(item) {
      this.esParams.keyword = item.keyword;
      this.date.date = [new Date(item.search_range_start_time), new Date(item.search_range_end_time)];
      this.dateChange();
      this.search();
    },
    // 滚动加载
    myScroll() {
      let self = this;
      this.$nextTick(() => {
        let dom = this.nodeSearch
          ? document.querySelectorAll('.esNode .bk-tab-label-item')[0]
          : document.getElementsByClassName('bk-tab-label-item')[0];
        if (this.scrollTop) {
          dom.scrollTo({ top: '350px' }, 155);
          self.scrollTop = !self.scrollTop;
        } else {
          dom.scrollTo({ top: '0' }, 0);
          self.headerFixed = false;
          $('.esTable .motionless').css('left', '');
          if (document.querySelectorAll('.result-table .table-head').length > 0) {
            document.querySelectorAll('.result-table .table-head')[0].style.height = 0 + 'px';
          }
        }
        dom.onscroll = function (e) {
          let scrollTop = dom.scrollTop; // 滚动高度
          if (document.getElementsByClassName('retrieval').length > 0 && self.tabname === 'searchInfo') {
            let cutHeight =              document.getElementsByClassName('header')[0].clientHeight
              + document.getElementsByClassName('bk-tab2-head')[0].clientHeight
              + 1; // 1px边框
            let winHeight = document.body.clientHeight - cutHeight; // 可视高度
            let pageHeight = document.getElementsByClassName('retrieval')[0].clientHeight; // 页面高度
            if (scrollTop > 300) {
              self.topShow = true;
            } else {
              self.topShow = false;
            }
            if (scrollTop >= pageHeight - winHeight && self.esParams.page < self.esParams.totalPage) {
              self.esParams.page = self.esParams.page + 1;
              // self.inquire()
            }
          }
          if (scrollTop > 500) {
            self.headerFixed = true;
            self.analogScrollAdjust();
            if (document.querySelectorAll('.table-ehead').length === 0) return;
            let pseudo = document.querySelectorAll('.table-ehead')[0];
            document.querySelectorAll('.result-table .table-head')[0].style.height = pseudo.offsetHeight
                          + 'px';
          } else {
            self.headerFixed = false;
            if (document.querySelectorAll('.result-table .table-head').length === 0) return;
            document.querySelectorAll('.result-table .table-head')[0].style.height = 0 + 'px';
            $('.esTable .motionless').css('left', '');
          }
        };
      });
    },
    // 模拟滚动条自适应
    analogScrollAdjust() {
      if ($('.motionless').length > 0) {
        let that = this;
        let initLeft = $('.esTab').offset().left;
        document.body.onscroll = function () {
          that.reDrawScroll(initLeft);
        };
        that.reDrawScroll(initLeft);
      }
    },
    reDrawScroll(initLeft) {
      let x = document.getElementsByTagName('html')[0].scrollLeft;
      if (this.nodeSearch) return;
      $('.esTable .motionless').css('left', initLeft - x + 1 + 'px');
      $('.Masking').css('left', initLeft - x - 26 + 'px');
      $('.esTable .fixbg').css('left', initLeft - x + 'px');
    },
    // 模拟滚动条
    analogScroll() {
      const self = this;
      const scroll = document.querySelectorAll('.es-scroll')[0]; // 横向滚动条的容器
      const scrollBar = document.querySelectorAll('.es-scroll .drag-bar')[0]; // 横向滚动条的滑块
      const dragWidth = document.querySelectorAll('.result-table .table-box')[0]; // 查询表格的容器
      const dragTable = document.querySelectorAll('.result-table .table-box .table')[0]; // 查询表格
      const tableHead = document.querySelectorAll('.table-ehead')[0]; // 表格头
      if (dragTable && tableHead && scrollBar) {
        dragTable.style.left = 0 + 'px';
        tableHead.style.left = 0 + 'px';
        scrollBar.style.left = 0 + 'px';
      }
      if (!dragTable || dragWidth.offsetWidth === 0) return;
      if (dragTable.offsetWidth <= dragWidth.offsetWidth) {
        this.scrollShow = false;
      } else {
        this.scrollShow = true;
      }
      let scale = dragWidth.offsetWidth / dragTable.offsetWidth; // 获取比例
      scroll.style.width = dragWidth.offsetWidth + 1 + 'px';
      scrollBar.style.width = dragWidth.offsetWidth * scale + 'px'; // 滑块自适应
      let iMaxWidth;
      this.$nextTick(() => {
        iMaxWidth = scroll.offsetWidth - scrollBar.offsetWidth;
      });
      let disX;
      scrollBar.addEventListener(
        'mousedown',
        function (ev) {
          self.isScroll = true;
          ev = ev || event;
          disX = ev.clientX - this.offsetLeft;
          return false;
        },
        false
      );
      if (document.onmousemove !== null) {
        document.onmousemove = null;
      }
      document.onmousemove = function (e) {
        if (self.isScroll) {
          e = e || event;
          let T = e.clientX - disX;
          if (T < 0) {
            T = 0;
          } else if (T > iMaxWidth) {
            T = iMaxWidth;
          }
          let iScale = T / iMaxWidth;
          dragTable.style.left = (dragWidth.offsetWidth - dragTable.offsetWidth) * iScale + 'px';
          tableHead.style.left = (dragWidth.offsetWidth - dragTable.offsetWidth) * iScale + 'px';
          scrollBar.style.left = T + 'px';
        }
      };
      if (document.onmouseup !== null) {
        document.onmouseup = null;
      }
      document.onmouseup = function () {
        self.isScroll = false;
      };
      scrollBar.addEventListener(
        'mouseleave',
        function (ev) {
          document.onmousedown = null;
        },
        false
      );
      // 表格伪头部
      let pseudo = document.querySelectorAll('.table-ehead')[0];
      document.querySelectorAll('.result-table .table-head')[0].style.width = dragWidth.offsetWidth + 'px';
      pseudo.style.width = dragTable.offsetWidth + 'px';
      let arr = pseudo.getElementsByTagName('li');
      for (let i = 0; i < arr.length; i++) {
        arr[i].style.width = dragTable.getElementsByTagName('th')[i].offsetWidth + 'px';
      }
    },
    // 回到顶部
    backTop() {
      if (document.getElementsByClassName('bk-tab-label-item').length === 0) return;
      let dom = this.nodeSearch
        ? document.querySelectorAll('.esNode .bk-tab-label-item')[0]
        : document.getElementsByClassName('bk-tab-label-item')[0];
      dom.scrollTop = 0;
    },
    // 获取图表数据
    changeChart(interval = 'hour') {
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
        .post('result_tables/' + this.resultTable + '/get_es_chart/', chartParm, {
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
      // document.querySelectorAll('.retrieval .tab2-nav-item')[1].click()
      this.esParams.page = 1;
      this.charShow = true;
      this.inquire();
      this.changeChart();
    },
    async inquire() {
      this.moreLoading = true;
      if (!this.scrollMore || this.searchLoading) return;
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
        .post('result_tables/' + this.resultTable + '/query_es/', this.esParams, {
          cancelToken: this.source.token,
        })
        .then(res => {
          if (res.result) {
            this.resultList.time_taken = res.data.time_taken;
            if (this.esParams.page === 1) {
              this.resultList.total = res.data.total;
              // 加中文字段
              res.data.select_fields_order.forEach((value, i) => {
                res.data.select_fields_order[i] = {
                  value: value,
                };
              });
              this.resultList.select_fields_order = res.data.select_fields_order;
              this.resultList.list = res.data.list;
              let totalPage = Math.ceil(res.data.total / this.esParams.page_size);
              this.resultList.list = res.data.list.slice(0, this.esParams.page_size);
              // 总页数不能为0
              this.esParams.totalPage = totalPage !== 0 ? totalPage : 1;
              // 下拉选择
              this.select.selected = this.resultList.select_fields_order;
              // 显示项下拉
              this.select.optionsList = [];
              for (let i = 0; i < this.resultList.select_fields_order.length; i++) {
                this.select.optionsList.push({
                  id: this.resultList.select_fields_order[i],
                  name: this.resultList.select_fields_order[i],
                });
              }
              this.scrollTop = true;
              this.myScroll();
            } else {
              res.data.select_fields_order.forEach((value, i) => {
                res.data.select_fields_order[i] = {
                  value: value,
                };
              });
              this.resultList.select_fields_order = res.data.select_fields_order;
              this.resultList.list = this.resultList.list.concat(res.data.list);
            }
            this.$nextTick(() => {
              this.analogScroll();
            });
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
          this.moreLoading = false;
          this.stopBtn = false;
          window.clearInterval(timer); // 计时结束
          this.scrollMore = true;
          this.searchLoading = false;
        });
    },
    init() {
      if (this.date.date.length === 0) {
        this.getTheDate();
      }
      this.topShow = false;
      this.myScroll();
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
    transition: transform 0.5s;
    .title {
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
        line-height: 42px;
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
    .history-table {
      /* width: 100%;
                display: inline-block;
                margin-top: 20px;
                text-align: left;
                position: relative;*/
      min-height: 200px;
      .page {
        padding: 20px;
        text-align: right;
      }
      .bk-table {
        margin-top: 20px;
        border-top: 1px solid #ddd;
        table-layout: fixed;
      }
      thead {
        background: #f5f5f5;
        th {
          padding: 10px 20px;
          border-right: 1px solid #ddd;
          color: #212232;
          font-weight: normal;
          &:last-of-type {
            border: none;
          }
        }
      }
      tr {
        border-bottom: 1px solid #ddd;
        &:hover {
          background: #e8e9ec;
        }
      }
      tbody {
        tr:last-of-type {
          border-bottom: none;
        }
      }
      /*tbody tr:last-of-type{
                    border-bottom: none;
                }*/
      td {
        padding: 10px 20px;
        position: relative;
        color: #212232;
        &:hover {
          .tips {
            display: block;
          }
        }
        .check-again {
          color: #3a84ff;
        }
        .beyond {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          max-width: 100%;
          display: inline-block;
        }
        .no-keyword {
          color: #c3cdd7;
        }
        .tips {
          display: none;
          /*width: 95%;*/
          position: absolute;
          padding: 10px;
          border-radius: 2px;
          background: #212232;
          color: #fff;
          left: 50%;
          bottom: 40px;
          font-size: 12px;
          transform: translate(-50%, 0);
          word-break: break-all;
          &:after {
            position: absolute;
            left: 50%;
            bottom: -6px;
            content: '';
            width: 0;
            height: 0px;
            border: 3px solid #fff;
            border-color: #212232 transparent transparent transparent;
          }
        }
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
    .result-table {
      position: relative;
      background-color: #fff;
      border-top: 1px solid #ddd;
      display: table;
      table-layout: fixed;
      width: 100%;
      padding-top: 10px;
      overflow-x: hidden;
      table {
        border-left: none;
        border-right: none;
        border-top: none;
        position: relative;
        left: 0;
        top: 0;
        min-width: 100%;
      }
      .table-header {
        padding: 15px 15px 15px 40px;
        line-height: 36px;
        font-size: 20px;
      }
      .active {
        background: #f5f5f5;
      }
      thead {
        opacity: 0;
      }
      thead tr {
        height: 40px;
        text-align: left;
        th {
          border-left: 1px solid #ddd;
        }
      }
      tbody tr {
        height: 60px;
        padding: 10px;
        &:hover {
          background: #e8e9ec;
        }
      }
      tr {
        border-bottom: 1px solid #ddd;
        em {
          color: #000034;
          background: #ff0;
          font-style: normal;
        }
      }
      td,
      th {
        min-width: 100px;
        padding: 10px 20px;
        display: table-cell;
        color: #212232;
        &:first-of-type {
          border-left: none;
          min-width: 70px;
        }
      }
      .table-box {
        position: relative;
        min-height: 200px;
        margin-top: -2px;
        overflow: hidden;
      }
      /*滚动条*/
      .es-scroll {
        width: 100%;
        height: 7px;
        position: absolute;
        left: -2px;
        top: 0px;
        background: #fafafa;
        z-index: 2;
        .drag-bar {
          width: 200px;
          height: 11px;
          position: absolute;
          left: 0;
          top: -2px;
          background: #828793;
          border-radius: 5px;
          cursor: pointer;
          &:before {
            content: '';
            position: absolute;
            left: 3px;
            top: 2px;
            width: 0;
            height: 0;
            border: 3px solid #c7c9cf;
            border-color: transparent #c7c9cf transparent transparent;
          }
          &:after {
            content: '';
            position: absolute;
            right: 3px;
            top: 2px;
            width: 0;
            height: 0;
            border: 3px solid #c7c9cf;
            border-color: transparent transparent transparent #c7c9cf;
          }
        }
        &.motionless {
          position: fixed;
          top: 138px;
          left: 326px;
          border-top: 1px solid #ddd;
          border-right: 1px solid #ddd;
        }
      }
      .table-head {
        width: 100%;
        &.motionless {
          height: 53px;
          position: fixed;
          top: 145px;
          left: 325px;
          z-index: 1;
          background: #fff;
          border-bottom: 1px solid #ddd;
          overflow: hidden;
          ul {
            top: 2px;
          }
          &.noSroll {
            top: 135px;
          }
        }
      }
      .fixbg {
        width: calc(100% - 367px);
        height: 30px;
        position: fixed;
        background: #fff;
        top: 108px;
        left: 325px;
        z-index: 1;
        min-width: 885px;
      }
      .table-ehead {
        position: absolute;
        top: 0;
        left: 0;
        z-index: 1;
        background-color: #fafafa;
        border-top: 1px solid #ddd;
        border-bottom: 1px solid #ddd;
        display: flex;
        li {
          align-items: stretch;
          float: left;
          line-height: 20px;
          padding: 10px 20px;
          color: #212232;
          font-size: 14px;
          background-color: #f5f5f5;
          border-right: 1px solid #ddd;
          &:last-of-type {
            border-right: none;
          }
        }
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
    .moreLoading {
      line-height: 44px;
      text-align: center;
      background: #fafafa;
      text-align: center;
      p {
        display: inline-block;
        position: relative;
        padding: 20px;
      }
      .bk-icon {
        position: absolute;
        left: -20px;
        top: 35px;
        animation: rotate 0.5s infinite linear;
      }
    }
    @keyframes rotate {
      0% {
        transform: rotate(0deg);
        -webkit-transform: rotate(0deg);
        -moz-transform: rotate(0deg);
      }
      100% {
        transform: rotate(360deg);
        -webkit-transform: rotate(360deg);
        -moz-transform: rotate(360deg);
      }
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
    .back-top {
      position: fixed;
      bottom: 46px;
      right: 30px;
      width: 36px;
      height: 36px;
      line-height: 36px;
      text-align: center;
      background: rgba(238, 238, 238, 0.48);
      box-shadow: -4px 0px 6px 0px rgba(27, 28, 17, 0.03);
      border-radius: 50%;
      border: solid 1px rgba(221, 221, 221, 0.48);
      opacity: 0.8;
      font-size: 16px;
      cursor: pointer;
      &:hover {
        background: #3a84ff;
        border-color: #3a84ff;
        opacity: 1;
        color: #fff;
        .tips {
          display: block;
        }
      }
      .tips {
        display: none;
        position: absolute;
        top: 50%;
        right: 50px;
        padding: 8px 12px;
        background: #212232;
        border-radius: 2px;
        font-size: 12px;
        color: #fff;
        line-height: 12px;
        min-width: 80px;
        transform: translate(0, -50%);
        &:after {
          position: absolute;
          right: -6px;
          top: 50%;
          content: '';
          width: 0;
          height: 0px;
          border: 3px solid #fff;
          border-color: transparent transparent transparent #212232;
          transform: translate(0, -50%);
        }
      }
    }
  }
  .esTab {
    .bk-tab2-head {
      height: 42px;
      .tab2-nav-item {
        height: 42px !important;
        line-height: 42px !important;
        border-right: 1px solid #ccc;
      }
    }
  }
  .grammar-rules {
    top: 108px;
    left: inherit;
    width: 710px;
    background: none;
    z-index: 5;
    .bk-sideslider-content {
      height: 100%;
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
        margin: -55px 0 0;
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
  .no-data {
    display: block;
    text-align: center;
    padding: 65px 0;
    p {
      color: #cfd3dd;
    }
  }
}
</style>
