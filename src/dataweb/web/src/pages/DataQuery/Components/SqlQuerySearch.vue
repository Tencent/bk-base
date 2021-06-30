

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
  <div class="sql-query-search">
    <div class="search-button">
      <!-- 'is-disabled': !this.chineseData.fields 加中文描述-->
      <button
        type="button"
        :disabled="!sqlData"
        class="bk-button bk-primary bk-button-large"
        :title="btnText"
        @click="handleSqlQuerySearch(null)">
        <span>{{ btnText }}</span>
      </button>
      <p class="tips">
        <i18n v-if="!searchLoading && !searchFailed"
          path="找到_条结果_用时_秒"
          tag="span">
          <strong place="num">{{ resultList.total }}</strong>
          <strong place="time">{{ timerTxt }}</strong>
        </i18n>
        <span v-if="queryTips.isShow"
          class="query-tip-text">
          {{ queryTips.text }}
        </span>
        <template v-if="!searchLoading && !searchFailed && runVersion === 'tgdp'">
          <!-- 这个提示只在企业版显示 -->
          <span style="padding: 0 5px">|</span>
          <span class="ml5">
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
        </template>
        <span v-if="searchLoading">{{ $t('耗时') }}（{{ timerTxt }}s）</span>
        <span v-if="!searchLoading && searchFailed"
          class="failed">
          {{ failedInfo }}
        </span>
        <bkdata-popover
          v-if="!searchLoading && searchFailed && failedError && failedError.error"
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
      </p>
    </div>
    <!--查询结果部分-->
    <div id="result-table">
      <div v-bkloading="{ isLoading: searchLoading }">
        <div class="tab-header">
          <span
            v-if="!nodeSearch"
            :class="{ active: activeName === 'query-history' }"
            @click="activeName = 'query-history'">
            {{ $t('查询历史') }}
          </span>
          <span
            v-if="searchShow || nodeSearch"
            :class="{ active: activeName === 'searchResult' }"
            @click="activeName = 'searchResult'">
            {{ $t('查询结果') }}
          </span>
        </div>
        <div class="tab-body">
          <template v-if="activeName === 'query-history'">
            <SqlQuerySearchHistory :queryHistoryList="searchHistory"
              @sqlHistoryQueryAgain="handleQueryAgain" />
          </template>
          <template v-if="activeName === 'searchResult'">
            <div style="min-height: 30vh; padding-bottom: 30px">
              <template v-if="!searchLoading">
                <SqlQuerySearchResult :resultList="resultList"
                  @loadMoreResult="handleHistoryMoreClick" />
              </template>
            </div>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
import moment from 'moment';
import { mapGetters, mapState } from 'vuex';
import { postMethodWarning, readBlobRespToJson } from '@/common/js/util';
import SqlQuerySearchHistory from './SearchHistoryTable';
import SqlQuerySearchResult from './SearchResultTable';
import Bus from '@/common/js/bus';
import axios from 'axios';
import mixin from '@/common/js/queryMixin.js';

export default {
  components: {
    SqlQuerySearchHistory,
    SqlQuerySearchResult,
  },
  mixins: [mixin],
  props: {
    nodeSearch: {
      type: Boolean,
      default: false,
    },
    activeType: {
      type: String,
    },
    sqlData: {
      type: String,
    },
    resultTable: {
      type: String,
    },
  },
  data() {
    return {
      stopBtn: false,
      source: null,
      searchFailed: false,
      moreLoading: false,
      failedInfo: '',
      failedError: null,
      headerFixed: false,
      historyPageing: {
        // 查询历史分页
        page: 1, // 当前页码
        totalPage: 1,
        page_size: 10, // 每页数量
        storage_type: '',
      },
      resultList: {
        // 查询结果列表
        list: [],
      },
      searchHistory: {},
      timerTxt: 0, // 倒计时
      searchShow: false, // 查询历史 tab切换显示项
      activeName: '',
      runVersion: '',
    };
  },
  computed: {
    btnText() {
      return this.stopBtn ? this.$t('停止') : this.$t('查询');
    },
    ...mapState({
      searchLoading: state => state.dataQuery.search.isLoading,
    }),
    noteTime: function () {
      const time = {};
      time.user_time = `${moment().format('YYYY-MM-DD HH:mm:ss')} ${new Date().toString()
        .slice(25)}`;
      time.utc_time = `${moment.utc().format('YYYY-MM-DD HH:mm:ss')} GTM+0000 UTC`;
      return time;
    },
    isTspider() {
      return /tspider/gi.test(this.activeType);
    },
    queryTips() {
      const text = this.isTspider ? 'Tspider' : 'HDFS';
      return {
        isShow: /(tspider|hdfs)/gi.test(this.activeType),
        text: `${text} ${this.$t('queryTip')}`,
      };
    },
  },
  watch: {
    nodeSearch: {
      handler(val) {
        this.activeName = !val ? 'query-history' : 'searchResult';
      },
      immediate: true,
    },
  },
  mounted() {
    this.runVersion = window.BKBASE_Global.runVersion;
    Bus.$on('DataQuery-QueryResult-History', (storageType, sql) => {
      this.historyPageing.storage_type = storageType;
      if (this.historyPageing.page !== 1) {
        this.historyPageing.page = 1;
      }
      // this.$nextTick(() => {
      //     this.handleSqlQuerySearch(sql, storageType)
      // })
    });
    Bus.$on('changeSearchResult', (status, value) => {
      if (status === 'pageChange') {
        // 当前页改变
        this.historyPageing.page = value;
      } else {
        // 当前页可展示数量改变
        this.historyPageing.page = 1;
        this.historyPageing.page_size = value;
      }
      this.getHistoryList(this.resultTable);
    });
  },

  methods: {
    handleQueryAgain(item) {
      this.$emit('update:sqlData', item.sql);
      this.handleSqlQuerySearch(item.sql);
    },
    tabChangetabChange(name) {
      this.activeName = name;
    },
    stopSearch() {
      this.source && this.source.cancel('Operation canceled by the user.');
    },
    handleSqlQuerySearch(sql, storageType) {
      if (this.stopBtn) {
        this.stopBtn = false;
        this.source && this.source.cancel('Operation canceled by the user.');
        return;
      }
      this.source = axios.CancelToken.source();
      if (!this.resultTable) {
        // 没有RT表，先解析RT
        return this.axios
          .post(
            'v3/bksql/api/v1/convert/table-names/',
            {
              sql: this.sqlData,
            },
            { cancelToken: this.source.token }
          )
          .then(res => {
            if (res.result) {
              if (res.data.length !== 0) {
                this.$emit('update:resultTable', res.data[0]);
                this.$nextTick(() => {
                  this.handleSqlQuerySearch(null);
                  Bus.$emit('updateHistoryList', res.data[0]);
                });
              } else {
                postMethodWarning(this.$t('sql解析失败，请检查'), 'error');
              }
            } else {
              postMethodWarning(res.message, 'error');
            }
          })
          ['catch'](error => {
            this.handleNetworkError(error);
          });
      }
      this.stopBtn = true;
      this.resultList = {
        list: [],
      };
      this.searchFailed = false;
      this.activeName = 'searchResult';
      // 清空源数据
      if (this.searchLoading) return;
      this.$store.commit('dataQuery/setSearchLoading', true);
      this.searchShow = true;
      // 计时开始
      let seed = 0; // 每10毫秒该值增加1
      let timer = window.setInterval(() => {
        seed++;
        this.timerTxt = seed / 100;
      }, 10);
      const currentActiveType = this.activeType ? this.activeType : this.nodeType;
      let params = {
        storage_type: storageType || currentActiveType,
        sql: sql || this.sqlData,
      };
      // 查询结果拉取
      return this.axios
        .post('result_tables/' + this.resultTable + '/query_rt/', params, {
          cancelToken: this.source.token,
          responseType: 'blob',
        })
        .then(response => {
          readBlobRespToJson(response).then(res => {
            if (res.result) {
              this.isShow = true;
              this.resultList = res.data;
              this.resultList.list = res.data.list.map(item => res.data.select_fields_order
                .reduce((pre, current) => {
                  const key = typeof current === 'string' ? current : current.value;
                  if (typeof pre[key] === 'string') {
                    const originLen = pre[key].length;
                    const targetLen = pre[key].replace(/^\s+|\s+$/, '').length;
                    const replaceChar = new Array(originLen - targetLen).fill('&nbsp;')
                      .join('');
                    return Object.assign(pre, {
                      [key]: pre[key].replace(/^\s+|\s+$/, replaceChar),
                    });
                  } else {
                    return pre;
                  }
                }, item)
              );
            } else {
              this.failedInfo = res.message;
              this.failedError = res.errors;
              this.searchFailed = true;
            }
          });
        })
        ['catch']((thrown, status) => {
          this.handleNetworkError(thrown);
          if (this.Axios.origin.isCancel(thrown)) {
            console.log('Search request canceled', thrown.message);
          }
        })
        ['finally'](() => {
          this.stopBtn = false;
          this.activeName = 'searchResult';
          this.$store.commit('dataQuery/setSearchLoading', false);
          window.clearInterval(timer); // 计时结束
          this.getHistoryList(this.resultTable, params.storage_type);
        });
    },

    handleNetworkError(thrown) {
      if (/Network Error/gi.test(thrown)) {
        this.failedInfo = thrown;
        this.failedError = thrown;
        this.searchFailed = true;
      }
    },

    /** 获取查询历史数据 */
    getHistoryList(id, storageType) {
      if (!id) {
        return;
      }
      this.$store.commit('dataQuery/setSearchHistoryLoading', true);
      this.historyPageing.storage_type = this.historyPageing.storage_type || storageType;
      this.axios
        .get('result_tables/' + id + '/list_query_history/', {
          params: this.historyPageing,
        })
        .then(res => {
          if (res.result) {
            this.searchHistory = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.$store.commit('dataQuery/setSearchHistoryLoading', false);
        });
    },

    handleHistoryMoreClick() {
      if (this.searchLoading) return;
      if (this.resultPaging.page < this.resultPaging.totalPage) {
        this.resultPaging.page = this.resultPaging.page + 1;
      } else {
        this.noMore = true;
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.sql-query-search {
  .search-button {
    margin-top: 10px;
    .bk-button {
      width: 120px;
      font-size: 16px;
    }
    .tips {
      color: #737987;
      margin-left: 20px;
      line-height: 42px;
      display: inline-block;
      .query-tip-text {
        position: relative;
        padding-left: 10px;
        &::before {
          position: absolute;
          left: 2px;
          top: 3px;
          display: inline-block;
          content: '';
          height: 14px;
          line-height: 19px;
          width: 5px;
          border-left: 1px solid #dcdee5;
        }
      }
      strong {
        font-weight: bold;
        font-size: 15px;
      }
      .failed {
        color: #fe621d;
      }
      .note {
        font-size: 16px;
        vertical-align: middle;
        margin-left: 6px;
        color: #212232;
        cursor: pointer;
        display: flex;
      }
    }
  }
  .king-instruction {
    border-radius: 2px;
    margin-bottom: 20px;
  }

  #result-table {
    margin: 10px 0;

    .tab-header {
      height: 38px;
      background: #fafbfd;
      border-left: solid 1px #ddd;
      border-bottom: none;
      border-right: none;
      display: flex;
      justify-content: flex-start;
      align-items: center;

      span {
        height: 38px;
        line-height: 38px;
        padding: 0 20px;
        cursor: pointer;

        &.active {
          color: #3a84ff;
          background: #fff;
          border-right: solid 1px #ddd;
          border-left: solid 1px #ddd;
          border-top: none;
        }

        &:first-child {
          &.active {
            border-left: none;
          }
        }
      }
    }

    // .tab-body {
    //     margin-top: 15px;
    // }

    .seach-box {
      width: 270px;
      padding: 17px 0 17px 20px;
    }
    .chart {
      height: 500px;
      .chart-content {
        padding: 20px 20px 0;
      }
    }
    .chart-config {
      > div {
        display: flex;
        > div {
          display: flex;
          .title-text {
            width: 90px;
            height: 36px;
            line-height: 36px;
            text-align: right;
            margin-right: 15px;
          }
        }
        .no-data {
          display: block;
        }
      }
      .select-box {
        margin-bottom: 20px;
      }
      #chart-content {
        padding: 0px;
      }
      .no-data {
        display: block;
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
      }
    }
    #chart-content {
      height: 420px;
      width: 100%;
      padding: 0 30px 50px 30px;
      .no-data {
        position: absolute;
        left: 50%;
        top: 30%;
        transform: translate(-50%, 0);
      }
    }
    .table {
      min-height: 60px;
      position: relative;
      // margin: 18px 0 0;
      //   padding-bottom: 20px;
      overflow: hidden;
      tr th:first-of-type,
      tr td:first-of-type {
        border-left: none;
        min-width: 70px;
      }
      tr th:last-of-type,
      tr td:last-of-type {
        border-right: none;
      }
      tr:last-of-type td {
        border-bottom: none;
      }
      &.noScroll {
        margin-top: 8px;
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
    .page {
      text-align: right;
      padding: 20px 28px;
    }
    /*查询历史*/
    .query-history {
      margin: 18px 0;
      margin-bottom: 0;
      border-top: none;
      .check-again {
        color: #3a84ff;
        &.disable {
          color: #888ccb;
        }
      }
      .bk-table {
        border-bottom: none;
      }
    }
    .search-table {
      .bk-table {
        border-top: 1px solid #ddd;
        table-layout: fixed;
        min-width: 750px;
        th + th {
          border-left: 1px solid #ddd;
        }
        td + td {
          border-left: none;
        }
        td {
          position: relative;
          &:hover {
            .tips {
              display: block;
            }
          }
          .tips {
            display: none;
            width: 95%;
            position: absolute;
            padding: 10px;
            border-radius: 2px;
            background: #212232;
            color: #fff;
            left: 50%;
            bottom: 40px;
            font-size: 12px;
            transform: translate(-50%, 0);
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
        .beyond {
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          width: 100%;
        }
        tbody tr:hover {
          background: #e8e9ec;
        }
      }
      .no-keyword {
        color: #c3cdd7;
      }
    }
    /*滚动条*/
    .scroll-bar {
      width: 100%;
      height: 10px;
      position: absolute;
      left: -1px;
      top: 12px;
      background: #fafafa;
      border: 1px solid #ddd;
      .drag-bar {
        width: 200px;
        height: 11px;
        position: absolute;
        left: 0;
        top: -2px;
        background: #828793;
        border-radius: 5px;
        cursor: pointer;
        z-index: 2;
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
        left: 325px;
        z-index: 2;
      }
    }
    .drag-con {
      width: 100%;
      border-top: none;
      position: relative;
      left: 0px;
      top: 20px;
      overflow: hidden;
      .bk-table {
        position: relative;
        left: 0;
        top: 0;
        thead {
          opacity: 0;
        }
        th,
        td {
          min-width: 100px;
        }
        tbody tr:hover {
          background: #e8e9ec;
        }
      }
      .table-shead-con {
        width: 100%;
        &.motionless {
          height: 46px;
          position: fixed;
          top: 145px;
          /* left: 326px; */
          z-index: 1;
          background: #fff;
          overflow: hidden;
          border-bottom: 1px solid #ddd;
          ul {
            top: 2px;
          }
          &.noScroll {
            top: 135px;
          }
        }
      }
      .table-head {
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
          padding: 10px 19px;
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
  }
}
</style>
