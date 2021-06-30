

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
  <div class="clean-Detail">
    <div class="back-list">
      <a class="bk-button bk-primary bk-button-mini"
        title="返回列表"
        @click="cleanList">
        <span>{{ $t('返回列表') }}</span>
      </a>
    </div>
    <DetailStatusOverview
      :data-type="'clean'"
      :cleanClass="cleanClass"
      :canStart="canStart"
      :canStop="canStop"
      :loading="loading"
      :data-info="dataInfo"
      @start="start"
      @stop="showStopDialog = true"
      @edit="edit" />
    <template v-if="$modules.isActive('dataflow')">
      <div class="related-task">
        <div class="title">
          <p class="title-text fl">
            {{ $t('关联任务') }}
          </p>
          <span
            :class="['collapse fr', { active: !relatedTask.isShow }]"
            @click="relatedTask.isShow = !relatedTask.isShow">
            {{ relatedTask.isShow ? $t('收起') : $t('展开') }}
            <i :title="relatedTask.isShow ? $t('收起') : $t('展开')"
              class="bk-icon icon-arrows-up-circle arrows" />
          </span>
        </div>
        <div v-show="relatedTask.isShow">
          <bkdata-table
            v-bkloading="{ isLoading: relatedTask.loading }"
            :data="relatedtableList"
            :emptyText="$t('暂无数据')"
            :pagination="pagination"
            @page-change="handlePageChange"
            @page-limit-change="handlePageLimitChange">
            <bkdata-table-column :label="$t('状态')"
              :sortable="true">
              <div slot-scope="item"
                class="bk-table-inlineblock">
                <span
                  :class="[
                    'sum-status',
                    /started|running/i.test(item.row.status) && !item.row.has_exception
                      ? 'running'
                      : item.row.status === 'started' && item.row.has_exception
                        ? 'exception'
                        : '',
                  ]">
                  {{
                    /started|running/i.test(item.row.status) && !item.row.has_exception
                      ? $t('运行中')
                      : item.row.status === 'started' && item.row.has_exception
                        ? $t('运行异常')
                        : $t('未运行')
                  }}
                </span>
              </div>
            </bkdata-table-column>
            <bkdata-table-column :label="`${$t('项目')}/${$t('任务')}`"
              :sortable="true">
              <div slot-scope="item"
                class="bk-table-inlineblock">
                <p class="project-task">
                  <span class="icon">
                    <i class="bk-icon icon-folder" />
                  </span>
                  <span class="ml5">
                    {{ item.row.project_name
                      ? item.row.project_name : item.row.project_id }}
                  </span>
                  /
                  <span class="ml5">
                    {{ item.row.flow_name
                      ? item.row.flow_name : item.row.flow_id }}
                  </span>
                </p>
              </div>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('创建人')"
              prop="created_by" />
            <bkdata-table-column :label="$t('操作')">
              <div slot-scope="item"
                class="bk-table-inlineblock">
                <span class="checkDeatil"
                  @click="jumpFlow(item.row.flow_id, item.row.project_id)">
                  {{ $t('查看详情') }}
                </span>
              </div>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </div>
    </template>
    <div class="clean-trends">
      <div class="title">
        <p class="title-text">
          {{ $t('清洗数据质量') }}
        </p>
      </div>
      <DataManageChart
        :allOutPut="allOutPut"
        :allInPut="allInPut"
        :isShowCount="true"
        :isLoading="chart.chartLoading"
        :cleanChartInfo="cleanChartInfo"
        @filterChartData="filterChartData"
        @changeActive="changeActive"
        @getChartData="getChartData" />
      <div class="table">
        <div class="table-top clearfix">
          <i18n class="table-top-text fl"
            path="数据丢弃共_条"
            tag="p">
            <span class="text-num"
              place="num">
              {{ trend.tableList.length }}
            </span>
          </i18n>
          <span :class="['collapse fr', { active: !trend.isShow }]"
            @click="trend.isShow = !trend.isShow">
            {{ trend.isShow ? $t('收起') : $t('展开') }}
            <i :title="trend.isShow ? $t('收起') : $t('展开')"
              class="bk-icon icon-arrows-up-circle arrows" />
          </span>
          <span class="table-top-discardedDat fr"
            @click="showDiscardedData = true">
            {{ $t('查看最近丢弃数据') || '查看最近丢弃数据' }}
          </span>
          <span>({{ $t('最近一小时') }})</span>
        </div>
        <div v-show="trend.isShow">
          <bkdata-table
            v-bkloading="{ isLoading: trend.loading }"
            :data="trendTableList"
            :pagination="dropPagination"
            :emptyText="$t('暂无数据')"
            @page-change="handleDropPageChange"
            @page-limit-change="handleDropPageLimitChange">
            <bkdata-table-column :label="$t('时间')"
              prop="time" />
            <bkdata-table-column :label="$t('丢弃条数')"
              :sortable="true"
              prop="data_cnt" />
            <bkdata-table-column :label="$t('丢弃原因')"
              prop="reason" />
          </bkdata-table>
        </div>
      </div>
    </div>
    <div class="clean-info">
      <div class="title">
        <p class="title-text fl">
          {{ $t('清洗字段信息') }}
        </p>
        <span :class="['collapse fr', { active: !fieldInfo.isShow }]"
          @click="fieldInfo.isShow = !fieldInfo.isShow">
          {{ fieldInfo.isShow ? $t('收起') : $t('展开') }}
          <i :title="fieldInfo.isShow ? $t('收起') : $t('展开')"
            class="bk-icon icon-arrows-up-circle arrows" />
        </span>
      </div>
      <div v-show="fieldInfo.isShow">
        <bkdata-table
          v-bkloading="{ isLoading: fieldInfo.loading }"
          :data="fieldInfo.tableList"
          :emptyText="$t('暂无数据')">
          <bkdata-table-column :label="$t('序号')"
            width="80"
            type="index" />
          <bkdata-table-column :label="$t('名称')"
            :sortable="true">
            <template slot-scope="item">
              <span>
                {{ item.row.name }}
                <i
                  :title="$t('时间字段')"
                  :class="['bk-icon', { 'icon-time': cleanInfo.time_field_name === item.row.name }]"
                  style="color: red" />
              </span>
            </template>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('类型')"
            prop="type" />
          <bkdata-table-column :label="$t('中文名称')"
            prop="description" />
          <bkdata-table-column :label="$t('最新字段值')">
            <template slot-scope="item">
              <span v-bk-tooltips="item.row.newvalue">{{ item.row.newvalue }}</span>
            </template>
          </bkdata-table-column>
        </bkdata-table>
      </div>
    </div>
    <div class="clean-data">
      <div class="title">
        <p class="title-text fl">
          {{ $t('清洗后最新数据') }}
        </p>
        <span :class="['collapse fr', { active: !lastData.isShow }]"
          @click="lastData.isShow = !lastData.isShow">
          {{ lastData.isShow ? $t('收起') : $t('展开') }}
          <i class="bk-icon icon-arrows-up-circle arrows" />
        </span>
      </div>
      <div v-show="lastData.isShow"
        v-bkloading="{ isLoading: lastData.loading }"
        class="latest-data">
        <div class="line-num">
          <p v-for="(n, i) in lastData.lineNum"
            :key="i">
            {{ n }}
          </p>
        </div>
        <div class="data-content bk-scroll-x">
          <!--eslint-disable vue/no-v-html-->
          <pre class="data-json"
            v-html="lastData.cleanLastData" />
        </div>
      </div>
    </div>
    <div class="clean-data">
      <div class="title">
        <p class="title-text fl">
          {{ $t('操作历史') }}
        </p>
        <span :class="['collapse fr', { active: !isLogShow }]"
          @click="isLogShow = !isLogShow">
          {{ isLogShow ? $t('收起') : $t('展开') }}
          <i class="bk-icon icon-arrows-up-circle arrows" />
        </span>
      </div>
      <div v-show="isLogShow">
        <OperationHistory :isOnlyTable="true"
          :storageItem="itemObj" />
      </div>
    </div>
    <bkdata-dialog
      v-model="showStopDialog"
      extCls="bkdata-dialog"
      :theme="stopTaskDialogSetting.theme"
      :maskClose="false"
      :okText="$t('确定')"
      :cancelText="$t('取消')"
      :width="stopTaskDialogSetting.width"
      :title="stopTaskDialogSetting.title"
      @confirm="stop">
      <p class="clean-list-stop">
        {{ $t('停止清洗任务描述') }}
      </p>
    </bkdata-dialog>
    <!-- 查看丢弃的清洗数据 -->
    <discardDataTable v-model="showDiscardedData"
      :rid="rid" />
  </div>
</template>
<script>
// import cleanTable from '@/bkdata-ui/magicbox/bk-magic/bkdata-components/bkdata-table/bkdata-table'
import discardDataTable from './components/discardDataTable';
import { postMethodWarning } from '@/common/js/util.js';
import OperationHistory from '@/pages/DataAccess/Details/DataStorage/operatorList';
import DetailStatusOverview from '@/pages/dataManage/datadetailChildren/components/detailStatusOverview';
import DataManageChart from '@/pages/dataManage/datadetailChildren/components/dataManageChart';
import { mapState } from 'vuex';
import Cookies from 'js-cookie';
let beautify = require('json-beautify');
export default {
  components: {
    // cleanTable,
    OperationHistory,
    DetailStatusOverview,
    DataManageChart,
    discardDataTable,
  },
  provide: {
    userName: Cookies.get('bk_uid'),
  },
  props: {
    rid: {
      type: String,
    },
    itemObj: {
      type: Object,
      default: () => ({}),
    },
    details: {
      type: Object,
      default: () => ({}),
    },
  },
  computed: {
    cleanClass() {
      return /running|started/i.test(this.cleanInfo.status)
        ? 'running'
        : /failed|failure/i.test(this.cleanInfo.status)
          ? 'failed'
          : '';
    },
    canStart() {
      return /running|started/i.test(this.cleanInfo.status) || /failed/i.test(this.cleanInfo.status);
    },
    canStop() {
      return /NOSTART/i.test(this.cleanInfo.status);
    },
    dataInfo() {
      return {
        name: this.cleanInfo.result_table_name_alias,
        status: this.cleanInfo.status_display,
        updatedAt: this.cleanInfo.updated_at,
        updatedBy: this.cleanInfo.updated_by || this.cleanInfo.created_by,
      };
    },
    relatedtableList() {
      // this.tableDataTotal = this.relatedTask.tableList.length
      return this.relatedTask.tableList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    trendTableList() {
      return this.trend.tableList.slice(
        this.dropPageCount * (this.dropCurPage - 1),
        this.dropPageCount * this.dropCurPage
      );
    },
    pagination() {
      return {
        count: this.relatedTask.tableList.length,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
    dropPagination() {
      return {
        count: this.dropTableDataTotal,
        limit: this.dropPageCount,
        current: this.dropCurPage || 1,
      };
    },
    ...mapState({
      alertData: state => state.accessDetail.alertData,
    }),
  },
  watch: {
    active(val) {
      this.cleanChartInfo.data = [];
      this.getChartData();
    },
    alertData: {
      immediate: true,
      handler(val) {
        if (Object.keys(val).length) {
          this.getWarningList();
        }
      },
    },
  },
  /* eslint-disable */
  data() {
    return {
      // 打开查看丢弃数据的弹窗
      showDiscardedData: false,
      dropTableDataTotal: 1,
      dropCurPage: 1,
      dropPageCount: 10,
      cleanInfo: {},
      chart: {
        chartLoading: false,
      },
      loading: {
        startLoading: false,
        stopLoading: false,
      },
      trend: {
        // 数据趋势
        loading: false,
        isShow: false,
        tableFields: [],
        tableList: [],
      },
      fieldInfo: {
        //  清洗字段信息
        loading: true,
        isShow: true,
        tableList: [],
      },
      lastData: {
        // 清洗后最新数据
        loading: false,
        isShow: true,
        lineNum: 5, // json行数
        cleanLastData: '',
      },
      relatedTask: {
        // 关联任务
        loading: true,
        isShow: true,
        tableList: [],
      },
      cleanChartInfo: {
        data: [
          {
            type: 'lines',
            name: this.$t('输出量'),
            line: {
              color: 'rgba(133, 142, 200, 1)',
            },
            x: [],
            y: [],
          },
          {
            type: 'lines',
            name: this.$t('输入量'),
            line: {
              color: '#c2d0e3',
            },
            x: [],
            y: [],
          },
        ],
        layout: {
          height: 320,
          // scale: 1,
          margin: {
            l: 35,
            r: 70,
            b: 80,
            t: 20,
          },
          showlegend: true,
          // width: 1148,
          xaxis: {
            type: 'category',
            autorange: true,
            range: [0, 360],
            dtick: 50,
            showgrid: true, // 是否显示x轴线条
            tickwidth: 1,
            zeroline: true,
            showline: false,
            showtick: true,
          },
          yaxis: {
            autorange: true,
            showgrid: true,
            showline: true,
            showtick: true,
          },
        },
        config: {
          operationButtons: ['toImage', 'autoScale2d', 'resetScale2d'],
        },
      },
      curPage: 1,
      pageCount: 10, // 每一页数据量设置
      tableDataTotal: 1,
      isLogShow: false,
      showStopDialog: false,
      stopTaskDialogSetting: {
        width: '422px',
        title: window.$t('请确认是否停止清洗任务'),
        content: window.$t('停止清洗任务描述'),
        theme: 'danger',
      },
      active: 'trend',
      allOutPut: 0,
      allInPut: 0,
      hasGetCount: false,
      warningList: [],
      chartYaxisMax: 0,
    };
  },
  methods: {
    getWarningList() {
      this.bkRequest
        .httpRequest('dataAccess/getWarningList', {
          params: {
            raw_data_id: this.$route.params.did,
          },
          query: {
            alert_config_ids: [this.alertData.id],
            dimensions: JSON.stringify({
              module: 'clean',
              data_set_id: this.rid,
            }),
          },
        })
        .then(res => {
          if (res.result) {
            this.warningList = res.data || [];
            // this.addAlertWarnLine()
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    filterChartData(filterArr) {
      this.cleanChartInfo.data = this.cleanChartInfo.data.filter(item => !filterArr.includes(item.name));
    },
    changeActive(val) {
      this.active = val;
    },
    addAlertWarnLine() {
      const timeArr = [];
      const yArr = [];
      const warningMessage = [];
      this.warningList.reverse().forEach(item => {
        timeArr.push(this.forMateDate(new Date(item.alert_time).getTime()));
        yArr.push(this.chartYaxisMax);
        warningMessage.push(item.full_message);
      });
      this.cleanChartInfo.data.push({
        type: 'bar',
        name: this.$t('告警线'),
        x: timeArr,
        y: yArr,
        hovertemplate: warningMessage,
        width: 0.05,
        marker: {
          color: '#ea3636',
        },
      });
    },
    forMateDate(time, status) {
      let preArr = Array.apply(null, Array(10)).map((elem, index) => {
        return '0' + index;
      });
      let date = new Date(time);
      let year = date.getFullYear();
      let month = date.getMonth() + 1; // 月份是从0开始的
      let day = date.getDate();
      let hour = date.getHours();
      let min = date.getMinutes();
      let sec = date.getSeconds();

      let paramsTime =
        (preArr[month] || month) +
        '-' +
        (preArr[day] || day) +
        ' ' +
        (preArr[hour] || hour) +
        ':' +
        (preArr[min] || min) +
        ':' +
        (preArr[sec] || sec);
      let newTime =
        year +
        '-' +
        (preArr[month] || month) +
        '-' +
        (preArr[day] || day) +
        ' ' +
        (preArr[hour] || hour) +
        ':' +
        (preArr[min] || min);
      let nowDayTime = (preArr[hour] || hour) + ':' + (preArr[min] || min);
      if (status === 'params') {
        return paramsTime;
      } else if (status === 'nowDay') {
        return nowDayTime;
      } else {
        return newTime;
      }
    },
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    handleDropPageChange(page) {
      this.dropCurPage = page;
    },
    handleDropPageLimitChange(pageSize) {
      this.dropCurPage = 1;
      this.dropPageCount = pageSize;
    },
    /*
     *   点击编辑
     */
    edit() {
      this.$router.push({
        name: 'edit_clean',
        params: {
          rawDataId: this.$route.params.did,
          rtid: this.rid,
        },
      });
    },
    /*
     *   停止
     */
    stop() {
      this.loading.stopLoading = true;
      this.axios
        .post(`etls/${this.rid}/stop/`)
        .then(res => {
          if (res.result) {
            this.cleanInfo.status = res.data.status;
            this.cleanInfo.status_display = res.data.status_display;
            postMethodWarning(this.$t('停止成功'), 'success');
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        .finally(() => {
          this.loading.stopLoading = false;
        });
    },
    /*
     *   启动
     */
    start() {
      this.loading.startLoading = true;
      this.axios
        .post(`etls/${this.rid}/start/`)
        .then(res => {
          if (res.result) {
            this.cleanInfo.status = res.data.status;
            this.cleanInfo.status_display = res.data.status_display;
            postMethodWarning(this.$t('启动成功'), 'success');
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        .finally(() => {
          this.loading.startLoading = false;
        });
    },
    init() {
      this.getCleanInfo();
      this.getChartData();
      this.getDiscards();
      this.$nextTick(() => {
        this.$modules.isActive('dataflow') && this.getRelatedTask();
      });
      this.cleanData();
    },
    /*
     *   获取清洗信息
     */
    getCleanInfo() {
      this.bkRequest
        .httpRequest('dataClear/getCleanRules', {
          params: { rtid: this.rid },
        })
        .then(res => {
          if (res.result) {
            this.cleanInfo = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        .then(() => {
          this.trend.loading = false;
        });
    },
    /*
     *   获取清洗RT统计丢弃量(最近1小时)
     */
    getDiscards() {
      this.trend.loading = true;
      this.axios
        .get(`etls/${this.rid}/list_monitor_data_loss_drop/`)
        .then(res => {
          if (res.result) {
            this.trend.tableList = res.data;
            this.dropTableDataTotal = this.trend.tableList.length;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        .then(() => {
          this.trend.loading = false;
        });
    },
    /*
     *   获取清洗字段信息, 清洗后最新数据
     */
    cleanData() {
      this.fieldInfo.loading = true;
      this.lastData.loading = true;
      this.Axios.origin
        .all([
          this.axios.get(`result_tables/${this.rid}/list_field/`),
          this.axios.get(`result_tables/${this.rid}/get_latest_msg/?source_type=stream_source`),
        ])
        .then(
          this.Axios.origin.spread((res1, res2) => {
            if (res1.result) {
              this.fieldInfo.tableList = res1.data.fields_detail;
              this.fieldInfo.tableList.forEach(element => {
                this.$set(element, 'newvalue', '_');
              });
            } else {
              this.getMethodWarning(res1.message, res1.code);
            }
            let datajson;
            if (res2.result) {
              let kafka = (res2.data && res2.data.kafka_msg) || res2.data;
              if (kafka === '暂无数据') {
                this.lastData.cleanLastData = kafka;
                return;
              }
              datajson = (kafka && JSON.parse(kafka)) || {};
              let beauty = beautify(datajson, null, 4, 80);
              this.lastData.cleanLastData = beauty
                .replace(/dtEventTimeStamp/g, '<strong>dtEventTimeStamp</strong>')
                .replace(/dtEventTime/g, '<strong>dtEventTime</strong>')
                .replace(/localTime/g, '<strong>localTime</strong>');
            } else {
              this.getMethodWarning(res2.message, res2.code);
            }
            this.newVal(this.fieldInfo.tableList, datajson);
          })
        )
        .then(() => {
          this.getLineNum();
        })
        .finally(_ => {
          this.fieldInfo.loading = false;
          this.lastData.loading = false;
        });
    },
    /*
     *   清洗后最新数据json高度
     */
    getLineNum() {
      this.$nextTick().then(() => {
        let height = document.querySelectorAll('.clean-data .data-json')[0].offsetHeight;
        this.lastData.lineNum = Math.ceil(height / 19);
      });
    },
    /*
     *   清洗字段信息 最新字段值
     */
    newVal(cleanField, cleanData) {
      for (let [key, value] of Object.entries(cleanData)) {
        cleanField.forEach(ele => {
          if (key === ele.name) {
            ele.newvalue = value;
          }
        });
      }
    },
    /*
     *   获取关联任务列表
     */
    getRelatedTask() {
      const userName = this.$store.getters.getUserName;
      this.relatedTask.loading = true;
      this.bkRequest
        .httpRequest('dataFlow/listRelaTaskByRtid', {
          query: {
            result_table_id: this.rid,
            bk_username: userName,
          },
        })
        .then(res => {
          if (res.result) {
            this.relatedTask.tableList = res.data;
            this.relatedTask.tableList.forEach((x, i) => {
              let sumStatus =
                x.status === 'started' && !x.has_exception
                  ? 'running'
                  : x.status === 'started' && x.has_exception
                  ? 'exception'
                  : 'stopping';
              this.$set(x, 'sumStatus', sumStatus);
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        .finally(() => {
          this.relatedTask.loading = false;
        });
    },
    /*
     *   获取图表数据
     */
    getChartData(timeMultiple = 1) {
      this.chart.chartLoading = true;
      var tags = '';
      if (this.details.access_raw_data.tags.manage && this.details.access_raw_data.tags.manage.geog_area) {
        tags = this.details.access_raw_data.tags.manage.geog_area[0].code;
      }
      const query = {
        data_set_ids: this.rid,
        storages: 'kafka',
        tags: tags,
      };
      const nowTime = new Date().getTime();
      const oneDay = 1000 * 60 * 60 * 24;
      if (this.active === 'trend') {
        if (timeMultiple > 1) {
          // 7天前或1天前数据
          query.start_time = nowTime - oneDay * timeMultiple + 'ms';
          query.end_time = nowTime - oneDay * (timeMultiple - 1) + 'ms';
        }
        const getOutPut = this.bkRequest
          .httpRequest('dataAccess/getDepotOutput', {
            query: query,
          })
          .then(res => {
            if (res.result) {
              return {
                dataType: 'output',
                data: res.data,
              };
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          });
        const getInPut = this.bkRequest
          .httpRequest('dataAccess/getDepotInput', {
            query: query,
          })
          .then(res => {
            if (res.result) {
              return {
                dataType: 'input',
                data: res.data,
              };
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          });
        const getAllOutPut = this.bkRequest
          .httpRequest('dataAccess/getDepotOutput', {
            query: {
              data_set_ids: this.rid,
              storages: 'kafka',
              tags: tags,
              format: 'value',
            },
          })
          .then(res => {
            if (res.result) {
              return {
                dataType: 'AllOutput',
                data: res.data.length ? res.data[0].value.output_count : 0,
              };
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          });
        const getAllInPut = this.bkRequest
          .httpRequest('dataAccess/getDepotInput', {
            query: {
              data_set_ids: this.rid,
              storages: 'kafka',
              tags: tags,
              format: 'value',
            },
          })
          .then(res => {
            if (res.result) {
              return {
                dataType: 'AllInput',
                data: res.data.length ? res.data[0].value.input_count : 0,
              };
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          });
        let httpArr = [getOutPut, getInPut, getAllOutPut, getAllInPut];
        if (this.hasGetCount) {
          httpArr = [getOutPut, getInPut];
        }
        Promise.all(httpArr)
          .then(res => {
            const inputData = {
              input: [],
              time: [],
            };
            const outputData = {
              output: [],
              time: [],
            };
            if (
              !res.find(item => item.dataType === 'input').data.length &&
              !res.find(item => item.dataType === 'output').data.length
            )
              return;
            res.forEach(item => {
              if (item.dataType === 'input' && item.data[0]) {
                item.data[0].series.map(child => {
                  inputData.input.push(child.input_count || 0);
                  inputData.time.push(this.forMateDate(child.time * 1000 + oneDay * (timeMultiple - 1)));
                });
              } else if (item.dataType === 'output' && item.data[0]) {
                item.data[0].series.map(child => {
                  outputData.output.push(child.output_count || 0);
                  outputData.time.push(this.forMateDate(child.time * 1000 + oneDay * (timeMultiple - 1)));
                });
              }
            });
            this.chartYaxisMax =
              Math.max(...inputData.input) > Math.max(...outputData.output)
                ? Math.max(...inputData.input)
                : Math.max(...outputData.output);
            if (!this.hasGetCount) {
              this.allOutPut = res.find(item => item.dataType === 'AllOutput').data;
              this.allInPut = res.find(item => item.dataType === 'AllInput').data;
            }
            this.hasGetCount = true;
            this.createChart(outputData, inputData, timeMultiple);
          })
          .finally(() => {
            this.chart.chartLoading = false;
          });
      } else if (this.active === 'delay') {
        query.fill = 'none';
        this.bkRequest
          .httpRequest('dataAccess/getDataTimeDelay', {
            query: query,
          })
          .then(res => {
            if (res.result) {
              let dataChart = res.data;
              if (dataChart.length && dataChart[0].series.length > 0) {
                const dataDelayData = {
                  output: [],
                  time: [],
                };
                dataChart[0].series.forEach(item => {
                  dataDelayData.time.push(this.forMateDate(item.time * 1000));
                  dataDelayData.output.push(item.process_time_delay || 0);
                });
                this.createChart(dataDelayData);
              }
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          })
          .finally(() => {
            this.chart.chartLoading = false;
          });
      }
    },
    addChartData(outputData, inputData, timeMultiple) {
      if (timeMultiple === 7) {
        const isHaveDayData = this.cleanChartInfo.data.findIndex(item => item.name === this.$t('7天前输出量'));
        if (isHaveDayData < 0) {
          this.cleanChartInfo.data.push(
            {
              type: 'scatter',
              name: this.$t('7天前输出量'),
              line: {
                color: '#006000',
                dash: 'dot',
              },
              x: outputData.time,
              y: outputData.output,
            },
            {
              type: 'scatter',
              name: this.$t('7天前输入量'),
              line: {
                color: '#00BB00',
                dash: 'dot',
              },
              x: inputData.time,
              y: inputData.input,
            }
          );
        }
      }
      if (timeMultiple === 2) {
        const isHaveDayData = this.cleanChartInfo.data.findIndex(item => item.name === this.$t('1天前输出量'));
        if (isHaveDayData < 0) {
          this.cleanChartInfo.data.push(
            {
              type: 'lines',
              name: this.$t('1天前输出量'),
              line: {
                color: '#844200',
                dash: 'dashdot',
              },
              x: outputData.time,
              y: outputData.output,
            },
            {
              type: 'lines',
              name: this.$t('1天前输入量'),
              line: {
                color: '#FF9224',
                dash: 'dashdot',
              },
              x: inputData.time,
              y: inputData.input,
            }
          );
        }
      }
    },
    /*
     *   创建清洗数据质量图
     */
    createChart(
      outputData,
      inputData = {
        time: [],
        input: [],
      },
      timeMultiple
    ) {
      if (timeMultiple > 1) {
        return this.addChartData(outputData, inputData, timeMultiple);
      }
      this.cleanChartInfo.data = [];
      this.cleanChartInfo.data.push(
        {
          type: 'lines',
          name: this.active === 'trend' ? this.$t('输出量') : this.$t('延迟时间(单位: 秒)'),
          line: {
            color: 'rgba(133, 142, 200, 1)',
          },
          x: outputData.time,
          y: outputData.output,
        },
        {
          type: 'lines',
          name: this.$t('输入量'),
          line: {
            color: '#c2d0e3',
          },
          x: inputData.time,
          y: inputData.input,
        }
      );
      if (!this.warningList.length || this.active === 'delay') return;
      // this.addAlertWarnLine()
    },
    /*
     *   返回列表页
     */
    cleanList() {
      this.$emit('backList');
    },
    jumpFlow(flowId, projectId) {
      this.$router.push({
        name: 'dataflow_ide', // 'dataflow_detail',
        params: {
          fid: flowId,
        },
        query: {
          project_id: projectId,
        },
      });
    },
  },
  mounted() {
    this.init(this.dataChart);
  },
};
</script>
<style lang="scss">
.clean-Detail {
  padding: 15px;
  .title {
    height: 60px;
    line-height: 20px;
    padding: 20px 15px 20px 0;
    .title-text {
      font-weight: bold;
      border-left: 2px solid #3a84ff;
      padding: 0 10px;
    }
    .arrows {
      top: 3px;
    }
  }
  .collapse {
    position: relative;
    color: #3a84ff;
    padding: 0 20px 0 0;
    cursor: pointer;
    &.active {
      .arrows {
        transform: rotate(180deg);
      }
    }
  }
  .arrows {
    position: absolute;
    right: 3px;
    top: 35%;
    transition: all 0.3s ease-out;
  }
  .back-list {
    .bk-button {
      line-height: 26px;
      height: 26px;
    }
  }
  .detail-labels {
    line-height: 20px;
    margin: 16px 0;
    .lables-title {
      float: left;
      margin-right: 10px;
    }
    .lable {
      margin-right: 3px;
      height: 22px;
      color: #737987;
      font-size: 14px;
      background: #efefef;
      border-color: #efefef;
    }
  }
  .clean-trends {
    .detail-chart {
      width: 100%;
      background: #fafafa;
    }
    .table {
      margin-top: 20px;
    }
    .table-top {
      background: #efefef;
      line-height: 46px;
      padding: 0 15px;
      border: 1px solid #dbe1e7;
      border-bottom: none;
    }
    .table-top-text {
      font-weight: bold;
      .text-num {
        display: inline-block;
        font-size: 12px;
        color: #fff;
        line-height: 16px;
        border-radius: 2px;
        background: #fe771d;
        padding: 0 5px;
        margin: 0 5px;
      }
    }
    .table-top-discardedDat {
      cursor: pointer;
      color: #3a84ff;
      margin-right: 20px;
    }
  }
  .latest-data {
    min-height: 280px;
    border: 1px solid #d9dfe5;
    background: #fafafa;
    position: relative;
    .line-num {
      width: 40px;
      height: 100%;
      float: left;
      text-align: center;
      border-right: 1px solid #d9dfe5;
      background: #fff;
      position: absolute;
    }
    .data-content {
      margin-left: 40px;
      width: calc(100% - 40px);

      strong {
        color: #212232;
        font-weight: bold;
      }
    }
  }
  .related-task {
    .sum-status {
      display: inline-block;
      font-size: 12px;
      background: #737987;
      border-radius: 2px;
      padding: 0 5px;
      color: #fff;
      &.running {
        background: #9dcb6b;
      }
      &.exception {
        background: #ff5555;
      }
    }
    .checkDeatil {
      color: #3a84ff;
      cursor: pointer;
    }
    .project-task {
      .icon {
        color: #737987;
      }
      span {
        margin-right: 5px;
      }
    }
  }
}
</style>
