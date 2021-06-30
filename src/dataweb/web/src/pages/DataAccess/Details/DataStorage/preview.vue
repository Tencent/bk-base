

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
  <div class="data-preview-container">
    <div class="mb10">
      <bkdata-button size="small"
        theme="primary"
        @click="$emit('prev')">
        {{ $t('返回列表') }}
      </bkdata-button>
    </div>

    <detailStatusOverview
      :data-type="'shipper'"
      :canStart="canStart"
      :canStop="canStop"
      class="mb20"
      :cleanClass="itemObj.status_en"
      :loading="loading"
      :data-info="dataInfo"
      @start="taskOption('start', itemObj, ['stopped', 'failed'].includes(itemObj.status_en))"
      @stop="tryStop(itemObj, !['stopped', 'failed', 'deleted'].includes(itemObj.status_en))"
      @edit="editItem" />
    <bkdata-dialog
      v-model="showStopDialog"
      extCls="bkdata-dialog"
      :theme="stopTaskDialogSetting.theme"
      :maskClose="false"
      :okText="$t('确定')"
      :cancelText="$t('取消')"
      :width="stopTaskDialogSetting.width"
      :title="stopTaskDialogSetting.title"
      @confirm="taskOption('stop', stopitem, stopActive)"
      @cancel="cancelFn">
      <p class="clean-list-stop">
        {{ stopTaskDialogSetting.content }}
      </p>
    </bkdata-dialog>
    <Layout
      class="container-with-shadow"
      :crumbName="$t('存储配置')"
      :withMargin="false"
      :headerMargin="false"
      :isOpen="true"
      :headerBackground="'inherit'"
      :collspan="true"
      height="auto">
      <span slot="header"
        style="margin-right: 20px">
        <i
          style="cursor: pointer"
          :title="$t('编辑')"
          class="bk-icon icon-edit"
          @click="$parent.activeModule = 'edit'" />
      </span>

      <StorageConfigModule
        ref="storageConfigModule"
        :bizId="details.bk_biz_id"
        :topicName="details.access_raw_data.topic_name"
        :hasReplica="hasReplica"
        :mode="'detail'"
        :validate="validate"
        :storageConfigModuleData="StorageConfigModuleData"
        :disabled="true"
        :displayItem="itemObj"
        @changeDuplication="changeDuplication" />
    </Layout>
    <template v-if="!isRawData">
      <Layout
        class="container-with-shadow"
        :crumbName="$t('字段设置')"
        :withMargin="false"
        :headerMargin="false"
        :isOpen="true"
        :headerBackground="'inherit'"
        :collspan="true"
        height="auto">
        <span slot="header"
          style="margin-right: 20px">
          <i
            style="cursor: pointer"
            title="$t('编辑')"
            class="bk-icon icon-edit"
            @click="$parent.activeModule = 'edit'" />
        </span>
        <FieldConfigModule
          :disabled="true"
          :backupField.sync="backupField"
          :fieldConfigModuleData="FieldConfigModuleData"
          mode="preview" />
      </Layout>
    </template>
    <template v-if="!isRawData">
      <Layout
        class="container-with-shadow"
        :crumbName="$t('入库数据质量')"
        :withMargin="false"
        :headerMargin="false"
        :isOpen="true"
        :headerBackground="'inherit'"
        :collspan="true"
        height="auto">
        <span slot="header"
          style="margin-right: 20px">
          <i
            style="cursor: pointer"
            title="$t('编辑')"
            class="bk-icon icon-edit"
            @click="$parent.activeModule = 'edit'" />
        </span>
        <DataManageChart
          :isLoading="chart.chartLoading"
          :cleanChartInfo="cleanChartInfo"
          @filterChartData="filterChartData"
          @changeActive="changeActive"
          @getChartData="getChartData" />
      </Layout>
    </template>
    <template v-if="!isRawData">
      <Layout
        class="container-with-shadow"
        :crumbName="$t('结果查询')"
        :withMargin="false"
        :headerMargin="false"
        :headerBackground="'inherit'"
        :collspan="true"
        height="auto">
        <div style="margin: -30px -15px -15px; width: 100%">
          <template v-if="isSqlQuery">
            <SqlQuery
              :nodeSearch="false"
              :formData="sqlFormData"
              :showTitle="false"
              :resultTable="itemObj.result_table_id"
              :activeType="itemObj.storage_type" />
          </template>
          <template v-if="isEsQuery">
            <EsQuery :nodeSearch="false"
              :showTitle="false"
              :resultTable="itemObj.result_table_id" />
          </template>
        </div>
      </Layout>
    </template>
    <OperationHistory :storageItem="itemObj" />
  </div>
</template>

<script>
import Layout from '@/components/global/layout';
import StorageConfigModule from './storage-config-module';
import FieldConfigModule from './field-config-module';
import SqlQuery from '@/pages/DataQuery/Components/SqlQuery';
import EsQuery from '@/pages/DataQuery/Components/EsQuery';
import OperationHistory from './operatorList';
import detailStatusOverview from '@/pages/dataManage/datadetailChildren/components/detailStatusOverview';
import optionsMixins from '@/pages/DataAccess/Details/DataStorage/mixin/options.js';
import DataManageChart from '@/pages/dataManage/datadetailChildren/components/dataManageChart';

export default {
  components: {
    Layout,
    StorageConfigModule,
    FieldConfigModule,
    SqlQuery,
    EsQuery,
    OperationHistory,
    detailStatusOverview,
    DataManageChart,
  },
  mixins: [optionsMixins],
  props: {
    details: {
      type: Object,
      default: () => {
        {
        }
      },
    },
    itemObj: {
      type: Object,
      default: () => {
        {
        }
      },
    },
  },
  data() {
    return {
      backupField: {},
      FieldConfigModuleData: {},
      changeOptions: {
        type: null,
        cluster: null,
      },
      validate: {
        dataName: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { regExp: /^[a-zA-Z][a-zA-Z0-9_]*$/, error: '只能是字母加下划线格式' },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        dataAlias: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        deadline: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      hasReplica: false,
      showStopDialog: false,
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
      chart: {
        chartLoading: false,
        chartData: [
          {
            series: [],
          },
        ],
      },
      active: 'trend',
    };
  },
  computed: {
    /** 是否为原始数据 */
    isRawData() {
      return this.itemObj.data_type === 'raw_data';
    },
    cleanClass() {
      return /running|started/i.test(this.itemObj.status_en)
        ? 'running'
        : /failed/i.test(this.itemObj.status_en)
          ? 'failed'
          : '';
    },
    canStart() {
      return !this.isRawData && /running|started/i.test(this.itemObj.status_en);
    },
    canStop() {
      return !this.isRawData && ['stopped', 'failed', 'deleted'].includes(this.itemObj.status_en);
    },
    dataInfo() {
      return {
        name: this.itemObj.data_alias,
        status: this.itemObj.status,
        updatedAt: this.itemObj.created_at,
        updatedBy: this.itemObj.created_by,
      };
    },
    sqlFormData() {
      return {
        order: [this.itemObj.storage_type],
        storage: {
          [this.itemObj.storage_type]: {
            sql: this.FieldConfigModuleData.sql,
          },
        },
      };
    },
    totalPage() {
      return Math.ceil(this.logList.length / this.pageCount) || 1;
    },
    logListView() {
      return this.logList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    isSqlQuery() {
      return !/^es/i.test(this.itemObj.storage_type);
    },
    isEsQuery() {
      return /^es/i.test(this.itemObj.storage_type);
    },
  },
  watch: {
    active(val) {
      this.cleanChartInfo.data = [];
      this.getChartData();
    },
  },
  mounted() {
    /** 获取数据入库详情（查询rt以及rt相关配置信息需整合存储配置项接口 */
    this.getStorageDetail();

    this.getStoreType();

    if (!this.isRawData) {
      /** 获取详情页面字段设置配置 */
      this.getFieldConfigModuleData();
      this.getChartData();
    }
  },
  methods: {
    filterChartData(filterArr) {
      this.cleanChartInfo.data = this.cleanChartInfo.data.filter(item => !filterArr.includes(item.name));
    },
    changeActive(val) {
      this.active = val;
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

      let paramsTime =        (preArr[month] || month)
        + '-'
        + (preArr[day] || day)
        + ' '
        + (preArr[hour] || hour)
        + ':'
        + (preArr[min] || min)
        + ':'
        + (preArr[sec] || sec);
      let newTime =        year
        + '-'
        + (preArr[month] || month)
        + '-'
        + (preArr[day] || day)
        + ' '
        + (preArr[hour] || hour)
        + ':'
        + (preArr[min] || min);
      let nowDayTime = (preArr[hour] || hour) + ':' + (preArr[min] || min);
      if (status === 'params') {
        return paramsTime;
      } else if (status === 'nowDay') {
        return nowDayTime;
      } else {
        return newTime;
      }
    },
    getChartData(timeMultiple = 1) {
      // 获取图表数据
      this.chart.chartLoading = true;
      var tags = '';
      if (this.details.access_raw_data.tags.manage && this.details.access_raw_data.tags.manage.geog_area) {
        tags = this.details.access_raw_data.tags.manage.geog_area[0].code;
      }
      const query = {
        data_set_ids: this.itemObj.result_table_id,
        storages: this.StorageConfigModuleData.storageType,
        tags: tags,
      };
      let url = '';
      const nowTime = new Date().getTime();
      const oneDay = 1000 * 60 * 60 * 24;
      if (this.active === 'trend') {
        url = 'dataAccess/getDepotOutput';
        if (timeMultiple > 1) {
          query.start_time = nowTime - oneDay * timeMultiple + 'ms';
          query.end_time = nowTime - oneDay * (timeMultiple - 1) + 'ms';
        }
      } else {
        url = 'dataAccess/getDataTimeDelay';
        query.fill = 'none';
      }
      this.bkRequest
        .httpRequest(url, {
          query: query,
        })
        .then(res => {
          if (res.result) {
            let dataChart = res.data;
            if (dataChart.length && dataChart[0].series.length > 0) {
              const xArr = [];
              const yArr = [];
              dataChart[0].series.forEach(item => {
                xArr.push(this.forMateDate(item.time * 1000 + oneDay * (timeMultiple - 1)));
                if (this.active === 'trend') {
                  yArr.push(item.output_count || 0);
                } else {
                  yArr.push(item.process_time_delay || 0);
                }
              });
              this.createChart(xArr, yArr, timeMultiple);
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.chart.chartLoading = false;
        });
    },
    addChartData(xArr, yArr, timeMultiple) {
      if (timeMultiple === 7) {
        const isHaveDayData = this.cleanChartInfo.data.findIndex(item => item.name === this.$t('7天前输出量'));
        if (isHaveDayData < 0) {
          this.cleanChartInfo.data.push({
            type: 'scatter',
            name: this.$t('7天前输出量'),
            line: {
              color: '#006000',
              dash: 'dot',
            },
            x: xArr,
            y: yArr,
          });
        }
      }
      if (timeMultiple === 2) {
        const isHaveDayData = this.cleanChartInfo.data.findIndex(item => item.name === this.$t('1天前输出量'));
        if (isHaveDayData < 0) {
          this.cleanChartInfo.data.push({
            type: 'lines',
            name: this.$t('1天前输出量'),
            line: {
              color: '#844200',
              dash: 'dashdot',
            },
            x: xArr,
            y: yArr,
          });
        }
      }
    },
    createChart(xArr, yArr, timeMultiple) {
      // 创建清洗数据质量图
      if (timeMultiple > 1) {
        return this.addChartData(xArr, yArr, timeMultiple);
      }
      this.cleanChartInfo.data = [];
      this.cleanChartInfo.data.push({
        type: 'lines',
        name: this.active === 'trend' ? this.$t('输出量') : this.$t('延迟时间(单位: 秒)'),
        line: {
          color: 'rgba(133, 142, 200, 1)',
        },
        x: xArr,
        y: yArr,
      });
    },
    editItem() {
      this.$parent.activeModule = 'edit';
      this.$parent.itemObj = this.itemObj;
    },
    getStoreType() {
      this.bkRequest.httpRequest('dataStorage/getStorageType').then(res => {
        if (res.result) {
          this.$set(this.StorageConfigModuleData, 'storageTypeList', res.data);
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    getStorageDetail() {
      /** 存储表名 */
      this.StorageConfigModuleData.dataName = this.isRawData ? '--' : this.itemObj.data_name;
      this.StorageConfigModuleData.dataAlias = this.isRawData ? '--' : this.itemObj.data_alias;

      /** 数据源 */
      this.StorageConfigModuleData.rawData = this.itemObj.data_name;

      /** 存储类型 */
      this.StorageConfigModuleData.storageType = this.itemObj.storage_type;

      /** 存储集群 */
      this.StorageConfigModuleData.storageCluster = this.itemObj.storage_cluster;

      /** 过期时间 */
      this.StorageConfigModuleData.deadline = this.isRawData ? '--' : this.itemObj.expire_time_alias;
    },
    getFieldConfigModuleData() {
      const params = {
        rtid: this.itemObj.result_table_id,
        clusterType: this.itemObj.storage_type,
      };

      const query = {
        flag: 'all',
      };
      this.bkRequest.httpRequest('meta/getSchemaAndSqlByClusterType', { params, query }).then(res => {
        // console.log('Result=>', res)
        this.FieldConfigModuleData = res;
        this.backupField = JSON.parse(JSON.stringify(res));
        this.StorageConfigModuleData.allowDuplication = this.FieldConfigModuleData.config.has_unique_key;
        if (this.FieldConfigModuleData.config.has_replica !== undefined) {
          this.StorageConfigModuleData.copyStorage = this.FieldConfigModuleData.config.has_replica;
        }
        this.processFieldData(this.StorageConfigModuleData.allowDuplication); // 新增入库时，默认过滤掉
        this.StorageConfigModuleData.maxRecords = this.FieldConfigModuleData.config.max_records;
        this.StorageConfigModuleData.zoneID = this.FieldConfigModuleData.config.zone_id;
        this.StorageConfigModuleData.setID = this.FieldConfigModuleData.config.set_id;

        this.StorageConfigModuleData.index_fields = (
          this.FieldConfigModuleData.config.order_by || []
        ).map((by, index) => ({ physical_field: by, index }));
        this.backupField.index_fields = this.StorageConfigModuleData.index_fields;
        this.FieldConfigModuleData.index_fields = this.StorageConfigModuleData.index_fields;

        if (this.FieldConfigModuleData.config.hasOwnProperty('has_replica')) {
          if (this.FieldConfigModuleData.config.has_replica) {
            this.hasReplica = true;
          } else {
            this.hasReplica = false;
          }
        } else {
          this.hasReplica = false;
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .crumb-right {
  display: flex;
  align-items: center;
}
.no_data {
  height: 120px;
  display: flex;
  align-items: center;
  flex-direction: column;
  justify-content: center;
}
.data-preview-container {
  padding: 0 15px;
}

.operation-history {
  width: 100%;
  table {
    border: 1px solid #e6e6e6;
  }
}
</style>
