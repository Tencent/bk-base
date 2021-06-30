

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
    <template v-if="showPreRoute">
      <div class="mb10">
        <bkdata-button size="small"
          theme="primary"
          @click="$emit('prev')">
          {{ $t('返回列表') }}
        </bkdata-button>
      </div>
    </template>
    <Layout
      class="container-with-shadow"
      :crumbName="$t('存储配置')"
      :withMargin="false"
      :headerMargin="false"
      :isOpen="true"
      :headerBackground="'inherit'"
      :collspan="true"
      height="auto">
      <slot name="configModule"
        :data="slotProp" />
    </Layout>

    <Layout
      class="container-with-shadow"
      :crumbName="$t('迁移概况')"
      :withMargin="false"
      :isOpen="true"
      :headerMargin="false"
      :headerBackground="'inherit'"
      :collspan="true">
      <div style="width: 100%"
        class="remeval-preview">
        <div class="remeval-data">
          <div class="remeval-data-item">
            <div class="name">
              {{ StorageConfigModuleData.sourceStorage }} {{ $t('读取量') }}：
            </div>
            <div class="value">
              {{ inputCount }}
            </div>
          </div>
          <div class="remeval-data-item">
            <div class="name">
              {{ StorageConfigModuleData.targetStorage }} {{ $t('写入量') }}：
            </div>
            <div class="value">
              {{ outputCount }}
            </div>
          </div>
        </div>
        <RemovalStatus
          :chartData="chartData"
          :isChartLoading="previewInfo.loading"
          :dtick="20"
          :width="430"
          :margin="{ l: 60, r: 50, b: 90, t: 80 }" />
      </div>
    </Layout>

    <Layout
      class="container-with-shadow"
      :crumbName="$t('迁移进度')"
      :withMargin="false"
      :isOpen="true"
      :headerMargin="false"
      :headerBackground="'inherit'"
      :collspan="true">
      <div style="width: 100%">
        <RemovalProgress
          :input="`${itemObj.source} ${$t('读取量')}`"
          :output="`${itemObj.dest} ${$t('写入量')}`"
          :detailInfo="detailInfo" />
      </div>
    </Layout>
  </div>
</template>

<script>
import Layout from '@/components/global/layout';
import RemovalProgress from '@/bizComponents/RemovalProgress/index.vue';
import RemovalStatus from '@/bizComponents/RemovalStatus/index.vue';

export default {
  components: {
    Layout,
    RemovalProgress,
    RemovalStatus,
  },
  props: {
    details: {
      type: Object,
      default: () => ({}),
    },
    itemObj: {
      type: Object,
      default: () => ({}),
    },
    showPreRoute: {
      type: Boolean,
      default: false,
    },
    needDataSourceGet: {
      type: Boolean,
      default: false
    }
  },
  data() {
    return {
      StorageConfigModuleData: {
        dataSourceDisable: false,
        dataStorageTypeDisable: false,
        dataNameDisable: false,
        dataNameAliasDisable: false,
        rawDataList: [],
        rawData: {
          id: null,
          name: null,
        },
        storageType: {
          id: null,
          name: null,
        },
        sourceStorage: '',
        targetStorage: '',
        initDateTime: [],
        coverData: false,
      },
      detailInfo: {
        loading: false,
        list: [
          {
            status: '',
          },
        ],
      },
      previewInfo: {
        loading: false,
        list: [],
      },

      validate: {
        targetStorage: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { regExp: /^[a-zA-Z][a-zA-Z0-9_]*$/, error: '只能是字母加下划线格式' },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        initDateTime: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        sourceStorage: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        coverData: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        rawData: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      chartData: [],
      inputCount: 0,
      outputCount: 0,
      timer: null,
    };
  },
  computed: {
    slotProp() {
      return {
        validate: this.validate,
        module: this.StorageConfigModuleData
      };
    }
  },
  mounted() {
    this.needDataSourceGet && this.getDataSourceList();
    /** 获取数据入库详情（查询rt以及rt相关配置信息需整合存储配置项接口 */
    this.getDataRemovalDetail();
    this.pollData();
  },
  beforeDestroy() {
    clearTimeout(this.timer);
  },
  methods: {
    // 轮询迁移概况和迁移进度数据
    pollData() {
      const that = this;
      Promise.all([that.getDataRemovalPreview(), that.getDataRemovalCount(), that.getDataRemovalProcess()]).then(
        res => {
          that.timer = setTimeout(that.pollData, 60000);
        }
      );
    },
    forMateDate(time, connector = '/', status) {
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

      let newTime =                year
                + connector
                + (preArr[month] || month)
                + connector
                + (preArr[day] || day)
                + ' '
                + (preArr[hour] || hour)
                + ':'
                + (preArr[min] || min)
                + ':'
                + (preArr[sec] || sec);
      let nowTime =                (preArr[month] || month)
                + connector
                + (preArr[day] || day)
                + ' '
                + (preArr[hour] || hour)
                + ':'
                + (preArr[min] || min);
      if (status === 'nowTime') {
        return nowTime;
      }
      return newTime;
    },
    getDataRemovalPreview() {
      this.previewInfo.loading = true;
      this.chartData = [];
      const query = {
        storages: this.itemObj.dest,
        start_time: this.itemObj.created_at,
        end_time: this.forMateDate(new Date(), '-'),
        conditions: JSON.stringify({ module: 'migration', component: this.itemObj.source }),
      };
      if (this.itemObj.status === 'finish') {
        // 参数按前闭后开取值，在现有的end_time上加一分钟来把end_time的时间点传进去
        query.end_time = this.forMateDate(new Date(this.itemObj.updated_at).getTime() + 60000, '-');
      }
      const params = {
        result_table_id: this.itemObj.result_table_id,
      };
      return this.bkRequest.httpRequest('dataRemoval/getDataRemovalPreview', { params, query }).then(res => {
        if (res.result) {
          if (!res.data.input_data.length && !res.data.output_data.length) {
            return (this.previewInfo.loading = false);
          }
          this.previewInfo.list = [];
          this.previewInfo.list.push({
            type: 'input',
            storage: this.itemObj.source,
            series: res.data.input_data[0].series,
          });
          this.previewInfo.list.push({
            type: 'output',
            storage: this.itemObj.dest,
            series: res.data.output_data[0].series,
          });
          this.handlePreviewData(this.previewInfo.list);
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.previewInfo.loading = false;
      });
    },
    getDataRemovalCount() {
      const query = {
        storages: this.itemObj.dest,
        start_time: this.itemObj.created_at,
        end_time: this.forMateDate(new Date(), '-'),
        conditions: JSON.stringify({ module: 'migration', component: this.itemObj.source }),
        output_format: 'value',
        input_format: 'value',
      };
      if (this.itemObj.status === 'finish') {
        // 参数按前闭后开取值，在现有的end_time上加一分钟来把end_time的时间点传进去
        query.end_time = this.forMateDate(new Date(this.itemObj.updated_at).getTime() + 60000, '-');
      }
      const params = {
        result_table_id: this.itemObj.result_table_id,
      };
      return this.bkRequest.httpRequest('dataRemoval/getDataRemovalPreview', { params, query }).then(res => {
        if (res.result) {
          this.inputCount = res.data.input_data.length ? res.data.input_data[0].value.input_count : 0;
          this.outputCount = res.data.output_data.length ? res.data.output_data[0].value.output_count : 0;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.previewInfo.loading = false;
      });
    },
    handlePreviewData(data) {
      const color = {
        0: '#f6ae00',
        1: '#3a84ff',
      };
      data.forEach((item, index) => {
        const data = {
          line: {
            color: color[index],
          },
          name: item.storage,
          type: 'lines',
          x: [],
          y: [],
        };
        item.series.forEach(child => {
          data.x.push(this.forMateDate(child.time * 1000 || 0, '/', 'nowTime'));
          data.y.push((item.type === 'input' ? child.input_count : child.output_count) || 0);
        });
        this.chartData.push(data);
      });
    },
    getDataRemovalProcess() {
      this.detailInfo.loading = true;
      const params = {
        id: this.itemObj.id,
      };
      this.StorageConfigModuleData.dataSourceLoading = true;
      return this.bkRequest.httpRequest('dataRemoval/getDataRemovalProcess', { params }).then(res => {
        if (res.result) {
          this.detailInfo.list = (res.data || []).sort((a, b) => {
            return new Date(b.created_at) - new Date(a.created_at);
          });
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.detailInfo.loading = false;
      });
    },
    getDataSourceList() {
      const query = {
        raw_data_id: this.$route.params.did,
      };
      this.StorageConfigModuleData.dataSourceLoading = true;
      this.bkRequest.httpRequest('dataStorage/getDataSourceList', { query }).then(res => {
        if (res.result) {
          const { data } = res;
          this.StorageConfigModuleData.rawDataList = (data || []).filter(item => item.id !== 'raw_data');
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.StorageConfigModuleData.dataSourceLoading = false;
      });
    },
    getDataRemovalDetail() {
      /** 数据源 */
      this.StorageConfigModuleData.rawData.id = this.itemObj.result_table_id;

      /** 来源存储 */
      this.StorageConfigModuleData.sourceStorage = this.itemObj.source;

      /** 目标存储 */
      this.StorageConfigModuleData.targetStorage = this.itemObj.dest;

      /** 时间范围 */
      this.StorageConfigModuleData.initDateTime = [this.itemObj.start, this.itemObj.end];

      /** 是否覆盖目标数据 */
      this.StorageConfigModuleData.coverData = this.itemObj.task_type === 'overall';

      this.$emit('afterGetDetail');
    },
  },
};
</script>

<style lang="scss" scoped>
.container-with-shadow {
    -webkit-box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
    box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
    border-radius: 2px;
    border: 1px solid rgba(195, 205, 215, 0.6);
}
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
    ::v-deep .layout-body {
        overflow: inherit;
        .remeval-preview {
            width: 100%;
            display: flex;
            justify-content: space-between;
            .remeval-data {
                flex: 1;
                display: flex;
                justify-content: space-around;
                .remeval-data-item {
                    border: 1px solid #ddd;
                    flex: 0.48;
                    padding-top: 20px;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                    .name {
                        font-size: 18px;
                        text-align: center;
                        font-weight: 700;
                    }
                    .value {
                        display: flex;
                        flex: 1;
                        justify-content: center;
                        align-items: center;
                        font-size: 18px;
                        font-weight: 550;
                    }
                }
            }
        }
    }
}

.operation-history {
    width: 100%;
    table {
        border: 1px solid #e6e6e6;
    }
}
</style>
