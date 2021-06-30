

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
  <div class="search-result-table" />
</template>

<script>
import '@tencent/bkcharts-panel';
import '@tencent/bkcharts-panel/dist/main.min.css';
import { format } from 'date-fns';
export default {
  components: {
    // cellClick
  },
  props: {
    tableData: {
      type: Object,
      required: true,
    },
    tableLoading: {
      type: Boolean,
      default: false,
    },
    task: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      panelInstance: null,
      optionChanged: false,
      autoSaveTimer: 0,
      chartConfig: {
        tableHeight: 500,
        watermark: {
          display: true,
          text: 'bk-data-web',
          fontSize: 30,
        },
        chart: {
          width: 460,
          height: 500,
        },
        plugins: {
          download: {
            enabled: true,
            text: window.$t('下载结果数据'),
          },
        },
        bkChartsActiveTabIndex: 0,
        bkChartsFormData: {
          chartType: '',
          yField: '',
          xField: '',
          groupBy: '',
          title: '',
          xTitle: '',
          yTitle: '',
          order: 'none',
        },
      },
    };
  },

  computed: {
    showIndex() {
      return this.tableData.list && !!this.tableData.list.length;
    },
  },
  watch: {
    tableData: {
      deep: true,
      handler(val) {
        if (val) {
          const { list, select_fields_order, info } = this.tableData;
          this.panelInstance.updateTableList(list || [], select_fields_order || [], this.getQueryMessage(), info);
          this.updateChartConfig();
          this.updateChartPanelOptions(this.chartConfig);
          this.$emit('panel-chart-update');
        }
      },
    },
  },
  mounted() {
    this.updateChartConfig();
    const { list, select_fields_order, info } = this.tableData;
    this.panelInstance = new window.BkChartsPanel(
      this.$el,
      list || [],
      select_fields_order || [],
      this.chartConfig,
      this.getQueryMessage(),
      info
    );
    this.panelInstance.on('option-changed', (newVal, oldVal, isLastConfig) => {
      window.$bk_perfume.start('Chart_Changed_Action', { data: this.chartConfig });
      window.$bk_perfume.end('Chart_Changed_Action', { data: { newVal, oldVal, isLastConfig } });
      this.optionChanged = true;
      this.$emit('option-changed', this.panelInstance.getOptions());
      this.autoSaveTimer && clearTimeout(this.autoSaveTimer);
      this.autoSaveTimer = setTimeout(() => {
        this.autoSavePanelConfig();
      }, 300);
    });
    this.panelInstance.on('plugin-click', (e, type) => {
      if (type === 'download') {
        this.$emit('on-download-result');
      }
    });
    window.addEventListener('beforeunload', this.beforeunloadFn);
  },
  beforeDestroy() {
    this.autoSaveTimer && clearInterval(this.autoSaveTimer);
  },
  methods: {
    updateChartConfig() {
      const minWidht = this.chartConfig.chart.width;
      let maxHeight = this.chartConfig.chart.height;
      const contentHeight = this.$el.offsetHeight - 80;
      maxHeight = maxHeight < contentHeight ? maxHeight : contentHeight;
      const activeWidth = this.$el.offsetWidth - 600;
      this.chartConfig.chart.width = activeWidth > minWidht ? activeWidth : minWidht;

      const activeHeight = Math.floor(activeWidth / 1.6);
      const minHeight = 300;
      this.chartConfig.chart.height = activeHeight > maxHeight
        ? maxHeight
        : activeHeight > minHeight
          ? activeHeight
          : minHeight;
      this.chartConfig.tableHeight = this.chartConfig.chart.height;
      this.chartConfig.watermark.text = this.$store.getters.getUserName;
    },
    getQueryMessage() {
      return isNaN(Date.parse(this.tableData.execute_time))
        ? ''
        : `查询时间：${this.tableData.execute_time}（数据有效期 7 天，保存至 ${this.getNextDate(
          this.tableData.execute_time
        )}）`;
    },
    getNextDate(dateStr) {
      if (isNaN(Date.parse(dateStr))) {
        return dateStr;
      }

      const date = new Date(dateStr);
      const nextDate = date.setDate(date.getDate() + 7);
      return format(new Date(nextDate), 'yyyy-MM-dd HH:mm:ss');
    },

    updateChartPanelOptions(cfg = {}) {
      this.panelInstance.updateOptions(cfg);
    },

    updateChartPanelHight(movedHeight = 0, updateChart = true) {
      const oldOpt = this.panelInstance.getOptions();
      const copyCfg = JSON.parse(JSON.stringify(this.chartConfig));
      if (updateChart) {
        copyCfg.chart.height += movedHeight;
      }
      copyCfg.tableHeight += movedHeight;
      this.panelInstance.updateOptions(copyCfg);
    },

    autoSavePanelConfig() {
      this.bkRequest
        .httpRequest('dataExplore/updateTask', {
          params: {
            chart_config: JSON.stringify(this.panelInstance.getOptions()),
            query_id: this.$route.query.query,
          },
        })
        .then(res => {
          if (!res.result) {
            console.warn('autoSavePanelConfig error:' + res.message);
          }
        })
        ['catch'](e => {
          console.warn('autoSavePanelConfig error:' + e.message);
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.search-result-table {
  height: 100%;
  min-height: 400px;
  padding-bottom: 0px;
  // padding-left: 10px;
  ::v-deep .bk-table-row {
    td {
      &:first-child {
        padding: 0 5px;
      }
    }
  }

  ::v-deep .bk-table-header {
    th {
      &:first-child {
        padding: 0 5px;
      }
    }
  }
}

::v-deep .chart-type-help {
  height: 30px;
  line-height: 30px;
}

::v-deep .bk-charts-panel-container {
  .panel-header {
    border-top: none;
    border-left: none;
    border-right: none;
  }

  .bkchart-panel-settings {
    border-left: none;
    /* min-height: 607px; */
    height: 100%;
  }
}
</style>
