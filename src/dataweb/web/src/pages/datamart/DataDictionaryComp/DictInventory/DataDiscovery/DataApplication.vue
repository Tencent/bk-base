

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
  <div>
    <HeaderType :title="$t('数据应用')"
      :tipsContent="$t('不同的数据被各类APP使用的分布')">
      <div class="chart-container">
        <div class="chart">
          <div v-if="isShowChart && !isPieLoading"
            class="chart-type-container"
            :class="{ 'chart-en-style': isEn }">
            <div>
              {{ $t('数据分类') }}
              <span class="line" />
            </div>
            <div :class="[nodeLevel === 4 ? 'data-handler-type' : '']">
              {{ $t('数据处理类型') }}
              <span v-if="nodeLevel === 4"
                class="line" />
            </div>
            <div :class="{ margin85: isEn }">
              {{ $t('APP应用') }}
              <span class="line" />
            </div>
          </div>
          <PlotlyChart
            v-if="isShowChart"
            v-bkloading="{ isLoading: isPieLoading }"
            :eventListen="['plotly_relayout', 'onmousedown', 'plotly_hover', 'plotly_unhover']"
            :chartConfig="config"
            :chartData="applyData"
            :chartLayout="applyLayout"
            @plotly_hover="plotly_hover"
            @plotly_unhover="plotly_unhover" />
          <NoData v-else />
        </div>
      </div>
    </HeaderType>
    <div class="tippy-content">
      <bkdata-table ref="tippyContent"
        :data="tableData"
        :outerBorder="false"
        :pagination="calcPagination"
        :emptyText="$t('暂无数据')"
        @page-change="handlePageChange">
        <bkdata-table-column width="200"
          :label="$t('总计')"
          prop="count" />
        <bkdata-table-column width="200"
          :label="$t('APP应用')"
          prop="app_code" />
        <bkdata-table-column width="200"
          :label="$t('数据处理类型')">
          <div slot-scope="item"
            class="text-overflow">
            {{ nodeType[item.row.processing_type] }}
          </div>
        </bkdata-table-column>
      </bkdata-table>
    </div>
  </div>
</template>

<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import PlotlyChart from '@/components/plotly';
import tippy from 'tippy.js';

export default {
  name: 'DataApplication',
  components: {
    HeaderType,
    PlotlyChart,
    NoData: () => import('@/components/global/NoData.vue'),
  },
  inject: ['selectedParams', 'tagParam', 'getTags', 'handerSafeVal', 'getParams'],
  props: {
    level: {
      type: Number,
      default: 3,
    },
  },
  data() {
    return {
      lala: {},
      applyData: [],
      applyLayout: {},
      config: {
        staticPlot: true,
      },
      layout: {
        width: 1100,
        height: 750,
        grid: { rows: 1, columns: 1 },
      },
      isPieLoading: false,
      instance: [],
      otherData: [],
      currentPage: 1,
      nodeType: {
        batch_model: this.$t('ModelFlow模型_离线'),
        stream: this.$t('实时计算'),
        stream_model: this.$t('ModelFlow模型_实时'),
        batch: this.$t('离线计算'),
        transform: this.$t('转换'),
        clean: this.$t('清洗'),
        model: this.$t('场景化模型'),
        storage: this.$t('存储类计算'),
        view: this.$t('视图'),
      },
      isShowChart: true,
      nodeLevel: 3,
    };
  },
  computed: {
    calcPagination() {
      return {
        current: Number(this.currentPage),
        count: this.otherData.length,
        limit: 10,
      };
    },
    tableData() {
      return this.otherData.slice((this.currentPage - 1) * 10, 10 * this.currentPage);
    },
    isEn() {
      return this.$i18n.locale === 'en';
    },
  },
  mounted() {
    this.initData();
  },
  methods: {
    handlePageChange(page) {
      this.currentPage = page;
    },
    plotly_unhover() {
      // while (this.instance.length) {
      //     if (this.instance[0]) {
      //         this.instance[0].hide()
      //         this.instance[0].destroy()
      //     }
      //     this.instance.shift()
      // }
    },
    getAllData(data) {
      if (data.length) {
        const count = data.map(item => item.value).reduce((a, b) => a + b);
        return count > 0.1 ? count : 0;
      }
      return 0;
    },
    showPopover(data) {
      let distance = 0;
      let content = '';
      if (!data.points[0].label) {
        distance = -data.event.target.getBoundingClientRect().width / 2;
        // eslint-disable-next-line max-len
        content = `<div style="font-size:12px;"> source：${data.points[0].source.label} <br> target：${data.points[0].target.label}</div>`;
      } else {
        if (data.points[0].label !== '其他应用') {
          const index = this.applyData[0].node.label.findIndex(item => item === data.points[0].label);
          const name = this.applyData[0].node.name[index];
          // eslint-disable-next-line max-len
          content = `<div style="font-size:12px;">${name} <br>${this.$t('流入')}：${this.getAllData(data.points[0].targetLinks)} <br> ${this.$t('流出')}：${this.getAllData(data.points[0].sourceLinks)}</div>`;
        } else {
          content = this.$refs.tippyContent.$el;
        }
      }
      while (this.instance.length) {
        if (this.instance[0]) {
          this.instance[0].hide();
          this.instance[0].destroy();
        }
        this.instance.shift();
      }
      const instance = tippy(data.event.target, {
        theme: 'light',
        trigger: 'mouseenter',
        content,
        arrow: true,
        placement: 'right',
        interactive: true,
        distance,
        maxWidth: 800,
      });
      instance.show(100);
      this.instance.push(instance);
    },
    plotly_hover(data) {
      this.lala = data;
      this.showPopover(data);
    },
    initData() {
      this.getApplyData();
    },
    getApplyData() {
      this.applyData = [];
      this.isPieLoading = true;
      this.isShowChart = true;
      this.bkRequest
        .httpRequest('dataInventory/getApplyData', {
          params: Object.assign({}, this.getParams(), {
            level: this.level,
          }),
        })
        .then(res => {
          if (res.result) {
            if (!res.data.source.length) {
              this.isShowChart = false;
              return;
            }
            this.isShowChart = true;
            this.nodeLevel = res.data.level;
            this.otherData = res.data.other_app_code_list;
            this.applyData = [
              {
                type: 'sankey',
                orientation: 'h',
                hoverinfo: 'none',
                node: {
                  pad: 15,
                  thickness: 30,
                  line: {
                    color: 'black',
                    width: 0.5,
                  },
                  label: this.isEn ? res.data.label : res.data.alias,
                  name: res.data.label,
                  hoverlabel: {
                    bgcolor: 'transparent',
                    bordercolor: 'transparent',
                    font: {
                      color: 'transparent',
                    },
                  },
                },
                link: {
                  source: res.data.source,
                  target: res.data.target,
                  value: res.data.value,
                  line: {
                    width: 0.00001,
                  },
                },
              },
            ];
            this.applyLayout = Object.assign({}, this.layout, {
              legend: {
                x: 1,
                y: 0,
              },
            });
          } else {
            this.isShowChart = false;
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isPieLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.tippy-content {
  opacity: 0;
  height: 0;
  overflow: hidden;
}
.chart-container {
  justify-content: center;
  .chart-type-container {
    position: absolute;
    z-index: 1;
    top: 50px;
    display: flex;
    width: 1135px;
    padding: 0 80px;
    color: #373d41;
    justify-content: space-between;
    .data-handler-type {
      margin-left: 330px;
    }
    & > div {
      position: relative;
      padding-left: 10px;
      .line {
        position: absolute;
        left: 0;
        width: 0;
        height: 620px;
        border-left: 1px dashed #737987;
      }
    }
  }
  .chart-en-style {
    width: 1100px;
    .data-handler-type {
      margin-left: 322px;
    }
    .margin85 {
      margin-right: -85px;
    }
  }
}
::v-deep .tippy-popper {
  font-size: 12px;
  color: red;
}
::v-deep .no-data {
  max-width: 600px;
  height: 400px !important;
}
</style>
