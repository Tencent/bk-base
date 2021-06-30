

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
    <HeaderType :title="$t('整体统计')"
      :tipsContent="$t('检索到的所有数据的各维统计')">
      <div v-bkloading="{ isLoading: isLoading }"
        class="chart-container">
        <ul v-if="isShow">
          <li v-for="(item, index) in statisticsData"
            :key="index">
            <div class="data-type">
              <i :class="[icon[item['type']]]" />
              <p class="name">
                {{ item.name }}
              </p>
            </div>
            <div class="content">
              <div class="explain">
                <span class="text-overflow">{{ $t('个数') }}</span>：
                <span :title="item.count"
                  class="focus-style text-overflow mr5">
                  {{ item.count }}
                </span>
              </div>
              <div class="explain">
                <span class="text-overflow">{{ $t('占比') }}</span>：
                <span :title="item.percent"
                  class="focus-style text-overflow">
                  {{ item.percent }}
                </span>
              </div>
            </div>
          </li>
        </ul>
        <NoData v-else
          :height="'107px'" />
      </div>
    </HeaderType>
  </div>
</template>

<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';

export default {
  name: 'OverallStatistics',
  components: {
    HeaderType,
    NoData: () => import('@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue'),
  },
  inject: ['selectedParams', 'tagParam', 'getTags', 'handerSafeVal', 'getParams'],
  data() {
    return {
      typeData: [],
      typeLayout: {},
      pieData: [],
      pieLayout: {},
      config: {
        operationButtons: ['resetScale2d'],
      },
      layout: {
        title: 'Global Emissions 1990-2011',
        height: 400,
        width: 600,
        // showlegend: false,
        grid: { rows: 1, columns: 1 },
      },
      isLoading: false,
      isPieLoading: false,
      isBarLoading: false,
      statisticsData: {},
      icon: {
        dataSource: 'icon-datasource',
        resultTable: 'icon-resulttable',
        biz: 'icon-biz',
        proj: 'icon-project',
      },
      isShow: true,
    };
  },
  mounted() {
    this.initData();
  },
  methods: {
    initData() {
      this.getStandarDistribution();
    },
    handlerPercent(num) {
      const numStr = num.toString();
      const index = numStr.indexOf('.');
      let percent = Number(numStr.slice(0, index + 5)) * 100;
      if (percent.toString().length > 4) {
        // js浮点数计算精度问题
        const index = percent.toString().indexOf('.');
        percent = percent.toString().slice(0, index + 3);
      }
      return percent + '%';
    },
    getStandarDistribution() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataInventory/getOverallStatistacs', {
          params: this.getParams(),
        })
        .then(res => {
          if (res.result) {
            this.isShow = true;
            this.statisticsData = {
              proj: {
                type: 'proj',
                name: this.$t('项目'),
                count: res.data.project_count,
                percent: this.handlerPercent(res.data.project_perct),
              },
              biz: {
                type: 'biz',
                name: this.$t('业务'),
                count: res.data.bk_biz_count,
                percent: this.handlerPercent(res.data.bk_biz_perct),
              },
              dataSource: {
                type: 'dataSource',
                name: this.$t('数据源'),
                count: res.data.data_source_count,
                percent: this.handlerPercent(res.data.data_source_perct),
              },
              resultTable: {
                type: 'resultTable',
                name: this.$t('结果表'),
                count: res.data.dataset_count,
                percent: this.handlerPercent(res.data.dataset_perct),
              },
            };
          } else {
            this.isShow = false;
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.chart-container {
  min-height: 107px;
  padding: 10px;
  ::v-deep .no-data {
    width: 100%;
    height: 107px;
  }
  ul {
    width: 100%;
    display: flex;
    justify-content: space-around;
    li {
      padding: 0px 10px;
      border: 1px solid #63656e;
      display: flex;
      justify-content: center;
      align-items: center;
      .data-type {
        display: flex;
        flex-direction: column;
        padding-right: 10px;
        i {
          font-size: 38px;
        }
        .name {
          font-size: 18px;
          font-weight: 540;
          text-align: left;
          margin-top: 6px;
          color: #63656e;
          font-weight: 550;
        }
      }
      .content {
        padding: 10px 0 10px 5px;
        .explain {
          display: flex;
          align-items: center;
          .focus-style {
            font-weight: 550;
            font-size: 24px;
            color: #3a84ff;
            width: 90px;
          }
        }
      }
    }
  }
}
</style>
