

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
    <HeaderType :title="$t('质量概览')">
      <bkdata-dropdown-menu slot="title-right">
        <div slot="dropdown-trigger"
          class="dropdown-trigger-btn"
          style="padding-left: 19px">
          <span>{{ refreshBtnText }}</span>
          <i :class="['bk-icon icon-angle-down', { 'icon-flip': isDropdownShow }]" />
        </div>
        <ul slot="dropdown-content"
          class="bk-dropdown-list">
          <li>
            <a href="javascript:void(0);"
              @click="forceRefresh">
              {{ $t('强制刷新页面') }}
            </a>
          </li>
          <li>
            <a href="javascript:void(0);"
              @click="isShowWindow = true">
              {{ $t('设置自动刷新间隔') }}
            </a>
          </li>
        </ul>
      </bkdata-dropdown-menu>
      <div v-bkloading="{ isLoading: isLoading }"
        class="quality-index">
        <!-- <QualityIndex>
                            <template slot="header">
                                质量等级：—
                            </template>
                            <template slot="content">
                                <div class="chart-container">
                                    <PlotlyChart
                                        v-if="true"
                                        :chartData="pieData"
                                        :chartConfig="{
                                            staticPlot: true,
                                            responsive: true,
                                        }"
                                        :chartLayout="pieLayout"
                                    />
                                    <NoData v-else />
                                </div>
                            </template>
                        </QualityIndex> -->
        <QualityIndex :name="$t('告警')"
          :unit="$t('个')"
          :data-count="qualityData.alert_count"
          :alertLevel="'danger'"
          :isShowAverage="false"
          :data-des="$t('最近24小时告警个数')" />
        <div class="wrap"
          @mouseenter="changeTipsField('dataTrend')">
          <QualityIndex :name="$t('数据量')"
            :unit="$t('条')"
            :data-count="qualityData.last_output_count"
            :data-des="$t('最近1分钟数据量')"
            :averageData="qualityData.avg_output_count"
            averageDes="最近7天日平均数据量"
            @rateHover="rateHover" />
        </div>
        <div class="wrap"
          @mouseenter="changeTipsField('dataDelay')">
          <QualityIndex :name="$t('数据延迟')"
            :unit="$t('秒')"
            :data-count="qualityData.last_data_time_delay"
            :data-des="$t('最近1分钟数据延迟')"
            :averageData="qualityData.avg_data_time_delay"
            averageDes="最近7天日平均数据延迟"
            @rateHover="rateHover" />
        </div>
        <div class="wrap"
          @mouseenter="changeTipsField('dataProcessDelay')">
          <QualityIndex :name="$t('处理延迟')"
            :unit="$t('秒')"
            :data-count="qualityData.last_process_time_delay"
            :data-des="$t('最近1分钟处理延迟')"
            :averageData="qualityData.avg_process_time_delay"
            averageDes="最近7天日平均处理延迟"
            @rateHover="rateHover" />
        </div>
        <div class="wrap"
          @mouseenter="changeTipsField('dataLoss')">
          <QualityIndex
            :name="$t('数据丢失')"
            :unit="$t('条')"
            :data-count="qualityData.last_loss_count"
            :data-rate="qualityData.last_loss_rate"
            :data-des="$t('最近1分钟数据丢失量')"
            :averageData="qualityData.avg_loss_count"
            :averageRate="qualityData.avg_loss_rate"
            averageDes="最近7天日平均数据丢失量"
            @rateHover="rateHover" />
        </div>
        <div class="wrap"
          @mouseenter="changeTipsField('dataDrop')">
          <QualityIndex
            :name="$t('无效数据')"
            :unit="$t('条')"
            :data-count="qualityData.last_drop_count"
            :data-rate="qualityData.last_drop_rate"
            :data-des="$t('最近1分钟无效数据量')"
            :averageData="qualityData.avg_drop_count"
            :averageRate="qualityData.avg_drop_rate"
            :averageDes="$t('最近7天日平均无效数据量')"
            @rateHover="rateHover" />
        </div>
      </div>
    </HeaderType>
    <div class="table-container">
      <table ref="tippyContent"
        class="bk-table bk-table-outer-border dark-theme"
        border="1"
        cellspacing="0"
        cellpadding="0">
        <tr>
          <th colspan="3">
            {{ tipsTableLeftName[tipsField] }}
          </th>
          <th colspan="3">
            {{ tipsTableRightName[tipsField] }}
          </th>
        </tr>
        <tbody>
          <tr>
            <td v-for="(item, index) in avgData"
              :key="index"
              class="text-overflow max90"
              :title="item.label">
              {{ item.label }}
            </td>
          </tr>
          <tr>
            <td v-for="(item, index) in avgData"
              :key="index"
              class="text-overflow max90"
              :title="item.num">
              {{ item.num }}
            </td>
          </tr>
        </tbody>
      </table>
      <table ref="rateContent"
        class="bk-table bk-table-outer-border dark-theme"
        border="1"
        cellspacing="0"
        cellpadding="0">
        <tr>
          <th colspan="3">
            {{ rateThead }}
          </th>
        </tr>
        <tbody>
          <tr>
            <td v-for="(item, index) in rencentData"
              :key="index"
              class="text-overflow max90"
              :title="item.label">
              {{ item.label }}
            </td>
          </tr>
          <tr>
            <td v-for="(item, index) in rencentData"
              :key="index"
              class="text-overflow max90"
              :title="item.num">
              {{ item.num }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <bkdata-dialog v-model="isShowWindow"
      theme="primary"
      :maskClose="false"
      :title="$t('刷新间隔')"
      @confirm="confirm">
      <bkdata-form :labelWidth="300"
        formType="vertical">
        <bkdata-form-item :label="$t('请选择此仪表盘的刷新频率（默认不刷新）')">
          <bkdata-selector :displayKey="'name'"
            :list="refreshList"
            :searchable="true"
            :selected.sync="refreshFq"
            :settingKey="'id'"
            :searchKey="'name'" />
        </bkdata-form-item>
      </bkdata-form>
    </bkdata-dialog>
  </div>
</template>

<script>
import { dataTypeIds } from '@/pages/datamart/common/config';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import QualityIndex from '@/pages/datamart/common/components/QualityIndex';
import tippy from 'tippy.js';

export default {
  components: {
    HeaderType,
    QualityIndex,
  },
  props: {
    tipsInfo: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      dataAmount: ['dataTrend', 'dataLoss', 'dataDrop'],
      tipsTableLeftName: {
        dataTrend: '日数据量',
        dataProcessDelay: '日平均处理延迟',
        dataLoss: '日数据丢失量',
        dataDrop: '日无效数据量',
        dataDelay: '日平均数据延迟',
      },
      tipsTableRightName: {
        dataTrend: '最近7天日平均量',
        dataProcessDelay: '最近7天日平均处理延迟',
        dataLoss: '最近7天日统计量',
        dataDrop: '最近7天日统计量',
        dataDelay: '最近7天日平均数据延迟',
      },
      qualityData: {
        last_loss_rate: 10,
        last_data_time_delay: 60,
        avg_output_count: 144000,
        last_process_time_delay: 60,
        avg_loss_rate: 9,
        avg_loss_count: 9,
        alert_count: 10,
        avg_data_time_delay: 10,
        avg_process_time_delay: 10,
        last_output_count: 100,
        avg_drop_rate: 11,
        last_loss_count: 10,
        last_drop_count: 10,
        avg_drop_count: 11,
        last_drop_rate: 10,
      },
      tipsField: '',
      instance: [],
      isLoading: false,
      timer: null,
      rateThead: '',
      isDropdownShow: false,
      isShowWindow: false,
      formData: {
        name: '',
      },
      refreshList: [
        {
          id: 0,
          name: this.$t('不要刷新'),
        },
        {
          id: 1,
          name: this.$t('1分钟'),
        },
        {
          id: 5,
          name: this.$t('5分钟'),
        },
        {
          id: 15,
          name: this.$t('15分钟'),
        },
        {
          id: 30,
          name: this.$t('30分钟'),
        },
      ],
      refreshFq: 0,
      refreshBtnText: this.$t('编辑刷新方式'),
    };
  },
  computed: {
    rencentData() {
      const target = this.tipsInfo[this.tipsField];
      // target数组后三位为最近1分钟数据量
      return target ? target.slice(target.length - 3) : [];
    },
    avgData() {
      const target = this.tipsInfo[this.tipsField];
      return target ? target.slice(0, target.length - 3) : [];
    },
  },
  mounted() {
    this.getOverViewData();
  },
  beforeDestroy() {
    clearTimeout(this.timer);
  },
  methods: {
    confirm() {
      if (!this.refreshFq) {
        // 不要刷新
        this.$emit('cancelRefresh');
        clearTimeout(this.timer);
      } else {
        const timeout = this.refreshFq * 60 * 1000;
        this.$emit('refresh', timeout);
        this.getOverViewData(true, timeout);
      }
      this.refreshBtnText = !this.refreshFq
        ? this.$t('编辑刷新方式')
        : `当前刷新频率：${this.refreshList.find(item => item.id === this.refreshFq).name}`;
    },
    forceRefresh() {
      this.getOverViewData();
      this.$emit('refresh');
    },
    rateHover(event, type) {
      if (!this.tipsInfo[this.tipsField] || !this.tipsInfo[this.tipsField].length) return;
      if (type === 'rate') {
        const rateHeads = {
          dataDelay: this.$t('最近1分钟数据延迟'),
          dataDrop: this.$t('最近1分钟无效数据量'),
          dataLoss: this.$t('最近1分钟数据丢失量'),
          dataProcessDelay: this.$t('最近1分钟处理延迟'),
          dataTrend: this.$t('最近1分钟数据量'),
        };
        this.rateThead = rateHeads[this.tipsField];
      }
      while (this.instance.length) {
        if (this.instance[0]) {
          this.instance[0].hide();
          this.instance[0].destroy();
        }
        this.instance.shift();
      }
      const instance = tippy(event.target, {
        boundary: 'document.body',
        trigger: 'mouseenter',
        theme: 'dark',
        content: type === 'rate' ? this.$refs.rateContent : this.$refs.tippyContent,
        arrow: true,
        placement: 'right',
        interactive: true,
        maxWidth: 800,
      });
      instance.show();
      this.instance.push(instance);
    },
    handleNumRound(num) {
      return Math.floor(num) || 0;
    },
    changeTipsField(type) {
      this.tipsField = type;
    },
    getPercent(num, digit = 3) {
      const numStr = num.toString();
      const index = numStr.indexOf('.');
      return numStr.slice(0, index + digit);
    },
    getOverViewData(isPollData, timeout) {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getDataQualitySummary', {
          params: {
            measurement: '',
          },
          query: {
            data_set_id: this.$route.query[dataTypeIds[this.$route.query.dataType]],
          },
        })
        .then(res => {
          if (res.result) {
            this.qualityData = res.data;
            // 需要取整的字段
            const roundNums = [
              'avg_output_count',
              'avg_loss_count',
              // 'avg_data_time_delay',
              // 'avg_process_time_delay',
              'avg_drop_count',
            ];
            // 需要求百分比的字段
            const rateKeys = ['avg_loss_rate', 'avg_drop_rate'];
            for (const key in this.qualityData) {
              // 取整
              // if (roundNums.includes(key)) {
              //     this.qualityData[key] = this.handleNumRound(
              //         this.qualityData[key]
              //     )
              // }
              // 变化率小数点后保留2位
              if (rateKeys.includes(key)) {
                this.qualityData[key] = this.getPercent(this.qualityData[key]);
              }
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
          if (isPollData) {
            this.timer = setTimeout(() => {
              this.getOverViewData(true, timeout);
            }, timeout);
          }
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.max90 {
  max-width: 90px;
  padding: 0 5px;
}
.tab-style {
  width: 100%;
  ::v-deep .bk-tab-header {
    height: auto !important;
    background: #fff !important;
    padding: 10px 0;
    position: absolute;
    top: 0;
    bottom: 0;
    border: none;
  }
  ::v-deep .bk-tab-section {
    width: calc(100% - 100px);
    margin-left: 100px;
  }
}
.quality-index {
  display: flex;
  flex-wrap: nowrap;
  justify-content: space-between;
  padding: 0 20px;
  .chart-container {
    padding: 10px 5px;
  }
}
.table-container {
  height: 0;
  overflow: hidden;
}
.bk-table {
  border-collapse: collapse;
  border: none;
  // width: 300px;
  td {
    text-align: center;
    min-width: 50px;
  }
  th {
    text-align: center;
    max-width: 200px;
  }
}
.dark-theme {
  td,
  th {
    height: 30px;
    line-height: 30px;
    color: white;
    background-color: rgba(0, 0, 0, 0.8);
  }
}
.dropdown-trigger-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid #c4c6cc;
  height: 32px;
  min-width: 68px;
  border-radius: 2px;
  padding: 0 15px;
  color: #737987;
}
.dropdown-trigger-btn.bk-icon {
  font-size: 18px;
}
.dropdown-trigger-btn .bk-icon {
  font-size: 22px;
}
.dropdown-trigger-btn:hover {
  cursor: pointer;
  border-color: #979ba5;
}
.bk-dropdown-list,
.dropdown-trigger-btn {
  font-weight: normal;
  line-height: normal;
}
</style>
