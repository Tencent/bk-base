

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
  <div class="alert-chart">
    <div class="batch-delay-chart">
      <div class="batch-delay">
        <div class="type">
          <span>
            {{ $t('离线任务调度信息') }}
          </span>
        </div>
        <div class="pl30 pr30 mb20 mt5">
          <div>
            {{ $t('窗口类型') }}:
            <span class="status status-primary">
              {{ scheduleInfo.accumulate ? $t('按小时累加窗口') : $t('固定窗口') }}
            </span>
          </div>
          <div>{{ $t('统计频率') }}: {{ scheduleInfo.count_freq }} {{ scheduleInfo.schedule_period }}(s)</div>
          <div>{{ $t('统计延迟') }}: {{ scheduleInfo.delay }}{{ $t('小时') }}</div>
          <div v-if="scheduleInfo.accumulate">
            {{ $t('数据起点') }}: {{ scheduleInfo.data_start }}时
          </div>
          <div v-if="scheduleInfo.accumulate">
            {{ $t('数据终点') }}: {{ scheduleInfo.data_end }}时
          </div>
        </div>
        <div class="type">
          <span>
            {{ $t('该离线任务最近48次执行情况') }}
          </span>
        </div>
        <div class="pl30 pr30 mb20 mt5">
          <bkdata-table :data="executionList"
            :emptyText="$t('暂无数据')">
            <bkdata-table-column :label="$t('调度周期')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div>
                  {{ props.row.period_start_time }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('开始时间')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div>
                  {{ props.row.start_time }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('结束时间')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div v-if="endStatus.includes(props.row.src_status)">
                  {{ props.row.end_time }}
                </div>
                <div v-else>
                  -
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('执行状态')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div>
                  <span v-bk-tooltips.right="jobStatusConfigs[props.row.src_status].description
                          || jobStatusConfigs[props.row.src_status].status_alias"
                    :class="'status status-' + getJobStatusColor(props.row.status)">
                    {{ jobStatusConfigs[props.row.src_status].status_alias }}
                  </span>
                </div>
              </template>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue';
import moment from 'moment';
export default {
  props: {
    alertInfo: {
      type: Object,
      required: true,
    },
  },
  data() {
    const dimensions = this.alertInfo.dimensions;
    return {
      alertTime: moment(this.alertInfo.alert_time).unix(),
      loaded: false,
      executionList: [],
      processingId: dimensions.processing_id || dimensions.data_set_id,
      scheduleInfo: {},
      jobStatusConfigs: {},
      endStatus: [
        'finished', 'killed', 'decommissioned',
        'disabled', 'failed', 'failed_succeeded',
        'stopped', 'skipped'
      ],
    };
  },
  computed: {},
  mounted() {
    this.getBatchExecutions();
    this.getBatchSchedule();
    this.getJobStatusConfigs();
  },
  methods: {
    getJobStatusColor(status) {
      if (status === 'finished') {
        return 'success';
      } else if (status === 'running') {
        return 'primary';
      } else if (status === 'failed_succeeded') {
        return 'warning';
      } else if (status === 'fail') {
        return 'danger';
      } else {
        return 'default';
      }
    },
    getBatchExecutions() {
      const options = {
        query: {
          processing_ids: [this.processingId],
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/getBatchExecutions', options).then(res => {
        if (res.result) {
          this.executionList = res.data[this.processingId].execute_history;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    getBatchSchedule() {
      const options = {
        query: {
          processing_ids: [this.processingId],
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/getBatchSchedules', options).then(res => {
        if (res.result) {
          this.scheduleInfo = res.data[this.processingId];
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    getJobStatusConfigs() {
      this.bkRequest.httpRequest('dmonitorCenter/getJobStatusConfigs').then(res => {
        if (res.result) {
          for (let status_config of res.data) {
            this.jobStatusConfigs[status_config.status_id] = status_config;
          }
        }
      });
    },
  },
};
</script>

<style lang="scss">
.alert-chart {
  .type {
    height: 30px;
    line-height: 30px;
    padding: 0px 15px;
    font-weight: bold;
    &::before {
      content: '';
      width: 2px;
      height: 19px;
      background: #3a84ff;
      display: inline-block;
      margin-right: 15px;
      position: relative;
      top: 4px;
    }
    .name {
      display: inline-block;
      min-width: 58px;
      height: 24px;
      color: white;
      line-height: 24px;
      background-color: #737987;
      border-radius: 2px;
      text-align: center;
      margin-top: 16px;
      padding: 0 5px;
    }
    .bk-icon {
      color: #3a84ff;
      cursor: pointer;
    }
  }
}
</style>
