

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
  <div class="bk-form">
    <div v-bkloading="{ isLoading: tabLoading }"
      class="acompute-node-edit node">
      <div class="record-wrapper">
        <bkdata-form :labelWidth="125"
          extCls="mt20 bk-common-form">
          <bkdata-form-item :label="$t('开始时间')"
            :required="true"
            property="start_time">
            <bkdata-date-picker
              v-model="executionTime.start_time"
              :placeholder="$t('开始时间')"
              type="datetime"
              :transfer="true"
              style="width: 100%"
              @pick-success="getExecuteRecord" />
          </bkdata-form-item>
          <bkdata-form-item :label="$t('截止时间')"
            :required="true"
            property="end_time">
            <bkdata-date-picker
              v-model="executionTime.end_time"
              :placeholder="$t('截止时间')"
              :transfer="true"
              type="datetime"
              style="width: 100%"
              @pick-success="getExecuteRecord" />
          </bkdata-form-item>
        </bkdata-form>
        <div class="button-wrapper mt20">
          <bkdata-button
            v-show="selfDepOption"
            theme="primary"
            :loading="makeUpLoading"
            :disabled="isExecutable"
            @click="dialogShow">
            {{ $t('补齐数据') }}
          </bkdata-button>
          <bkdata-button @click="getExecuteRecord">
            {{ $t('刷新') }}
          </bkdata-button>
        </div>
        <bkdata-table :data="dataList"
          style="margin-top: 30px"
          extCls="make-up-table">
          <bkdata-table-column :label="$t('任务时间')"
            prop="schedule_time"
            width="148" />
          <bkdata-table-column :label="$t('提交时间')"
            prop="start_time"
            width="148" />
          <bkdata-table-column :label="$t('结束时间')"
            prop="end_time"
            width="148" />
          <bkdata-table-column :label="$t('状态')">
            <template slot-scope="props">
              <bkdata-checkbox
                v-model="props.row.active"
                extCls="record-checkbox"
                :disabled="!selfDepOption"
                @change="statusChange(props.$index)" />
              <span
                v-bk-tooltips="getTippyContent(props.row.err_msg)"
                class="status-label-success"
                :class="{
                  'status-label-failure': props.row.status_str === '失败',
                  'status-label-warning': props.row.status_str === '警告',
                }">
                {{ props.row.status_str }}
              </span>
            </template>
          </bkdata-table-column>
        </bkdata-table>
        <Tips :text="$t('最多显示24条记录')"
          class="mt10" />
      </div>
    </div>
    <DataMakeUP
      ref="MakeUpPop"
      :nodeName="nodeName"
      :executeTime="activeData.schedule_time"
      :historyTimeList="successTimeList"
      :fid="flowId"
      :nid="nodeId" />
  </div>
</template>
<script>
import DataMakeUP from './DataMakeUP';
import moment from 'moment';
import { postMethodWarning } from '@/common/js/util';
import Tips from '@/components/TipsInfo/TipsInfo.vue';
export default {
  components: {
    DataMakeUP,
    Tips,
  },
  props: {
    params: {
      type: Object,
      default: () => ({}),
    },
    nodeId: {
      type: [String, Number],
      default: '',
    },
    loading: {
      type: Boolean,
      default: true,
    },
    isSelfDepEnabled: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      makeUpLoading: false,
      recordLoading: false,
      executionTime: {
        start_time: moment()
          .subtract(1, 'hours')
          .subtract(1, 'days')
          .format('YYYY-MM-DD HH:00:00'),
        end_time: moment()
          .subtract(1, 'hours')
          .format('YYYY-MM-DD HH:59:59'),
      },
      activeData: {
        schedule_time: '',
        start_time: '',
        end_time: '',
        is_allowed: false,
        status: '',
        status_str: '',
        active: false,
      },
      dataList: [],
    };
  },
  computed: {
    selfDepOption() {
      return this.isSelfDepEnabled || this.params.advanced && this.params.advanced.self_dependency;
    },
    nodeName() {
      const output = this.params.outputs && this.params.outputs[0];
      const currentRtid = (output && `${output.bk_biz_id}_${output.table_name}`) || '';
      return `${currentRtid}(${this.params.name})`;
    },
    successTimeList() {
      const list = [];
      this.dataList.forEach((item, index) => {
        if (item.status_str === '成功') {
          list.push({
            id: item.schedule_time,
          });
        }
      });
      return list;
    },
    flowId() {
      return this.$route.params.fid;
    },
    tabLoading() {
      return this.loading || this.recordLoading;
    },
    isExecutable() {
      return !this.activeData.is_allowed;
    },
  },
  watch: {
    nodeId(val) {
      val && this.getExecuteRecord();
    },
  },
  methods: {
    validateForm() {
      return true;
    },
    getTippyContent(errMsg) {
      if (errMsg) {
        return {
          content: errMsg,
          placements: ['top'],
        };
      }
      return {
        disabled: true,
      };
    },
    getExecuteRecord() {
      if (this.executionTime.start_time.valueOf() >= this.executionTime.end_time.valueOf()) {
        postMethodWarning(this.$t('结束时间必须大于开始时间'), 'error');
        return;
      }
      this.activeData = {};
      this.recordLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getExecuteRecord', {
          params: {
            fid: this.flowId,
            nid: this.nodeId,
          },
          query: {
            start_time: moment(this.executionTime.start_time).format('YYYY-MM-DD HH:mm:ss'),
            end_time: moment(this.executionTime.end_time).format('YYYY-MM-DD HH:mm:ss'),
          },
        })
        .then(res => {
          if (res.result) {
            this.dataList = res.data.map(item => {
              item.active = false;
              return item;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.recordLoading = false;
        });
    },
    dialogShow() {
      this.makeUpLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getMakeUpResult', {
          params: {
            fid: this.flowId,
            nid: this.nodeId,
          },
          query: {
            schedule_time: [this.activeData.schedule_time],
          },
        })
        .then(res => {
          if (res.result) {
            if (res.data.is_allowed) {
              this.$refs.MakeUpPop.showDataMakeUp();
              return;
            }
            postMethodWarning(this.$t('当前任务记录不允许补齐'), 'error');
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.makeUpLoading = false;
        });
    },
    statusChange(index) {
      const action = this.dataList[index].active;
      if (action) {
        this.dataList.forEach((item, idx) => {
          item.active = idx === index;
          if (index === idx) {
            this.activeData = JSON.parse(JSON.stringify(item));
          }
        });
      } else {
        this.activeData = {};
      }
    },
  },
};
</script>
<style lang="scss" scoped>
@mixin status-text($backgroundColor) {
  background: $backgroundColor;
  padding: 2px 5px;
  border-radius: 2px;
  color: #fff;
}
.record-wrapper {
  min-height: 500px;
  ::v-deep .make-up-table {
    .bk-table-body-wrapper table tbody .bk-table-row .cell {
      padding-left: 11px;
      padding-right: 11px;
    }
  }
  .record-checkbox {
    margin-right: 10px;
  }
  .status-label-success {
    @include status-text($backgroundColor: #76be42);
  }
  .status-label-failure {
    @include status-text($backgroundColor: #fe621d);
    cursor: pointer;
  }

  .status-label-warning {
    @include status-text($backgroundColor: #f6ae00);
    cursor: pointer;
  }
}
</style>
