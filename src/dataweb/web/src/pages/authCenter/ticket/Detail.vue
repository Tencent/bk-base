

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
  <div v-bkloading="{ isLoading: isLoading }"
    class="apply-detail">
    <div v-if="!stateId"
      class="bkdata-parts">
      <div class="part-title">
        {{ $t('基本信息') }}
      </div>
      <div class="no-data">
        <img src="../../../common/images/no-data.png"
          alt="">
        <p>{{ $t('暂无数据') }}</p>
      </div>
    </div>
    <div v-else
      class="bkdata-parts">
      <div class="part-title">
        {{ $t('基本信息') }}
      </div>
      <div v-if="!stateId"
        class="no-data">
        {{ $t('暂无数据') }}
      </div>
      <div class="part-content">
        <div class="base-info mb20">
          {{ detail.created_by }} {{ $t('于') }} {{ detail.created_at }} {{ $t('提交关于') }}《{{
            detail.ticket_type_display
          }}》{{ $t('单据') }}。
        </div>
        <DetailBase :ticketType="detail.ticket_type"
          :permissions="permissions" />
        <ExtraInfo
          v-if="ticketContentType.includes(detail.ticket_type) && detail.extraInfo"
          :extraInfo="detail.extraInfo" />
      </div>

      <div class="part-title">
        {{ $t('工作流') }}
      </div>
      <div v-if="isResultShow"
        class="part-content">
        <ProcessFlow :applicant="detail.created_by"
          :steps="processFlows"
          :currentStep="detail.process_step" />
      </div>
      <div v-else
        class="no-data">
        <img src="../../../common/images/no-data.png"
          alt="">
        <p>{{ $t('暂无数据') }}</p>
      </div>

      <div class="part-title">
        {{ $t('流转日志') }}
      </div>
      <div class="part-content log-content">
        <ul>
          <li v-for="(_log, index) in logs"
            :key="index"
            :class="{ 'running-log': _log.isRunningStep }"
            class="log">
            <span class="log-index">{{ (index += 1) }}</span>
            <div v-for="(content, idx) in _log.stepLogs"
              :key="idx">
              {{ content }}
            </div>
          </li>
        </ul>
      </div>
    </div>
  </div>
</template>

<script>
// Mock
import { ticketDetail } from '@/common/api/auth';
import DetailBase from './components/DetailBase';
import ProcessFlow from './components/ProcessFlow';
import { showMsg } from '@/common/js/util.js';
import ExtraInfo from './components/ExtraInfo';

export default {
  components: {
    DetailBase,
    ProcessFlow,
    ExtraInfo,
  },
  props: {
    statusInfos: {
      type: Object,
      required: true,
    },
    stateId: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      isLoading: false,
      detail: {
        created_by: 'user1',
        created_at: '2018-10-10 10:00:00',

        ticket_type: 'project_biz',
        ticket_type_display: this.$t('项目申请结果数据'),

        status: 'succeeded',
        status_display: this.$t('成功'),

        reason: 'xxxx',

        process_step: 0,
        process_length: 1,
        extraInfo: '',
      },
      permissions: [],
      logs: [
        {
          isRunningStep: false,
          stepLogs: [],
        },
      ],
      processFlows: [],
      states: [],
      isResultShow: false,
      ticketContentType: [
        'batch_recalc',
        'verify_tdm_data',
        'create_resource_group',
        'expand_resource_group',
        'create_resource_group',
      ],
    };
  },
  watch: {
    stateId(newVal) {
      newVal && this.loadTicketDetail();
    },
  },
  mounted() {
    this.loadTicketDetail();
  },
  methods: {
    loadTicketDetail() {
      if (!this.stateId) return;
      this.isLoading = true;
      ticketDetail(this.stateId).then(resp => {
        if (resp.result) {
          this.isResultShow = true;
          this.fillInSelfData(resp.data);
        } else {
          this.isResultShow = false;
          showMsg(resp.message, 'error');
        }
        this.isLoading = false;
      });
    },
    fillInSelfData(detail) {
      this.detail.created_by = detail.ticket.created_by;
      this.detail.created_at = detail.ticket.created_at;
      this.detail.ticket_type = detail.ticket.ticket_type;
      this.detail.ticket_type_display = detail.ticket.ticket_type_display;
      this.detail.status = detail.ticket.status;
      this.detail.status_display = detail.ticket.status_display;
      this.detail.reason = detail.ticket.reason;
      this.detail.process_length = detail.ticket.process_length;
      this.detail.process_step = detail.ticket.process_step;
      if (this.ticketContentType.includes(this.detail.ticket_type)) {
        this.detail.extraInfo = detail.ticket.extra_info.content || '';
      }

      this.permissions = detail.ticket.permissions || [];
      this.states = detail.states;
      this.logs = this.toLogs();
      this.processFlows = this.toProcessFlows();
    },
    toProcessFlows() {
      let flows = [];
      let doneStatus = ['succeeded', 'failed'];
      for (let index = 0; index < this.detail.process_length; index++) {
        flows.push({
          step: 0,
          states: [],
        });
        this.states.map(state => {
          if (state.process_step === index) {
            flows[index].states.push({
              name: state.status_display,
              // 审批过了，只要列出审批过的人即可
              processors: doneStatus.includes(state.status) ? [state.processed_by] : state.processors,
              class: state.status,
            });
          }
        });
      }
      return flows;
    },
    toLogs() {
      let logs = [];

      // 将提交申请作为第一条记录
      logs.push({
        isRunningStep: false,
        stepLogs: [
          `[${this.detail.created_at}] ${this.detail.created_by} ${this.$t('提交申请')}, ${this.$t('申请原因')}：${
            this.detail.reason
          }`,
        ],
      });

      // 限制下单据审批流程步骤，绝对不可能超过 100
      let step = 0;
      while (step < 100) {
        let stepStates = this.findStepStates(step);
        if (stepStates.length === 0) {
          break;
        }

        let [_isRunningStep, _logs] = this.buildStepLogs(stepStates);

        logs.push({
          isRunningStep: _isRunningStep,
          stepLogs: _logs,
        });
        if (_isRunningStep) {
          break;
        }

        step += 1;
      }

      return logs;
    },
    /**
     * 查找目标步骤节点
     */
    findStepStates(step) {
      return this.states.filter(state => state.process_step === step);
    },
    buildStepLogs(stepStates) {
      let isRunningStep = false;
      let logs = [];
      for (let state of stepStates) {
        let statueInfo = this.statusInfos[state.status];
        let _log = '';

        switch (statueInfo.location) {
          case 'heads':
          case 'tail':
            isRunningStep = false;
            _log = `[${state.processed_at}] ${state.processed_by} ${state.status_display}，${state.process_message}`;
            break;
          case 'middle':
            isRunningStep = true;
            _log = `${state.processors.join(', ')} ${state.status_display}`;
            break;
          case 'stopped':
            isRunningStep = true;
            _log = this.$t('单据已终止');
            break;
        }
        logs.push(_log);
        if (isRunningStep === true) {
          break;
        }
      }
      return [isRunningStep, logs];
    },
  },
};
</script>

<style lang="scss" scoped>
@import '~@/common/scss/conf.scss';

.apply-detail {
  .base-info {
    border-bottom: 1px dashed #ccc;
    padding: 10px 0px;
    word-break: break-word;
  }

  .log-content {
    ul {
      list-style-type: circle;
    }

    .log {
      position: relative;
      margin-bottom: 5px;
      padding-bottom: 5px;
      padding-left: 20px;
      border-bottom: 1px dashed #ccc;
      color: #aaa;

      span.log-index {
        position: absolute;
        left: 5px;
      }
    }
    .log.running-log {
      color: #737987;
    }
  }
}
.no-data {
  margin: 20px 0;
  text-align: center;
  color: #cfd3dd;
}
</style>
