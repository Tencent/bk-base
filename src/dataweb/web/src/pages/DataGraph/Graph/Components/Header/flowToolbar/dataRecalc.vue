

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
  <dialog-wrapper
    :dialog="dialog"
    extCls="recalculate"
    :title="$t('数据补算')"
    :subtitle="$t('补算选定节点时间范围内的数据_目前只有离线计算支持补算')"
    :subtitleTip="{
      icon: 'icon-question-circle',
      content: {
        theme: 'light',
        content: $t('自动过单条件'),
        placement: 'right',
      },
    }">
    <template #content>
      <div class="bk-form">
        <p class="info-tip">
          <span class="icon-exclamation-circle" />{{ $t('执行补算操作可能会导致存储介质数据重复') }}
        </p>
        <div class="bk-form-item">
          <label class="bk-label">{{ $t('节点名称') }}：</label>
          <div class="bk-form-content">
            <bkdata-selector
              :id="'node_id'"
              :customStyle="{
                width: '300px',
              }"
              :disabled="status !== 'none'"
              :multiSelect="true"
              :list="getCanRecalNode"
              :selected.sync="selectedNodes"
              :searchable="true"
              :displayKey="'displayName'"
              :settingKey="'node_id'"
              :searchKey="'displayName'"
              :placeholder="$t('请选择节点_目前只支持离线计算节点')" />
          </div>
        </div>
        <div class="bk-form-item">
          <label class="bk-label">{{ $t('开始时间') }}：</label>
          <div class="bk-form-content">
            <bkdata-date-picker
              v-model="startTime"
              :disabled="status !== 'none'"
              :timePickerOptions="{
                steps: [1, 60, 60],
              }"
              :placeholder="$t('请选择')"
              :type="'datetime'"
              :transfer="true"
              format="yyyy-MM-dd HH:mm:ss"
              :options="starttimePickerOptions"
              style="width: 300px" />
          </div>
        </div>
        <div class="bk-form-item">
          <label class="bk-label">{{ $t('截止时间') }}：</label>
          <div class="bk-form-content">
            <bkdata-date-picker
              v-model="endTime"
              :disabled="status !== 'none'"
              :timePickerOptions="startHourPicker"
              :placeholder="$t('请选择')"
              :type="'datetime'"
              :transfer="true"
              format="yyyy-MM-dd HH:mm:ss"
              :options="endtimePickerOptions"
              style="width: 300px" />
          </div>
        </div>
        <div class="bk-form-item clearfix">
          <bkdata-checkbox v-model="withChild"
            :disabled="status !== 'none'">
            {{ $t('同时补算依赖节点') }}
          </bkdata-checkbox>
        </div>
        <div v-if="errorTip && status === 'none'"
          class="bk-form-item">
          <p class="bk-form-error">
            {{ errorTip }}
          </p>
        </div>
        <div v-if="status === 'applying'"
          class="bk-form-item">
          <p class="bk-form-link"
            @click="jumpLink">
            <span class="text">{{ $t('补算审批中_点击跳转到审批页') }}</span>
            <span class="icon-link-to" />
          </p>
        </div>
      </div>
    </template>
    <template #footer>
      <bkdata-button v-if="shouldSubmit"
        theme="primary"
        class="data-btn"
        :loading="isLoading"
        @click="submit">
        {{ submitText }}
      </bkdata-button>
      <bkdata-button v-if="shouldRevert"
        theme="primary"
        class="data-btn"
        :loading="revertLoading"
        @click="cancelApply">
        {{ $t('撤销') }}
      </bkdata-button>
      <bkdata-button class="data-btn"
        @click="newToken">
        {{ $t('取消') }}
      </bkdata-button>
    </template>
  </dialog-wrapper>
</template>
<script>
import { mapGetters, mapState } from 'vuex';
import { showMsg } from '@/common/js/util.js';
import { bkRequest } from '@/common/js/ajax';
import moment from 'moment';
import Bus from '@/common/js/bus';
import dialogWrapper from '@/components/dialogWrapper';
export default {
  components: {
    dialogWrapper,
  },
  data() {
    return {
      isLoading: false,
      revertLoading: false,
      selectedNodes: [],
      dialog: {
        isShow: false,
        width: 600,
        quickClose: false,
        loading: false,
      },
      withChild: false,
      startTime: moment()
        .subtract(1, 'hours')
        .subtract(1, 'days')
        .format('YYYY-MM-DD HH:00:00'),
      endTime: moment()
        .subtract(1, 'hours')
        .format('YYYY-MM-DD HH:59:59'),
      endtimePickerOptions: {},
      starttimePickerOptions: {},
      startHourPicker: {
        steps: [1, 59, 59],
        disabledHours: [],
      },
    };
  },
  computed: {
    flowId() {
      return this.fid || this.$route.params.fid || '';
    },
    errorTip() {
      return this.customCalculate.message;
    },
    formatStartTime(time) {
      return moment(this.startTime).format('YYYY-MM-DD HH:00:00');
    },
    formatEndTime(time) {
      return moment(this.endTime).format('YYYY-MM-DD HH:00');
    },
    submitText() {
      return this.status === 'ready' ? this.$t('执行') : this.$t('提交');
    },
    shouldRevert() {
      return this.status === 'ready' || this.status === 'applying';
    },
    shouldSubmit() {
      return this.status === 'none' || this.status === 'ready';
    },
    ...mapState({
      graphNode: state => state.ide.graphData.locations,
      status: state => state.ide.complementStatus.status,
      customCalculate: state => state.ide.complementStatus,
    }),
    ...mapGetters({
      getCanRecalNode: 'ide/getCanRecalNode',
    }),
  },
  watch: {
    'dialog.isShow'(cur, pre) {
      // 判断是重新打开界面，重新打开，请求接口，刷新状态
      if (cur === true && pre === false) {
        let id = this.flowId;
        if (id) {
          this.dialog.loading = true;
          bkRequest.httpRequest('dataFlow/getFlowInfoByFlowId', { params: { fid: id } }).then(resp => {
            if (resp.result) {
              this.$store.commit(
                'ide/setComplementConfig',
                resp.data.custom_calculate || { status: 'none', config: {}, message: '' }
              ); // 根据接口更新补算状态
            } else {
              showMsg(resp.message, 'error');
            }
            this.dialog.loading = false;
          });
        }
      }
    },
    customCalculate(config) {
      if (config.status !== 'none') {
        // 内容回填，如果状态不是可补算，所以内容根据后台数据进行回填
        this.selectedNodes = this.customCalculate.config.node_ids;
        this.startTime = this.customCalculate.config.start_time;
        this.endTime = this.customCalculate.config.end_time;
        this.withChild = this.customCalculate.config.with_child;
      }
    },
    endTime(newTime) {
      // 防止截止时间比开始事前早，做了强制处理
      if (new Date(newTime).getTime() < new Date(this.startTime).getTime()) {
        this.startTime = this.endTime;
      }
      this.endTime = moment(newTime).format('YYYY-MM-DD HH:59:59');

      // 动态禁用小时时间
      if (moment(this.endTime).format('YYYY-MM-DD') === moment(this.startTime).format('YYYY-MM-DD')) {
        let startHour = new Date(this.startTime).getHours();
        let disableHour = [];
        for (let i = 0; i < startHour; i++) {
          disableHour.push(i);
        }
        this.$set(this.startHourPicker, 'disabledHours', disableHour);
      } else {
        this.startHourPicker.disabledHours = [];
      }
    },
    startTime(newTime) {
      // 防止截止时间比开始事前早，做了强制处理
      if (new Date(newTime).getTime() > new Date(this.endTime).getTime()) {
        this.endTime = this.startTime;
      }
      this.startTime = moment(newTime)
        .startOf('hour')
        .format('YYYY-MM-DD HH:00:00');
    },
    getCanRecalNode(nodes) {
      if (
        (nodes || []).some(item => {
          return item.node_id === this.selectedNodes[0];
        })
      ) {
        return;
      }
      this.selectedNodes = [];
    },
  },

  created() {
    let self = this;
    this.endtimePickerOptions = {
      disabledDate(time) {
        // 当时间为00:00时， disable必须是当天的，否则要减去1天
        if (new Date(self.startTime).getHours() === 0) {
          return time.getTime() < moment(self.startTime).valueOf();
        } else {
          return (
            time.getTime() <
            moment(self.startTime)
              .subtract(1, 'days')
              .valueOf()
          );
        }
      },
    };
    this.starttimePickerOptions = {
      disabledDate(time) {
        return time.getTime() > new Date();
      },
    };
  },
  methods: {
    jumpLink() {
      this.$router.push({ path: '/auth-center/records' });
    },
    getSubmitSuccessUpdateNodes(data) {
      let updateNodes = [];
      data.node_info
        && Object.keys(data.node_info).forEach(key => {
          let nodeid = key;
          let status = { custom_calculate_status: data.node_info[key].status };
          updateNodes.push({ id: `ch_${nodeid}`, content: status });
        });
      return updateNodes;
    },
    getUpdateNodes() {
      let updateNodes = [];
      this.getCanRecalNode.forEach(node => {
        let nodeId = node.id;
        let status = { custom_calculate_status: 'none' };
        updateNodes.push({ id: nodeId, content: status });
      });
      return updateNodes;
    },
    submit() {
      this.isLoading = true;
      if (this.status === 'ready') {
        this.bkRequest
          .httpRequest('dataFlow/submitComplementTask', {
            params: {
              fid: this.$route.params.fid,
            },
          })
          .then(res => {
            if (res.result) {
              this.updateStatus('running');
              showMsg(this.$t('执行成功'), 'success');
              this.$emit('submitSuccess', this.getSubmitSuccessUpdateNodes(res.data));
              this.dialog.isShow = false;
            } else {
              showMsg(res.message, 'error');
            }
            this.isLoading = false;
          });
      } else if (this.status === 'none') {
        this.bkRequest
          .httpRequest('dataFlow/applyComplementTask', {
            params: {
              fid: this.$route.params.fid,
              node_ids: this.selectedNodes,
              start_time: this.formatStartTime,
              end_time: this.endTime,
              with_child: this.withChild,
            },
          })
          .then(res => {
            if (res.result) {
              const status = res.data.status;
              this.updateStatus(status);

              if (status === 'applying') {
                this.$emit('applySubmitSuccess', this.getSubmitSuccessUpdateNodes(res.data));
                showMsg(this.$t('补算需求待管理员审批'), 'success');
                this.dialog.isShow = false;
              } else {
                showMsg(this.$t('补算需求已自动过单'), 'success');
              }
            } else {
              showMsg(res.message, 'error');
            }
            this.isLoading = false;
          });
      }
    },
    cancelApply() {
      this.revertLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/canceleApplication', {
          params: {
            fid: this.$route.params.fid,
          },
        })
        .then(res => {
          if (res.result) {
            this.updateStatus('none');
            showMsg(this.$t('撤销成功'), 'success');
            this.$emit('cancelApply', this.getUpdateNodes());
            this.dialog.isShow = false;
          } else {
            showMsg(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.revertLoading = false;
        });
    },
    open() {
      this.dialog.isShow = true;
    },
    newToken() {
      this.dialog.isShow = false;
    },
    updateStatus(status) {
      this.$store.commit('ide/setComplementStatus', status);
    },
  },
};
</script>
<style lang="scss" scoped>
.recalculate {
  .content {
    max-height: 500px;
    .bk-form {
      width: 480px;
      padding: 20px 0;
      margin-bottom: 10px;
      .info-tip {
        position: relative;
        left: 83px;
        padding: 10px;
        padding-right: 25px;
        width: 397px;
        background: rgba(255, 244, 226, 1);
        border: 1px solid rgba(255, 184, 72, 1);
        border-radius: 2px;
        margin-bottom: 20px;
        line-height: 18px;
        color: rgba(99, 101, 110, 1);
        font-size: 14px;
        font-family: MicrosoftYaHei;
        display: flex;
        justify-content: flex-start;
        align-items: flex-start;
        span {
          padding-right: 8px;
          font-size: 18px;
          color: rgba(255, 156, 1, 1);
        }
      }
      .bk-label {
        // padding:11px 16px 11px 10px;
        width: 180px;
      }
      .bk-form-content {
        margin-left: 180px;
        .el-date-editor.el-input {
          width: 100%;
          .el-input__prefix {
            top: -4px;
          }
        }
      }
      .bk-form-checkbox {
        margin-left: 182px;
      }
    }
    .bk-form-error {
      color: red;
      width: 300px;
      margin-left: 80px;
      text-align: left;
    }
    .bk-form-link {
      color: #0484ff;
      width: 166px;
      margin-left: 182px;
      text-align: left;
      display: flex;
      align-items: center;
      cursor: pointer;
      &:hover {
        .text {
          text-decoration: underline;
        }
      }
      .icon-link-to {
        margin-left: 10px;
      }
    }
    .bk-form-action {
      margin-top: 20px;
    }
    .required {
      color: #f64646;
      display: inline-block;
      vertical-align: -2px;
      margin-right: 5px;
    }
    .bk-button {
      min-width: 120px;
    }
    .bk-label-checkbox {
      margin: 0;
      padding-top: 9px;
      padding-bottom: 9px;
    }
    .bk-form-not-power {
      line-height: 36px;
      a {
        vertical-align: sub;
        text-decoration: underline;
      }
    }
    .bk-form-power-loading {
      height: 36px;
    }
  }
  .footer {
    padding: 12px 24px;
    background-color: #fafbfd;
    border-top: 1px solid #dcdee5;
    border-radius: 2px;
    height: 60px;
    display: flex;
    justify-content: flex-end;
    .data-btn {
      margin-left: 8px;
    }
  }
}
</style>
