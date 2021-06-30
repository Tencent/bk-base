

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
  <div class="left-content">
    <div v-if="!isNormalDebug"
      class="mr15 fl">
      <span class="status"
        :class="statusClass">
        {{ statusContent }}
      </span>
      <span v-if="alertCount"
        class="flow-alert-container">
        <span class="icon-alert" />
        {{ alertCount }}
      </span>
    </div>
    <div v-else
      class="mr15">
      <span class="status"
        :class="{ 'status-exception': debugInfo.display === 'Failure' }">
        {{ debugInfo.display }}
      </span>
    </div>
    <template v-if="projectName">
      <div v-if="!isEditing"
        class="dataflow-name">
        <i class="bk-icon icon-folder mr5" />{{ projectName }}
        &nbsp; / &nbsp;
        <i class="bk-icon icon-dataflow mr5" />{{ taskName }}
      </div>
      <div v-else
        class="bk-form-item editInput pl0">
        <div class="bk-form-content">
          <i class="bk-icon icon-folder mr5" />
          {{ projectName }}&nbsp; / &nbsp;
          <i class="bk-icon icon-dataflow mr5" />
          <bkdata-input
            v-model="taskName"
            :maxlength="50"
            class="editing ml10"
            @blur="changeDataflowName"
            @enter="changeDataflowName" />
        </div>
      </div>
      &nbsp;&nbsp;
      <i
        v-if="!isEditing && $route.params.fid"
        class="bk-icon icon-edit2"
        :title="$t('点击修改任务名')"
        @click.prevent="dataFlowNameEdit" />
    </template>
  </div>
</template>

<script>
import Bus from '@/common/js/bus.js';
import { mapGetters, mapState } from 'vuex';
export default {
  name: 'flow-title',
  data() {
    return {
      isEditing: false,
      taskName: this.storeTaskName,
      alertCount: 0,
    };
  },
  computed: {
    ...mapGetters({
      beingDebug: 'ide/isBeingDebug',
      isNormalDebug: 'ide/isNormalDebug',
    }),
    ...mapState({
      debugInfo: state => state.ide.debugInfo,
    }),
    statusContent() {
      if (this.status === 'running') {
        if (this.hasException) {
          return this.$t('运行异常');
        } else {
          return this.$t('运行正常');
        }
      } else if (this.status === 'no-start') {
        return this.$t('未启动');
      } else {
        if (/stopping/i.test(this.status)) {
          return this.$t('正在停止');
        }

        if (/starting/i.test(this.status)) {
          return this.$t('正在启动');
        }
        return this.status;
      }
    },
    statusClass() {
      if (this.status === 'running') {
        if (this.hasException) {
          return 'status-exception';
        } else {
          return 'status-running';
        }
      } else {
        return '';
      }
    },
    status() {
      return this.$store.getters['ide/flowStatus'];
    },
    hasException() {
      return this.$store.state.ide.flowData ? this.$store.state.ide.flowData.has_exception : false;
    },
    projectName() {
      return this.$store.state.ide.flowData ? this.$store.state.ide.flowData.project_name : '';
    },
    storeTaskName() {
      return this.$store.state.ide.flowData ? this.$store.state.ide.flowData.flow_name : '';
    },
  },
  watch: {
    storeTaskName(newVal) {
      this.taskName = newVal;
      this.alertCount = 0; // 切换任务，重置告警数量为0
    },
  },

  mounted() {
    Bus.$on('dataFlowAlertCount', count => {
      this.alertCount = count;
    });
    this.taskName = this.storeTaskName;
  },
  beforeDestroy() {
    this.$store.commit('ide/setFlowData', {});
  },
  methods: {
    changeDataflowName() {
      this.isEditing = false;
      this.$store
        .dispatch('api/flows/updateDataflowInfo', {
          flowId: this.$route.params.fid,
          param: { flow_name: this.taskName },
        })
        .then(resp => {
          if (resp.result) {
            this.$store.commit('ide/changeFlowDataTaskName', resp.data.flow_name);
            Bus.$emit('refleshTaskName');
          } else {
            this.getMethodWarning(resp.message, resp.code);
          }
        });
    },
    dataFlowNameEdit() {
      this.isEditing = true;
      this.$nextTick(function () {
        const el = document.querySelector('.editing input');
        el.focus();
        el.setSelectionRange(0, el.value.length);
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.left-content {
  display: flex;
  align-items: center;
  .editing {
    position: relative;
    top: -2px;
    width: 160px;
  }
  .status {
    background: #737987;
    color: #fff;
    border-radius: 2px;
    text-align: center;
    display: inline-block;
    padding: 0 5px;
    height: 20px;
    line-height: 20px;
    &-running {
      background: #9dcb6b;
    }
    &-exception {
      background: #fe771d;
    }
  }
  i.icon-pipeline {
    color: #9dcb6b;
    padding-right: 20px;
  }
  i.icons-flow {
    font-size: 18px;
    display: inline-block;
    vertical-align: -3px;
  }
  i.icon-edit2 {
    cursor: pointer;
    line-height: 49px;
  }
}

.flow-alert-container {
  font-size: 14px;
  color: rgba(246, 174, 0, 1);
  margin-left: 5px;
  display: inline-flex;
  align-items: center;
  .icon-alert {
    font-size: 14px;
    margin-right: 3px;
  }
}
</style>
