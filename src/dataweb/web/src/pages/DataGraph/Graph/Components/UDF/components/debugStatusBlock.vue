

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
  <div class="udf-status-wrapper">
    <div v-if="logs.status === 'running'"
      class="bk-spin-loading bk-spin-loading-mid bk-spin-loading-primary status">
      <div class="rotate rotate1" />
      <div class="rotate rotate2" />
      <div class="rotate rotate3" />
      <div class="rotate rotate4" />
      <div class="rotate rotate5" />
      <div class="rotate rotate6" />
      <div class="rotate rotate7" />
      <div class="rotate rotate8" />
    </div>
    <span v-else-if="logs.status === 'failure'"
      class="icon-close-circle-shape status"
      style="color: #ea3636" />
    <span v-else-if="logs.status === 'terminated'"
      class="icon-minus-circle-shape status"
      style="color: #ffb848" />
    <span v-else
      class="icon-check-circle-shape status"
      style="color: #2dcb56" />
    <div class="content">
      <span class="desp">{{ logs.message }}</span>
      <bkdata-popover :theme="'light'"
        :trigger="'click'">
        <span v-if="isDebugStatus"
          class="detail">
          {{ detail }}
        </span>
        <div id="tooltip-content"
          slot="content"
          class="tooltip-content">
          <bkdata-tab
            v-if="logs.status === 'success'"
            :active.sync="activeTab"
            type="unborder-card"
            extCls="udf-debug-tab">
            <bkdata-tab-panel v-if="streamInfo.length"
              :label="$t('实时数据')"
              name="realtime">
              <div class="content"
                style="background: none">
                <bkdata-table :data="streamInfo"
                  :border="true">
                  <bkdata-table-column v-for="(item, index) in streamHeader"
                    :key="index"
                    :label="item"
                    :prop="item" />
                </bkdata-table>
              </div>
            </bkdata-tab-panel>
            <bkdata-tab-panel v-if="batchInfo.length"
              :label="$t('离线数据')"
              name="offline">
              <div class="content"
                style="background: none">
                <bkdata-table :data="batchInfo"
                  :border="true">
                  <bkdata-table-column v-for="(item, index) in batchHeader"
                    :key="index"
                    :label="item"
                    :prop="item" />
                </bkdata-table>
              </div>
            </bkdata-tab-panel>
          </bkdata-tab>
          <bkdata-tab v-else
            type="unborder-card"
            extCls="udf-debug-tab">
            <bkdata-tab-panel :label="$t('错误信息')"
              name="error">
              <div class="content">
                {{ logs.detail }}
              </div>
            </bkdata-tab-panel>
          </bkdata-tab>
        </div>
      </bkdata-popover>
    </div>
    <div class="step">
      {{ logs.stage }}
    </div>
  </div>
</template>

<script>
export default {
  props: {
    isDebugStatus: {
      type: Boolean,
      default: true,
    },
    logs: {
      type: Object,
      required: true,
    },
    calculateType: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      activeTab: 'realtime',
    };
  },
  computed: {
    detail() {
      if (this.logs) {
        return this.logs.status === 'running' ? '' : '查看结果';
      }
      return '';
    },
    streamHeader() {
      if (this.streamInfo.length) {
        return Object.keys(this.streamInfo[0]);
      }
      return [];
    },
    batchHeader() {
      if (this.batchInfo.length) {
        return Object.keys(this.batchInfo[0]);
      }
      return [];
    },
    streamInfo() {
      return JSON.parse(this.logs.detail).stream;
    },
    batchInfo() {
      return JSON.parse(this.logs.detail).batch;
    },
  },
};
</script>

<style lang="scss">
.udf-status-wrapper {
  width: 100%;
  height: 38px;
  display: flex;
  align-items: center;
  background: rgba(248, 251, 255, 1);
  border-radius: 2px;
  border: 1px solid rgba(225, 236, 255, 1);
  position: relative;
  &.udf-status-wrapper:last-child {
    margin-top: 6px;
  }
  .bk-spin-loading-mid {
    width: 20px;
    height: 20px;
    .rotate {
      width: 2px;
      height: 3px;
      transform-origin: 50% -5px;
    }
  }
  .status {
    margin-left: 20px;
    font-size: 20px;
    margin-right: 10px;
  }
  .content {
    height: auto;
    padding-left: 10px;
    .desp {
      font-size: 14px;
      font-family: MicrosoftYaHeiUI;
      color: rgba(99, 101, 110, 1);
      line-height: 18px;
      margin-right: 10px;
    }
    .detail {
      font-size: 14px;
      font-family: MicrosoftYaHeiUI;
      color: rgba(58, 132, 255, 1);
      line-height: 18px;
      cursor: pointer;
    }
  }
  .step {
    position: absolute;
    right: 25px;
    font-size: 14px;
    font-family: MicrosoftYaHeiUI;
    color: rgba(196, 198, 204, 1);
    line-height: 18px;
    margin-left: 390px;
  }
}
.tooltip-content {
  .udf-debug-tab {
    max-width: 600px;
    min-width: 400px;
    .bk-tab-section {
      padding: 10px 0 20px 0;
      .content {
        white-space: pre;
        overflow-x: auto;
        overflow-y: auto;
        max-width: 600px;
        min-width: 400px;
        height: 183px;
        background: rgba(240, 241, 245, 1);
      }
    }
  }
}
</style>
