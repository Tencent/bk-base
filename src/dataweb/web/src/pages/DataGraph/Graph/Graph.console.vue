

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
  <section id="info-area"
    class="info-log">
    <template v-if="isConsoleOpend">
      <span class="drag-button"
        @mousedown="answerMouseDown($event)">
        <i class="bk-icon icon-ellipsis" />
      </span>
    </template>
    <template v-if="isShowTab">
      <bkdata-tab :active="activeName"
        :size="'small'"
        :validateActive="false"
        @tab-change="msgBoxChange">
        <template slot="setting">
          <!-- info-log 头部 start -->
          <div class="info-log__header clearfix">
            <flow-console-loading v-if="isLoading" />
            <div class="fr">
              <!-- <span class="info-log__statistic mr10">
								<span class="info-log__statistic-item">
									<span class="nodes-status has-error bk-icon icon-exclamation-triangle-shape"></span>
									<span>{{warningCount}}</span>
								</span>
							</span> -->
              <span class="info-log__toggle"
                @click="toggleBox">
                {{ isConsoleOpend ? $t('收起') : $t('查看') }}
                <i class="bk-icon icon-more" />
              </span>
            </div>
          </div>
          <!-- info-log 头部 end -->
        </template>
        <bkdata-tab-panel :name="CONSOLE.WARNING">
          <template slot="label">
            <span class="panel-name mr10">{{ $t('告警信息') }}</span>
            <span class="panel-ext-warning">
              <span class="nodes-status has-error bk-icon icon-exclamation-triangle-shape" />
              <span :id="`${CONSOLE.WARNING}_count_lable`"
                class="warning-count-text">
                {{ warningCount[CONSOLE.WARNING] }}
              </span>
            </span>
          </template>
          <flow-warning-info ref="warning"
            @changeWarningCount="count => changeWarningCount(count, CONSOLE.WARNING)" />
        </bkdata-tab-panel>
        <bkdata-tab-panel :name="CONSOLE.RUNNING">
          <template slot="label">
            <span class="panel-name mr10">{{ $t('运行信息') }}</span>
            <span class="panel-ext-warning">
              <span class="nodes-status has-error bk-icon icon-exclamation-triangle-shape" />
              <span :id="`${CONSOLE.RUNNING}_count_lable`"
                class="warning-count-text">
                {{ warningCount[CONSOLE.RUNNING] }}
              </span>
            </span>
          </template>
          <flow-running-info ref="running"
            @changeWarningCount="count => changeWarningCount(count, CONSOLE.RUNNING)" />
        </bkdata-tab-panel>
        <bkdata-tab-panel :name="CONSOLE.HISTORY">
          <template slot="label">
            <span class="panel-name mr10">{{ $t('执行历史') }}</span>
            <span class="panel-ext-warning">
              <span class="nodes-status has-error bk-icon icon-exclamation-triangle-shape" />
              <span :id="`${CONSOLE.HISTORY}_count_lable`"
                class="warning-count-text">
                {{ warningCount[CONSOLE.HISTORY] }}
              </span>
            </span>
          </template>
          <flow-history-info ref="history"
            @changeWarningCount="count => changeWarningCount(count, CONSOLE.HISTORY)" />
        </bkdata-tab-panel>
      </bkdata-tab>
    </template>
  </section>
</template>

<script>
import Bus from '@/common/js/bus';
import { mapState } from 'vuex';
import { CONSOLE_ACTIVE_TYPE } from './Config/Console.config.js';
import FlowConsoleLoading from './Components/Console/flowConsoleLoading';
import FlowRunningInfo from './Components/Console/flowRunningInfo';
import FlowWarningInfo from './Components/Console/flowWarningInfo';
import FlowHistoryInfo from './Components/Console/flowHistoryInfo';
export default {
  name: 'flow-console',
  components: {
    FlowConsoleLoading,
    FlowRunningInfo,
    FlowWarningInfo,
    FlowHistoryInfo,
  },
  model: {
    prop: 'panelHeight',
    event: 'changeHeight',
  },
  props: {
    panelHeight: {
      type: Number,
      default: 0,
    },
    activeSection: {
      type: String,
      default: '',
    },
    isLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      activeName: '',
      CONSOLE: CONSOLE_ACTIVE_TYPE,
      isMouseDown: false,
      srcPosY: 0,
      isExpand: false,
      maxAutoHeight: 600,
      minAutoHeight: 40,
      warningCount: {
        [CONSOLE_ACTIVE_TYPE.RUNNING]: 0,
        [CONSOLE_ACTIVE_TYPE.HISTORY]: 0,
        [CONSOLE_ACTIVE_TYPE.WARNING]: 0,
      },
      isShowTab: true,
    };
  },
  computed: {
    isConsoleOpend() {
      return [this.CONSOLE.WARNING, this.CONSOLE.RUNNING, this.CONSOLE.HISTORY].includes(this.activeName);
    },
  },
  watch: {
    activeSection(val) {
      if (val !== this.activeName) {
        this.msgBoxChange(val);
        !val && this.setPanelHeight(this.minAutoHeight);
      }
    },
    '$route.params.fid'(val) {
      this.isShowTab = false;
      for (const key in this.warningCount) {
        this.$set(this.warningCount, key, 0);
      }
      this.$nextTick(() => {
        this.isShowTab = true;
      });
    },
  },
  mounted() {
    this.initDragEvent();
  },
  methods: {
    msgBoxChange(name) {
      this.activeName = name;
      this.activeSection !== name && this.$emit('update:activeSection', name);
      name && this.loadBoxInfo();
      this.panelHeight <= 40 && this.setPanelHeight(this.maxAutoHeight / 2);
    },
    changeWarningCount(count, tabName) {
      this.$set(this.warningCount, tabName, count);
      const tabLabel = document.getElementById(`${tabName}_count_lable`);
      if (tabLabel) {
        tabLabel.innerHTML = count;
      }
    },
    /**
     * 加载各个面板中的内容
     */
    loadBoxInfo() {
      switch (this.activeName) {
        case this.CONSOLE.HISTORY:
          this.$refs.history && this.$refs.history.updateHistoryInfo();
          break;
        case this.CONSOLE.RUNNING: {
          this.$refs.running && this.$refs.running.scrollToButtom();
          break;
        }
        case this.CONSOLE.WARNING: {
          this.$refs.warning && this.$refs.warning.updateWarningInfo();
          // let timer = setInterval(() => {
          //     if (this.activeName !== this.CONSOLE.WARNING) {
          //         clearInterval(timer)
          //         return
          //     }
          //     this.$refs.warning && this.$refs.warning.updateWarningInfo()
          // }, 1000 * 60 * 2)
          break;
        }
      }
    },
    toggleBox() {
      this.$emit('changeHeight', (this.isConsoleOpend && this.minAutoHeight) || this.maxAutoHeight / 2);
      this.setPanelHeight((this.isConsoleOpend && this.minAutoHeight) || this.maxAutoHeight / 2);
      this.msgBoxChange(this.isConsoleOpend ? '' : this.CONSOLE.RUNNING);
    },
    initDragEvent() {
      document.onmousemove = this.answerGlobalMouseMove;
      document.onmouseup = this.answerGlobalMouseUp;
    },
    /**
     * 响应全局鼠标移动事件，配合answerMouseDown和answerGlobalMouseUp完成info-area的拖拽伸缩事件
     */
    answerGlobalMouseMove(e) {
      if (this.isMouseDown) {
        let destPosY = 0;
        let moveY = 0;
        let destHeight = this.minAutoHeight;
        destPosY = e.pageY;
        moveY = this.srcPosY - destPosY;
        this.srcPosY = destPosY;
        let infoArea = document.querySelector('#info-area');
        destHeight = infoArea.clientHeight + moveY;
        let height = 0;
        if (destHeight < 100) {
          height = 100;
        } else if (destHeight > this.maxAutoHeight) {
          height = this.maxAutoHeight;
        } else {
          height = destHeight;
        }
        this.setPanelHeight(height);
      }
    },
    setPanelHeight(height) {
      const infoArea = document.querySelector('#info-area');
      infoArea.style.height = `${height}px`;
      let infoBottom = -(infoArea.clientHeight - this.minAutoHeight);
      infoArea.style.height = `${height}px`;
      infoArea.style.bottom = `${infoBottom}px`;
      let bodyHeight = height > 67 ? height - this.minAutoHeight : 67;
      this.$emit('changeHeight', Math.abs(infoBottom) + this.minAutoHeight);
      for (let item of document.querySelectorAll('.info-log__body')) {
        item.style.height = `${bodyHeight}px`;
      }
      return true;
    },
    answerMouseDown(e) {
      this.isMouseDown = true;
      this.srcPosY = e.pageY;
    },
    answerGlobalMouseUp(e) {
      this.isMouseDown = false;
    },
  },
};
</script>

<style lang="scss" scoped>
/*底部info start*/
.mask-hidden {
  position: absolute;
  top: 0px;
  left: 0px;
  right: 0px;
  bottom: -15px;
  z-index: 1;
}
.info-log {
  &.expanded {
    z-index: 199;
    bottom: 0 !important;
    transition: bottom 0.2s;
  }
  ::v-deep .bk-tab-section {
    padding: 0;
  }
  .drag-button {
    position: absolute;
    top: -2px;
    left: 50%;
    width: 24px;
    height: 6px;
    line-height: 6px;
    text-align: center;
    border-radius: 3px;
    background: #3a84ff;
    color: #ececf6;
    z-index: 101;
    cursor: s-resize;
    .bk-icon {
      line-height: 6px;
    }
  }

  ::v-deep .bk-tab-label-item {
    .panel-ext-warning {
      position: relative;
      color: rgba(255, 72, 0, 1);
      .nodes-status {
        position: absolute;
        top: 50%;
        left: 3px;
        transform: translateY(-50%);
      }

      .warning-count-text {
        padding-left: 20px;
      }
    }
  }

  ::v-deep .bk-tab-header {
    border-left: none;
    .info-log__header {
      border: none;
      line-height: 42px;
      height: 40px;
      background: #fafafa;
      .bk-spin-loading {
        top: 0px;
      }
    }

    .info-log__toggle {
      display: inline-block;
      height: 40px;
      line-height: 42px;
      padding: 0 15px;
      border-left: 1px solid #eaeaea;
      cursor: pointer;
    }
  }

  ::v-deep .bk-tab-content {
    .info-log__statistic-item {
      position: relative;
      margin-right: 10px;
    }
    .info-log__body {
      width: 100%;
      height: 300px;
      background-color: #fff;
      overflow-y: auto;
      position: relative;
      .no-warning {
        text-align: center;
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        p {
          color: #cfd3dd;
          font-size: 14px;
        }
      }
    }
    .info-log__report {
      .info-log__report-item {
        padding: 0 20px;
        word-break: break-all;
        &:not(:first-child) {
          border-top: 1px solid #eaeaea;
        }

        &.checked {
          background: #fff3ed;
        }
        &.expanded {
          background-color: #fafafa;
        }
        &:last-child {
          border-bottom: 1px solid #eaeaea;
        }
        &:hover {
          background: #fff3ed;
        }
        .expand-content {
          padding-left: 30px;
          &.error {
            color: #ea3636;
          }
        }
        .expand-body {
          padding-bottom: 10px;
        }
      }

      .info-log__report-title {
        &.danger {
          color: #ff6600;
        }
        min-height: 34px;
        line-height: 34px;
        font-size: 12px;
        font-weight: bold;
        cursor: pointer;
        &.error {
          color: #ea3636;
        }
        &.warning {
          color: #737987;
        }
      }
      .info-log__report-content {
        padding-left: 32px;
        font-size: 12px;
        color: #737987;
        .content-row {
          margin-bottom: 10px;
          line-height: 1;
        }
        .content-position {
          padding-left: 30px;
        }
      }
    }
    .expand-icon {
      display: inline-block;
      margin: 0 5px;
      border-left: 6px solid #888;
      border-top: 4px solid transparent;
      border-bottom: 4px solid transparent;
      transition: all linear 0.2s;
      &.expanded {
        transform: rotate(90deg);
      }
    }
    .nodes-status + .title {
      margin-left: 10px;
    }
    .Operator {
      font-weight: normal;
    }
    .down-icon {
      display: inline-block;
      vertical-align: -1px;
      transition: all linear 0.2s;
      &.down {
        transform: rotate(90deg);
      }
    }
  }
}
/*底部info end*/
</style>
