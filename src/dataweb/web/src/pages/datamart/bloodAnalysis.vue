

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
    class="data-blood">
    <div class="data-blood-switch-wrap">
      <div class="node-wrap">
        <p>{{ $t('节点说明') }}：</p>
        <div class="node-icon data-normal table-node-background">
          <i class="icon icon-datasource" />{{ $t('数据源') }}
        </div>
        <bkdata-popover v-if="isShowTargetDom"
          :width="200"
          :tippyOptions="popOverFullScreenOption"
          theme="light"
          placement="bottom-start">
          <div class="node-icon data-normal table-node-background">
            <i class="icon icon-resulttable" />{{ $t('结果表') }}
          </div>
          <div slot="content"
            class="bk-dropdown-menu blood-analysis-dropdown">
            <ul class="bk-dropdown-list">
              <li v-for="(item, index) in resultTableIcons"
                :key="index"
                class="result-table-border">
                <i :class="['icons', item.icon]" />{{ item.name }}
              </li>
            </ul>
          </div>
        </bkdata-popover>
        <bkdata-popover v-if="isShowTargetDom"
          :width="162"
          :tippyOptions="popOverFullScreenOption"
          theme="light"
          placement="bottom-start">
          <div class="node-icon">
            <i class="icon icon-cal-node" />{{ $t('数据处理') }}
          </div>
          <div slot="content"
            class="bk-dropdown-menu blood-analysis-dropdown">
            <ul class="bk-dropdown-list">
              <li v-for="(item, index) in processNodeIcons"
                :key="index">
                <i :class="['icons', item.icon]" />{{ item.name }}
              </li>
            </ul>
          </div>
        </bkdata-popover>
      </div>
      <div class="switch-sub-wrap">
        <bkdata-popover v-if="isShowTargetDom"
          :tippyOptions="popOverFullScreenOption"
          placement="top">
          <div class="data-blood-switch">
            <bkdata-switcher v-model="isSwitchChinese"
              :showText="true"
              size="small"
              :onText="$t('中文')"
              :offText="$t('英文')" />
            <p>{{ isSwitchChinese ? $t('显示中文名') : $t('显示英文名') }}</p>
          </div>
          <div slot="content"
            class="bk-dropdown-menu blood-analysis-dropdown">
            {{ isSwitchChinese ? $t('节点上仅显示中文名称') : $t('节点上仅显示英文名称') }}
          </div>
        </bkdata-popover>
        <bkdata-popover v-if="isShowTargetDom"
          :tippyOptions="popOverFullScreenOption"
          placement="top">
          <div class="data-blood-switch">
            <bkdata-switcher size="small"
              :showText="true"
              :onText="''"
              :offText="''"
              @change="changeGraphParam" />
            <p>{{ $t('仅显示数据节点') }}</p>
          </div>
          <div slot="content"
            class="bk-dropdown-menu blood-analysis-dropdown">
            {{ $t('仅显示数据节点_数据源和结果表') }}
          </div>
        </bkdata-popover>
        <i v-if="!isFullScreen"
          id="full-screen"
          v-bk-tooltips="$t('全屏')"
          class="bk-icon icon-full-screen icon-screen"
          @click="handleFullScreen" />
        <i v-if="isFullScreen"
          id="exit-full-screen"
          style="position: relative"
          class="bk-icon icon-un-full-screen icon-screen"
          @mouseleave="isQuitScreen = false"
          @mouseover="isQuitScreen = true"
          @click="quitFullScreen">
          <div v-if="isQuitScreen"
            class="h-tooltip vue-tooltip vue-tooltip-visible"
            x-placement="bottom-start"
            style="position: absolute; left: -380%; top: 20px">
            <div class="tooltip-arrow"
              x-arrow=""
              style="right: 0px" />
            <div class="tooltip-content text-overflow">
              {{ $t('退出全屏') }}
            </div>
          </div>
        </i>
      </div>
    </div>
    <div class="result-tooltips">
      <ul class="bk-dropdown-list">
        <li v-for="(item, index) in resultTableIcons"
          :key="index"
          class="result-table-border">
          <i :class="['icons', item.icon]" />{{ item.name }}
        </li>
      </ul>
    </div>
    <bkGraph ref="bkGraph"
      :isSwitchChinese="isSwitchChinese" />
  </div>
</template>

<script>
import bkGraph from '@/pages/datamart/graph/graphIndex';
import Bus from '@/common/js/bus';
import tippy from 'tippy.js';

export default {
  components: { bkGraph },
  data() {
    return {
      isLoading: true,
      visiable: false,
      sheetVal: '',
      outputZh: '',
      outputEn: '',
      output: {},
      showMonaco: false,
      scope: {},
      isSwitchChinese: false,
      isQuitScreen: false,
      isFullScreen: false,
      resultTableIcons: [
        {
          name: this.$t('清洗结果表'),
          icon: 'icon-clean-result',
        },
        {
          name: this.$t('转换结果表'),
          icon: 'icon-transform-table',
        },
        {
          name: this.$t('实时计算结果表'),
          icon: 'icon-batch-result',
        },
        {
          name: this.$t('离线计算结果表'),
          icon: 'icon-offline-calc',
        },
        {
          name: this.$t('场景化模型结果表'),
          icon: 'icon-abnormal-table',
        },
        {
          name: this.$t('ModelFlow模型结果表'),
          icon: 'icon-modelflow-table',
        },
      ],
      processNodeIcons: [
        {
          name: this.$t('清洗'),
          icon: 'icon-icon-filter',
        },
        {
          name: this.$t('转换'),
          icon: 'icon-transform',
        },
        {
          name: this.$t('实时计算'),
          icon: 'icon-realtime',
        },
        {
          name: this.$t('离线计算'),
          icon: 'icon-offline',
        },
        {
          name: this.$t('场景化模型'),
          icon: 'icon-abnormal',
        },
        {
          name: this.$t('ModelFlow模型'),
          icon: 'icon-digger',
        },
        {
          name: this.$t('存储类计算'),
          icon: 'icon-save-node',
        },
        {
          name: this.$t('视图'),
          icon: 'icon-view',
        },
      ],
      popOver: null,
      isShowTargetDom: true,
    };
  },
  computed: {
    popOverFullScreenOption() {
      if (this.isFullScreen) {
        return {
          appendTo: document.getElementsByClassName('data-blood')[0],
        };
      }
      return {
        appendTo: document.body,
      };
    },
  },
  mounted() {
    Bus.$on('bloodLoading', val => {
      this.isLoading = val;
    });
    let self = this;
    document.addEventListener('fullscreenchange', function () {
      if (!document.fullscreenElement) {
        self.$nextTick(() => {
          self.isFullScreen = false;
          Bus.$emit('isFullScreen', self.isFullScreen);
          document.body.style.marginTop = '0px';
        });
      }
    });
  },
  beforeDestroy() {
    this.$store.commit('ide/setGraphData', {});
  },
  methods: {
    handleFullScreen() {
      // 全屏
      document.getElementsByClassName('data-blood')[0].requestFullscreen();
      this.isFullScreen = true;
      // 先销毁popover组件，再重新创建，全屏下tooltips的appendTo需要动态改变
      // 而这个参数只在popover组件的mounted里传入tippy，所以需要重新加载组件
      this.isShowTargetDom = false;
      this.$nextTick(() => {
        this.isShowTargetDom = true;
      });
      document.body.style.marginTop = '-60px'; // 改变tooltips内容区域的transformY
      Bus.$emit('isFullScreen', this.isFullScreen); // 发送是否全屏状态给节点模板
    },
    quitFullScreen() {
      // 退出全屏
      document.exitFullscreen();
      document.body.style.marginTop = '0px';
      this.isFullScreen = false;
      this.isShowTargetDom = false;
      this.$nextTick(() => {
        this.isShowTargetDom = true;
      });
      Bus.$emit('isFullScreen', this.isFullScreen);
    },
    changeGraphParam(val, item) {
      this.$refs.bkGraph.changeGraphParam(val);
    },
    showPopOver(dom, dropDownDom) {
      let domContent = document.getElementsByClassName(dom)[0].getBoundingClientRect();
      let content = document.getElementsByClassName(dropDownDom)[0].innerHTML;
      const options = {
        theme: 'light',
        trigger: 'manual',
        content,
        arrow: true,
        placement: 'bottom',
      };
      if (this.isFullScreen) {
        options.appendTo = document.getElementsByClassName('data-blood')[0];
      }
      this.popOver = tippy(
        {
          getBoundingClientRect: () => domContent,
        },
        options
      );
      this.popOver.show(0);
    },
    hiddenPopOver() {
      this.popOver && this.popOver.hide();
    },
  },
};
</script>
<style lang="scss" scoped>
.data-blood {
  height: 100%;
  // ::v-deep .jsflow{
  //     border-color: rgba(195,205,215,.6);
  //     .__right{
  //         top: 61px;
  //         opacity: 1;
  //         z-index: 6;
  //         right: 20px;
  //     }
  //     .canvas-area{
  //         background-color: white;
  //     }
  // }
  .bk-graph-container {
    position: relative;
    height: calc(100% - 43px);
  }
  .data-blood-switch-wrap {
    position: relative;
    z-index: 1;
    padding: 7px 0;
    display: flex;
    justify-content: space-between;
    background-color: #f7f7f7;
    font-size: 12px;
    .switch-sub-wrap {
      display: flex;
      align-items: center;
      .data-blood-switch {
        margin-left: 20px;
        display: flex;
        align-items: center;
        background-color: #f7f7f7;
        p {
          margin-left: 10px;
        }
        ::v-deep .is-checked {
          background-color: #3a84ff;
        }
        .is-unchecked {
          background: #c4c6cc;
        }
      }
      .bk-icon {
        cursor: pointer;
        outline: none;
        font-size: 14px;
        margin: 0 32px 0 30px;
      }
      .switch-chinese-name {
        // 切换中英文siwtch背景
        ::v-deep .bk-switcher {
          background-color: #3a84ff;
        }
      }
    }
    .node-wrap {
      margin-left: 15px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      p {
        float: left;
      }
      .node-icon {
        display: flex;
        align-items: center;
        margin-right: 7px;
        border: 1px solid #63656e;
        border-radius: 4px;
        padding: 5px 10px;
        cursor: default;
        .icon {
          margin-right: 5px;
          border-right: 1px solid #979ba5;
          padding-right: 5px;
          font-size: 15px;
        }
      }
      .data-normal {
        border-radius: 12px;
      }
      .table-node-background {
        background-color: #e1ecff;
      }
    }
  }
  .result-tooltips {
    display: none;
  }
}
.full-screen {
  position: fixed !important;
  left: 0;
  right: 0;
  top: 0;
  z-index: 280;
  width: 100%;
  height: 100%;
  margin: 0;
}
</style>
<style lang="scss">
.blood-analysis-dropdown {
  .bk-dropdown-list {
    max-height: 300px;
    cursor: default;
    li {
      display: flex;
      align-items: center;
      padding: 5px 10px;
      border: 1px solid #63656e;
      margin-bottom: 5px;
      border-radius: 5px;
      &:hover {
        background-color: #eaf3ff;
        color: #3a84ff;
      }
      .icons {
        margin-right: 5px;
        font-size: 15px;
        border-right: 1px solid #979ba5;
        padding-right: 5px;
      }
      &:last-child {
        margin-bottom: 0px;
      }
    }
    .result-table-border {
      border-radius: 10px;
      background-color: #e1ecff;
    }
  }
}
</style>
