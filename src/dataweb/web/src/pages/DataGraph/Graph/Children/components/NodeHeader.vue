

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
  <div class="node-header">
    <slot>
      <span class="node-header-title">
        <i :class="['bk-icon', nodeConfig.iconName]" />
        {{ nodeConfig.node_type_instance_alias }}
      </span>
      <span class="node-header-description">{{ calcDescription }}</span>
    </slot>
    <div class="right-tips">
      <span class="item"
        @click="fullScreenHandle">
        <i :class="fullScreenIcon" /><span class="full-text">{{ fullScreenText }}</span>
      </span>
      <span v-if="doc"
        class="item"
        @click="jumpToDoc">
        <i class="icon-help-fill" /><span class="help-text">{{ $t('帮助文档') }}</span>
      </span>
      <div v-if="nodeName === 'batchv2'"
        class="help-mode-switch">
        <bkdata-switcher v-model="helpMode"
          theme="primary"
          size="small"
          @change="setHelpState" />
        <span class="item">
          {{ $t('帮助模式') }}
        </span>
      </div>
    </div>
  </div>
</template>
<script>
import { mapState, mapGetters } from 'vuex';
import pangu from 'pangu';
import { isFullScreen } from '@/common/js/util.js';
import bkStorage from '@/common/js/bkStorage.js';
import Bus from '@/common/js/bus.js';
export default {
  name: 'Node-Header',
  props: {
    nodeName: {
      type: String,
      default: '',
    },
    doc: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      nodeConfig: {},
      isFullScreen: false,
      helpMode: bkStorage.get('nodeHelpMode') || false,
    };
  },
  computed: {
    ...mapGetters({
      getNodeConfigByNodeWebType: 'ide/getNodeConfigByNodeWebType',
    }),
    fullScreenIcon() {
      return this.isFullScreen ? 'icon-un-full-screen' : 'icon-full-screen';
    },
    fullScreenText() {
      return this.isFullScreen ? this.$t('缩小') : this.$t('全屏');
    },
    calcDescription() {
      return pangu.spacing(
        (this.nodeConfig.description
          && this.nodeConfig.description.replace(/<br>\s?<b>/gim, '，').replace(/<\/?b>|<\/?br>/gim, ''))
          || ''
      );
    },
  },
  watch: {
    nodeName: {
      immediate: true,
      handler(val) {
        this.getNodeConfig(val);
      },
    },
  },
  mounted() {
    window.addEventListener('resize', this.updateFullScreenStatus);
  },
  beforeDestroy() {
    window.removeEventListener('resize', this.updateFullScreenStatus);
  },
  methods: {
    setHelpState(val) {
      bkStorage.set('nodeHelpMode', val);
      Bus.$emit('offlineNodeHelp', val);
    },
    jumpToDoc() {
      window.open(`${window.BKBASE_Global.helpDocUrl}${this.doc}`);
    },
    updateFullScreenStatus() {
      this.isFullScreen = isFullScreen();
    },
    getNodeConfig(name) {
      this.nodeConfig = this.getNodeConfigByNodeWebType(name);
    },
    fullScreenHandle() {
      this.$emit('headerFullScreenClick');
    },
  },
};
</script>

<style lang="scss" scoped>
.node-header {
  display: flex;
  flex-direction: column;
  justify-content: flex-start;
  align-items: flex-start;
  padding: 15px 20px;
  // border-left: solid 1px #ddd;
  position: relative;
  line-height: normal;

  .node-header-title {
    font-size: 18px;
    margin-bottom: 10px;
    color: #333;
    display: flex;
    align-items: center;
    font-weight: 400;

    i {
      margin-right: 5px;
    }
  }
  .node-header-description {
    font-size: 14px;
    font-weight: 400;
    color: #888;
  }

  .right-tips {
    position: absolute;
    right: 4px;
    top: 18px;
    font-size: 0;
    .help-mode-switch {
      margin-top: 12px;
      margin-left: 57px;
      display: flex;
      align-items: center;
      .item {
        margin-left: 5px;
      }
    }
    .item {
      font-size: 14px;
      line-height: 19px;
      margin-right: 20px;
      cursor: pointer;
      i {
        margin-right: 6px;
        color: #c4c6cc;
      }
    }
    span {
      color: #979ba5;
    }
  }
}
</style>
