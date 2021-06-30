

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
  <div class="components-container">
    <component
      v-bind="$attrs"
      :is="currentActiveComponent"
      :bizid="bizid"
      :permissionHostList="permissionHostList"
      :isModify="componentData !== null"
      @componentMounted="handleComponentMounted" />
  </div>
</template>
<script>
import { getCurrentComponent } from '@/pages/DataAccess/Config/index.js';
export default {
  props: {
    bizid: {
      type: Number,
      default: 0,
    },
    activeType: {
      type: String,
      default: 'log',
    },
    /** 需要申请权限IP列表 */
    permissionHostList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      componentData: null,
      currentActiveComponent: undefined
    };
  },
  watch: {
    activeType: {
      immediate: true,
      handler(val) {
        getCurrentComponent(val).then(resp => {
          this.currentActiveComponent = (resp || {}).component;
        });
      }
    }
  },
  methods: {
    handleComponentMounted() {
      this.componentData
        && Array.prototype.forEach.call(this.$children, child => child.$nextTick(() => {
          this.deepRenderEditData(child, this.componentData);
        })
        );
    },
    renderData(data) {
      this.componentData = data;
    },
    /**
     * 编辑页面，初始化已有数据
     * @param node: 根节点
     * @param data: 接口获取数据
     */
    deepRenderEditData(node, data) {
      if (node.renderData) {
        typeof node.renderData === 'function' && node.renderData(data);
        node.$forceUpdate();
      }
      !node.isDeepTarget
        && Array.prototype.forEach.call(node.$children, child => child.$nextTick(() => {
          this.deepRenderEditData(child, data);
        })
        );
    },
  },
};
</script>
<style lang="scss">
.components-container {
  min-height: 50px;
}
</style>
