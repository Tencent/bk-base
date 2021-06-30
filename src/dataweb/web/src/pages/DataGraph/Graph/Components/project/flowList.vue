/** * 侧栏flow的列表 */


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
  <section class="other-dataflow">
    <bkdata-sideslider
      :isShow.sync="show"
      :direction="'left'"
      :width="300"
      :quickClose="true"
      :class="{ 'sider-expand': isSidebarExpand }">
      <section slot="content">
        <projectSlide ref="projectSlide"
          :data-flow-info="flowData"
          @toggleOtherDataflow="toggleFlowList" />
      </section>
    </bkdata-sideslider>
  </section>
</template>

<script>
import { mapState } from 'vuex';
import projectSlide from './projectList';
export default {
  name: 'flow-list',
  components: {
    projectSlide,
  },
  props: {
    isShow: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      loading: true,
    };
  },
  computed: {
    show: {
      get() {
        return this.isShow;
      },
      set(newVal) {
        this.$emit('update:isShow', newVal);
      },
    },
    ...mapState({
      isSidebarExpand: state => state.ide.isSidebarExpand,
      flowData: state => state.ide.flowData,
    }),
  },

  methods: {
    toggleFlowList() {
      this.show = !this.show;
    },
  },
};
</script>

<style></style>
