

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
  <div class="data-storage-main">
    <div
      :is="activeModuleInfo.component"
      class="module-component"
      :itemObj="itemObj"
      :details="details"
      :name="activeModuleInfo.name"
      @prev="prevHandler" />
  </div>
</template>

<script>
import rawDataList from './list.vue';
import rawDataPreview from './preview.vue';
import rawDataEdit from './edit.vue';
import rawDataAdd from './add.vue';
export default {
  props: {
    details: {
      type: Object,
      default: () => {
        {
        }
      },
    },
  },
  data() {
    return {
      dataModuleInfo: {
        list: {
          name: 'list',
          component: rawDataList,
          prev: '',
        },
        preview: {
          name: 'preview',
          component: rawDataPreview,
          prev: 'list',
        },
        add: {
          name: 'add',
          component: rawDataAdd,
          prev: 'list',
        },
        edit: {
          name: 'edit',
          component: rawDataEdit,
          prev: 'list',
        },
      },
      activeModule: 'list',
      itemObj: {},
    };
  },
  computed: {
    activeModuleInfo() {
      return this.dataModuleInfo[this.activeModule];
    },
  },
  methods: {
    prevHandler() {
      this.activeModule = this.activeModuleInfo.prev;
    },
  },
};
</script>

<style lang="scss">
.clusters.storage-module-cluster .icon-no-cluster-apply {
  display: none;
}
</style>
